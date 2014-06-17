(ns liza.store.riak
  (:require [liza.store :as store]
            [liza.store.counters :as counter]
            [org.fressian.clojure :as fressian]
            [metrics.histograms :as histograms]
            [metrics.timers :as timers]
            [metrics.meters :as meters])
  (:import org.xerial.snappy.Snappy
           com.basho.riak.client.RiakFactory
           com.basho.riak.client.bucket.Bucket
           com.basho.riak.client.cap.DefaultRetrier
           com.basho.riak.client.cap.Retrier
           com.basho.riak.client.cap.ConflictResolver
           com.basho.riak.client.cap.Mutation
           com.basho.riak.client.builders.RiakObjectBuilder
           com.basho.riak.client.operations.FetchObject
           com.basho.riak.client.convert.Converter)
  (:refer-clojure :exclude [get put merge]))

(def default-content-type "application/fressian+snappy")

(declare make-converter mutator)

(defrecord RiakBucketMetrics [sibling-count
                              conflict-resolution-time
                              mutation-time
                              get-time
                              put-time
                              modify-time
                              delete-time
                              get-count-time
                              increment-count-time
                              deserializing-deleted-object-count
                              deserializing-missing-object-count
                              deserializing-real-object-count
                              serialize-size
                              serialize-time
                              deserialize-time])

(defn new-metrics [bucket-name]
  (RiakBucketMetrics.
    (histograms/histogram ["riak" bucket-name "sibling-count"])
    (timers/timer ["riak" bucket-name "conflict-resolution-time"])
    (timers/timer ["riak" bucket-name "mutation-time"])
    (timers/timer ["riak" bucket-name "get-time"])
    (timers/timer ["riak" bucket-name "put-time"])
    (timers/timer ["riak" bucket-name "modify-time"])
    (timers/timer ["riak" bucket-name "delete-time"])
    (timers/timer ["riak" bucket-name "get-count-time"])
    (timers/timer ["riak" bucket-name "increment-count-time"])
    (meters/meter ["riak" bucket-name "deserializing-deleted-object-count"] "objects")
    (meters/meter ["riak" bucket-name "deserializing-missing-object-count"] "objects")
    (meters/meter ["riak" bucket-name "deserializing-real-object-count"] "objects")
    (histograms/histogram ["riak" bucket-name "serialize-size"])
    (timers/timer ["riak" bucket-name "serialize-time-time"])
    (timers/timer ["riak" bucket-name "deserialize-time"])))

(deftype RiakBucket [bucket-name
                     ^Bucket bucket
                     ^ConflictResolver resolver
                     serialize
                     deserialize
                     ^Retrier retrier
                     merge-fn
                     ^String content-type
                     opts
                     ^RiakBucketMetrics metrics]
  java.lang.Object
  (toString [b] (str "RiakBucket: " bucket-name))

  store/Bucket
  (store/get [b k]
    (timers/time! (.get-time metrics)
                  (.execute
                    (doto ^FetchObject (.fetch bucket ^String k)
                      (.r ^int (:r opts))
                      (.notFoundOK (:not-found-ok? opts))
                      (.withConverter (make-converter b k))
                      (.withResolver resolver)
                      (.withRetrier retrier)))))

  (store/put [b k v]
    (timers/time! (.put-time metrics)
                  (-> (.store ^Bucket bucket ^String k v)
                    (.withConverter (make-converter b k))
                    (.withResolver resolver)
                    (.withRetrier retrier)
                    (.withValue v)
                    (.withoutFetch)
                    (.returnBody true)
                    (.execute))))

  store/MergeableBucket
  (store/merge [b v1 v2] (merge-fn v1 v2))

  store/ModifiableBucket
  (modify [b k f]
    (timers/time! (.modify-time ^RiakBucketMetrics metrics)
                  (-> (.store ^Bucket bucket ^String k "")
                    (.r ^int (:r opts))
                    (.notFoundOK (:not-found-ok? opts))
                    (.withConverter (make-converter b k))
                    (.withResolver resolver)
                    (.withRetrier retrier)
                    (.withMutator (mutator metrics f))
                    (.returnBody true)
                    (.execute))))

  store/DeleteableBucket
  (delete [b k]
    (timers/time! (.delete-time ^RiakBucketMetrics metrics)
                  (-> (.delete ^Bucket bucket ^String k)
                    (.withRetrier retrier)
                    (.execute))))


  store/Wipeable
  (wipe [^RiakBucket b]
    (doall
      (pmap
        (fn [^String k]
          (-> (.delete bucket k)
            (.execute)))
        (.keys bucket))))

  counter/CounterBucket
  (counter/get-count [b k]
    (timers/time! (.get-count-time metrics)
                  (-> (.counter ^Bucket bucket ^String k)
                    (.increment 0)
                    (.returnValue true)
                    (.execute))))

  (counter/increment [b k n]
    (timers/time! (.increment-count-time metrics)
                  (-> (.counter ^Bucket bucket ^String k)
                    (.increment n)
                    (.returnValue true)
                    (.execute)))))

(defn mutator [^RiakBucketMetrics metrics f]
  "creates a riak-java-client Mutation out of a clojure function"
  (reify Mutation
    (apply [_ original]
      (timers/time! (.mutation-time metrics)
                    (f original)))))

(defmulti serialize-content (fn [content-type data] content-type))
(defmulti deserialize-content (fn [content-type data] content-type))

(defmethod serialize-content default-content-type
  [content-type data]
  (Snappy/compress ^bytes (fressian/encode data)))

(defmethod deserialize-content default-content-type
  [content-type data]
  (fressian/decode (Snappy/uncompress data)))

(defn make-converter [^RiakBucket bucket ^String k]
  (let [^RiakBucketMetrics metrics (.metrics bucket)]
    (reify Converter
      (fromDomain [_ o vclock]
        (timers/time! (.serialize-time metrics)
                      (-> (RiakObjectBuilder/newBuilder (.bucket-name bucket) k)
                        (.withVClock vclock)
                        (.withValue ^bytes (serialize-content (.content-type bucket) ((.serialize bucket) o)))
                        (.withContentType (.content-type bucket))
                        (.build))))

      (toDomain [_ raw]
        (if (nil? raw)
          (meters/mark! (.deserializing-missing-object-count metrics)))
        (if (.isDeleted raw)
          (meters/mark! (.deserializing-deleted-object-count metrics)))
        (if (or (nil? raw) (.isDeleted raw))
          nil
          (do
            (histograms/update! (.serialize-size metrics) (count (.getValue raw)))
            (timers/time! (.deserialize-time metrics)
                          (->> (.getValue raw)
                            (deserialize-content (.getContentType raw))
                            ((.deserialize bucket))))))))))

(defn make-resolver [^RiakBucketMetrics metrics f]
  (reify ConflictResolver
    (resolve [_ siblings]
      (do
        (histograms/update! (.sibling-count metrics) (count siblings))
        (if (empty? siblings)
          nil
          (if (= 1 (count siblings))
            (first siblings)
            (timers/time! (.conflict-resolution-time metrics)
                          (reduce
                            (fn [curr n]
                              (f curr n))
                            (into #{}
                                  siblings)))))))))

(defn default-retrier
  ([] (default-retrier 5))
  ([n] (DefaultRetrier/attempts n)))

(defn connect-pb-client
  "
  Given a map of options, constructs a new RiakClient instance. Returns the
  instance.
  
  Required keys:

  :host string; host to connect to.
  :port integer; port to connect on.

  Optional keys:

  :pool-size integer; pool size, defaults to Integer/MAX_VALUE.
  :timeout   integer; connection timeout in milliseconds, defaults to 3000.

  Example invocation:

    (connect-pb-client {:host \"localhost\" :port 8087})
  "
  [{:keys [host port pool-size timeout]
    :or   {pool-size Integer/MAX_VALUE
           timeout   3000}}]

  (RiakFactory/newClient
     (-> (com.basho.riak.client.raw.pbc.PBClientConfig$Builder.)
         (.withConnectionTimeoutMillis timeout)
         (.withHost host)
         (.withPort port)
         (.withPoolSize pool-size)
         (.build))))

(defn connect-pb-bucket
  "
  Given a map of options, constructs a new RiakBucket instance. Returns the
  instance.

  Required keys:

  :bucket-name  string; the bucket's name.
  :client       IRiakClient; client used for connection.
  :merge-fn     clojure.lang.IFn; used to merge siblings.
  
  Optional keys:
  
  :content-type string; content type, defaults to riak/default-content-type.
  :serialize    clojure.lang.IFn; serializer function.
  :deserialize  clojure.lang.IFn; deserializer function.
  
  Optional Riak keys:
  
  :allow-siblings?  boolean; defaults to true.
  :last-write-wins? boolean; defaults to false.
  :not-found-ok?    boolean; defaults to false.
  :backend          string; backend to use, defaults to \"bitcask\".
  :r                integer; the number of required confirmed reads, defaults
                    to 2.
  
  Example invocation:

    (connect-pb-bucket {:bucket-name \"my-cool-bucket\"
                        :client      riak-client
                        :merge-fn    clojure.set/union})
  "
  [{:keys [^String bucket-name
           ^com.basho.riak.client.IRiakClient client
           merge-fn
           content-type
           serialize
           deserialize
           ^boolean allow-siblings?
           ^boolean last-write-wins?
           ^boolean not-found-ok?
           ^String  backend
           ^Integer r]

    ;; default values
    :or {content-type              default-content-type
         serialize                 identity
         deserialize               identity
         ^boolean allow-siblings?  true
         ^boolean last-write-wins? false
         ^boolean not-found-ok?    false
         ^String  backend          "bitcask"
         ^Integer r                2}}]

  ;; assert the required args are in the provided map
  {:pre [(instance? java.lang.String bucket-name)
         (instance? com.basho.riak.client.IRiakClient client)
         (instance? clojure.lang.IFn merge-fn)]}

  (let [riak-opts {:allow-siblings? allow-siblings?
                   :last-write-wins? last-write-wins?
                   :not-found-ok? not-found-ok?
                   :backend backend
                   :r r}
        metrics  (new-metrics bucket-name)
        bucket   (-> (.createBucket client bucket-name)
                     (.lazyLoadBucketProperties)
                     (.allowSiblings allow-siblings?)
                     (.backend backend)
                     (.lastWriteWins last-write-wins?)
                     (.execute))
        resolver (make-resolver metrics merge-fn)
        retrier  (default-retrier)]

    ;; construct RiakBucket instance
    (RiakBucket. bucket-name
                 bucket
                 resolver
                 serialize
                 deserialize
                 retrier
                 merge-fn
                 content-type
                 riak-opts
                 metrics)))

(defn connect-pb-test-bucket
  [opts]
  (connect-pb-bucket (assoc opts :backend "memory")))

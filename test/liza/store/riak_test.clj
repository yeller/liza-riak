(ns liza.store.riak-test
  (:use clojure.test)
  (:require [liza.store :as store]
            [liza.store.riak :as riak-store]
            [liza.store.counters :as counter]
            [metrics.meters :as meters]
            [clojure.set :as set]))

(def client (riak-store/connect-client {:host "localhost" :port 8087}))

(deftest get-gets-back-what-was-put
  (let [b (riak-store/connect-test-bucket {:bucket-name "test-bucket1"
                                           :client client
                                           :merge-fn first})]
    (store/wipe b)
    (store/put b "key" "value")
    (is (= "value" (store/get b "key")))))

(deftest get-gets-back-a-modify
  (let [b (riak-store/connect-test-bucket {:bucket-name "test-bucket2"
                                           :client client
                                           :merge-fn set/union})]
    (store/wipe b)
    (store/modify b "key" #(conj (or % #{}) "value"))
    (store/modify b "key" #(conj (or % #{}) "value2"))
    (is (= #{"value" "value2"} (store/get b "key")))))

(deftest put-with-merge-test
  (testing "put with merge before a value is there uses the default"
    (let [b (riak-store/connect-test-bucket {:bucket-name "test-bucket3"
                                             :client client
                                             :merge-fn +})]
      (store/wipe b)
      (store/put-with-merge b "key" 1 0)
      (is (= 1 (store/get b "key")))))

  (testing "put with merge merges a new value onto the previous one"
    (let [b (riak-store/connect-test-bucket {:bucket-name "test-bucket4"
                                             :client client
                                             :merge-fn set/union
                                             :deserialize clojure.core/set})]
      (store/wipe b)
      (store/put b "key" #{1})
      (store/put-with-merge b "key" #{2} #{})
      (is (= #{1 2} (store/get b "key"))))))

(deftest counter-test
  (testing "getting empty counters returns 0"
    (let [b (riak-store/connect-test-bucket {:bucket-name "test-bucket5"
                                             :client client
                                             :merge-fn set/union
                                             :deserialize clojure.core/set})]
      (store/wipe b)
      (is (= 0 (counter/get-count b "counter-1")))))

  (testing "incrementing counters gets the incremented value"
    (let [b (riak-store/connect-test-bucket {:bucket-name "test-bucket6"
                                             :client client
                                             :merge-fn set/union})]
      (store/wipe b)
      (is (= 1 (counter/increment b "counter-2" 1)))
      (is (= 2 (counter/increment b "counter-2" 1))))))

(defn fails-until-nth-attempt-callable [n]
  (let [state (atom 0)]
    (fn []
      (if (not= @state n)
        (do
          (swap! state inc)
          (throw (ex-info "failing so we retry" {})))
        0))))

(deftest retry-test
  (testing "it returns the right result and marks the meter"
    (let [failure-meter (meters/meter ["liza.store.riak-test" "retry-test" (str (rand 1000))] "failures")
          success-meter (meters/meter ["liza.store.riak-test" "retry-test" (str (rand 1000))] "success")
          retrier (riak-store/measured-retrier failure-meter success-meter)
          c (fails-until-nth-attempt-callable 1)]
      (is (= 0 (.attempt retrier c)))
      (is (= 1 (.count failure-meter)))
      (is (= 1 (.count success-meter)))))

  (testing "it throws the exception if it exceeds the max number of retries"
    (let [failure-meter (meters/meter ["liza.store.riak-test" "retry-test" (str (rand 1000))] "failures")
          success-meter (meters/meter ["liza.store.riak-test" "retry-test" (str (rand 1000))] "success")
          retrier (riak-store/measured-retrier failure-meter success-meter 1)
          c (fails-until-nth-attempt-callable 3)]
      (is (thrown? Exception (.attempt retrier c))))))

(ns liza.store.riak-test
  (:use clojure.test)
  (:require [liza.store :as store]
            [liza.store.riak :as riak-store]
            [liza.store.counters :as counter]
            [clojure.set :as set]))

(def client (riak-store/connect-pb-client "127.0.0.1" 10017 {}))

(deftest get-gets-back-what-was-put
  (let [b (riak-store/connect-pb-test-bucket "test-bucket1" client first riak-store/default-content-type {})]
    (store/wipe b)
    (store/put b "key" "value")
    (is (= "value" (store/get b "key")))))

(deftest put-with-merge-test
  (testing "put with merge before a value is there uses the default"
    (let [b (riak-store/connect-pb-test-bucket "test-bucket3" client + riak-store/default-content-type {})]
      (store/wipe b)
      (store/put-with-merge b "key" 1 0)
      (is (= 1 (store/get b "key")))))

  (testing "put with merge merges a new value onto the previous one"
    (let [b (riak-store/connect-pb-test-bucket "test-bucket2" client set/union riak-store/default-content-type identity clojure.core/set {})]
      (store/wipe b)
      (store/put b "key" #{1})
      (store/put-with-merge b "key" #{2} #{})
      (is (= #{1 2} (store/get b "key"))))))

(deftest counter-test
  (testing "getting empty counters returns 0"
    (let [b (riak-store/connect-pb-test-bucket "test-bucket3" client set/union riak-store/default-content-type identity clojure.core/set {})]
      (store/wipe b)
      (is (= 0 (counter/get-count b "counter-1")))))

  (testing "incrementing counters gets the incremented value"
    (let [b (riak-store/connect-pb-test-bucket "test-bucket4" client set/union riak-store/default-content-type {})]
      (store/wipe b)
      (is (= 1 (counter/increment b "counter-2" 1)))
      (is (= 2 (counter/increment b "counter-2" 1))))))

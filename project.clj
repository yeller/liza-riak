(defproject liza-riak "0.5.0"
  :global-vars {*warn-on-reflection* true}
  :description "Abstract key/value store interface"
  :url "https://github.com/liza-riak"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [liza "0.1.0"]
                 [com.basho.riak/riak-client "1.4.4"]
                 [metrics-clojure "1.0.1"]
                 [fressian-clojure "0.2.0"]
                 [org.xerial.snappy/snappy-java "1.1.1-M1"]])

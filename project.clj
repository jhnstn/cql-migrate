(defproject cql-migrate "0.1.0-SNAPSHOT"
  :description "Simple migration tool for cassandra backed projects via CQL"
  :url "http://github.com/snrobot"
  :main promojam.migrate.core
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [
                 [clojurewerkz/cassaforte "1.0.0-beta1"] 
                 [org.clojure/clojure-contrib "1.2.0"]
                 [clj-time "0.4.4"]
                 [org.clojure/clojure "1.4.0"]
                 ])

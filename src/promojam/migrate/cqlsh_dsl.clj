(ns promojam.migrate.cqlsh-dsl
  (:use [clojure.set])
  (:require [clojure.string :as cstr])
  (:gen-class))

(defmacro columns [& cols]
  (def dt [:blob	:ascii	:text :varchar	:varint	 :int  :uuid
           :timestamp	:boolean	:float	:double	:decimal :counter])
  (loop [c cols  s []]
    (if (empty? c) s
      (if (< (.indexOf dt (second c)) 0)
       (throw (Exception.
               (str "Error: "
                    (second c)
                    " is not a vaild Cassandra column type")))
      (recur (nthrest c 2) (conj s (str (first c) " " (->  c second name))))))))


(defn primary-keys [pks] pks)

(defmacro column-families [cfs columns primary-keys ]
  (let [cols (eval columns)
        c_v  (map #(first (cstr/split % #"\s")) cols)
        pk   (eval primary-keys)]
    (if (not (subset? (set pk) (set c_v) ))
      (throw (Exception. (str "Error: Invalid primary keys " (cstr/join ", " (difference (set pk) (set c_v)))))))
      (let [s-cols (cstr/join ", " cols)
            s-pk   (cstr/join ", " pk)
            s-cfs  (map #(str % " (" s-cols  ", PRIMARY KEY ( " s-pk "))" ) cfs )]
        (into [] s-cfs))))



(defn create-table [ keyspace column-families]
  (let [ cfs (eval column-families)]
    (into [(str "USE " keyspace ";")] (map  #(str "CREATE TABLE " %) cfs ))))

(defn create-tables [& tables]
  (into [] (map #(create-table (first %) (second %)) tables)))

(defn drop-tables [keyspace & tables]

  (into [(str "USE " keyspace ";")]( map #(str "DROP TABLE " % ";") tables)))


(defn create-keyspace [keyspace placement-strategy strategy-options]
  (case placement-strategy
    "NetworkTopologyStrategy" (let [data-centers (reduce #(str %1 "," %2)
                                                         (map #(str (-> name first %) ":" (second %))
                                                              strategy-options))]
                                 [(format "CREATE KEYSPACE %s WITH strategy_class = 'NetworkTopologyStrategy' AND strategy_options:{%s}"
                                          keyspace data-centers)])
    "SimpleStrategy" (if (number? (Integer. strategy-options))
                       [(format "CREATE KEYSPACE %s WITH strategy_class = 'SimpleStrategy' AND strategy_options:replication_factor = %s"
                                keyspace strategy-options)]
                       (throw (Exception. (str "Error: Invalid strategy option for SimpleStrategy: " strategy-options))))
    (throw (Exception. (str "Error: Invalid placement strategy: " placement-strategy)))))


(defmacro cql-migrate [& cmds]

  (into [] (map #(eval %) cmds)))

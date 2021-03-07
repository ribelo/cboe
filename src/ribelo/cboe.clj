(ns ribelo.cboe
  (:require
   [clojure.core.cache :as cache]
   [hato.client :as http]
   [taoensso.encore :as e]
   [cuerdas.core :as str]
   [meander.epsilon :as m]
   [jsonista.core :as json]
   [lambdaisland.regal :as regal]
   [com.wsscode.pathom3.cache :as p.cache]
   [com.wsscode.pathom3.connect.operation :as pco]
   [com.wsscode.pathom3.connect.indexes :as pci]
   [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
   [com.wsscode.pathom3.interface.eql :as p.eql]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(def kebab-case-object-mapper
  (json/object-mapper
   {:encode-key-fn (comp str/snake name)
    :decode-key-fn str/keyword}))

(defn change-ns-in-map [ns map]
  (persistent!
   (reduce-kv
    (fn [acc k v]
      (assoc! acc (e/merge-keywords [ns (name k)]) v))
    (transient {})
    map)))

(deftype Cache [^java.util.concurrent.ConcurrentHashMap x
                ^java.util.concurrent.ConcurrentHashMap ttl-map
                ^long ttl]
  clojure.lang.IAtom
  (reset [_ _]
    (.clear x)
    (.clear ttl-map))
  p.cache/CacheStore
  (p.cache/-cache-lookup-or-miss [_ k f]
    (doseq [k* (.keySet x)]
      (let [t (- ^long (e/now-udt) ^long (.getOrDefault ttl-map k* 0))]
        (when (> t ttl)
          (.remove x k*)
          (.remove ttl-map k*))))
    (if-let [v (.get x k)]
      v
      (let [res (f)]
        (.put ttl-map k (e/now-udt))
        (.put x k res)
        res))))

(def ttl-cache_ (->Cache (java.util.concurrent.ConcurrentHashMap.)
                         (java.util.concurrent.ConcurrentHashMap.)
                         (e/ms :mins 15)))

(defn -option-str->data [^String s]
  (let [rgx (regal/regex
             [:cat
              :start
              [:capture [:+ [:class [\A \Z]]]]
              [:capture [:repeat :digit 2]]
              [:capture [:repeat :digit 2]]
              [:capture [:repeat :digit 2]]
              [:capture [:class [\A \Z]]]
              [:capture [:+ :digit]]
              :end])]
    (m/match (re-matches rgx s)
      [_ ?symbol ?year ?month ?day ?type ?strike]
      {:cboe.option/symbol (keyword (str/lower ?symbol))
       :cboe.option/exp    (format "20%s-%s-%s" ?year ?month ?day)
       :cboe.option/type   (case ?type "C" :call "P" :put)
       :cboe.option/strike (e/parse-int ?strike)})))

(def ticker-resolver (pbir/alias-resolver :cboe/ticker :ticker))

(pco/defresolver options-data
  [{:cboe/keys [ticker]}]
  {::pco/output [:cboe/options-data]
   ::pco/cache-store ::ttl-cache}
  (let [data (-> (http/get (format "https://cdn.cboe.com/api/global/delayed_quotes/options/%s.json"
                                   (str/upper (name ticker))))
                 :body
                 (json/read-value kebab-case-object-mapper)
                 :data)
        data* (->> (update data :options
                           (fn [coll]
                             (mapv (partial change-ns-in-map :cboe.option) coll)))
                   (change-ns-in-map :cboe))]
    {:cboe/options-data data*}))

(comment
  (p.eql/process env {:cboe/ticker "msft"} [:cboe/options-data]))

(pco/defresolver ticker-ask
  [{:cboe/keys [options-data]}]
  {::pco/cache-store ::ttl-cache}
  {:cboe/ask (options-data :cboe/ask)})

(comment
  (p.eql/process env {:cboe/ticker "msft"} [:cboe/ask]))

(pco/defresolver ticker-bid
  [{:cboe/keys [options-data]}]
  {::pco/cache-store ::ttl-cache}
  {:cboe/bid (options-data :cboe/bid)})

(comment
  (p.eql/process env {:cboe/ticker "msft"} [:cboe/bid]))

(pco/defresolver ticker-open
  [{:cboe/keys [options-data]}]
  {::pco/cache-store ::ttl-cache}
  {:cboe/open (options-data :cboe/open)})

(comment
  (p.eql/process env {:cboe/ticker "msft"} [:cboe/open]))

(pco/defresolver ticker-high
  [{:cboe/keys [options-data]}]
  {::pco/cache-store ::ttl-cache}
  {:cboe/high (options-data :cboe/high)})

(comment
  (p.eql/process env {:cboe/ticker "msft"} [:cboe/high]))

(pco/defresolver ticker-low
  [{:cboe/keys [options-data]}]
  {::pco/cache-store ::ttl-cache}
  {:cboe/low (options-data :cboe/low)})

(comment
  (p.eql/process env {:cboe/ticker "msft"} [:cboe/low]))

(pco/defresolver ticker-close
  [{:cboe/keys [options-data]}]
  {::pco/cache-store ::ttl-cache}
  {:cboe/close (options-data :cboe/close)})
(comment
  (p.eql/process env {:cboe/ticker "msft"} [:cboe/close]))

(pco/defresolver ticker-volume
  [{:cboe/keys [options-data]}]
  {::pco/cache-store ::ttl-cache}
  {:cboe/volume (options-data :cboe/volume)})

(comment
  (p.eql/process env {:cboe/ticker "msft"} [:cboe/volume]))

(pco/defresolver options
  [{:cboe/keys [options-data]}]
  {::pco/cache-store ::ttl-cache}
  {:cboe/options
   (->> options-data
        :cboe/options
        (into []
              (comp
               (map (fn [{:cboe.option/keys [option] :as m}]
                      (merge m (-option-str->data option)))))))})

(comment
  (p.eql/process env {:cboe/ticker "msft"} [:cboe/options]))

(pco/defresolver call-options
  [{:cboe/keys [options]}]
  {::pco/cache-store ::ttl-cache}
  {:cboe.options/call-options
   (into [] (filter (fn [m] (#{:call} (:cboe.option/type m)))) options)})

(comment
  (p.eql/process env {:cboe/ticker "msft"} [:cboe.options/call-options]))

(pco/defresolver put-options
  [{:cboe/keys [options]}]
  {::pco/cache-store ::ttl-cache}
  {:cboe.options/put-options
   (into [] (filter (fn [m] (#{:put} (:cboe.option/type m)))) options)})

(comment
  (p.eql/process env {:cboe/ticker "msft"} [:cboe.options/put-options]))

(pco/defresolver total-call-delta
  [{:cboe.options/keys [call-options]}]
  {::pco/cache-store ::ttl-cache}
  {:cboe.options/total-call-delta
   (-> (reduce
        (fn [^double acc m]
          (+ acc (* ^double (:cboe.option/delta m)
                    ^double (:cboe.option/volume m))))
        0.0
        call-options)
       e/round2)})

(comment
  (p.eql/process env {:cboe/ticker "msft"} [:cboe.options/total-call-delta]))

(pco/defresolver total-put-delta
  [{:cboe.options/keys [put-options]}]
  {::pco/cache-store ::ttl-cache}
  {:cboe.options/total-put-delta
   (reduce
    (fn [^double acc m]
      (+ acc (* ^double (:cboe.option/delta m)
                ^double (:cboe.option/volume m))))
    0.0
    put-options)})

(comment
  (p.eql/process env {:cboe/ticker "msft"} [:cboe.options/total-put-delta]))

(pco/defresolver net-option-delta
  [{:cboe.options/keys [^double total-call-delta ^double total-put-delta]}]
  {::pco/cache-store ::ttl-cache}
  {:cboe.options/net-option-delta (+ total-call-delta (Math/abs total-put-delta))})

(comment
  (p.eql/process env {:cboe/ticker "msft"} [:cboe.options/net-option-delta]))

(pco/defresolver nope
  [{^double net-delta :cboe.options/net-option-delta ^double volume :cboe/volume}]
  {::pco/cache-store ::ttl-cache}
  {:cboe.options/nope (/ net-delta volume)})

(comment
  (p.eql/process env {:cboe/ticker "msft"} [:cboe.options/nope]))

(def env (-> {::ttl-cache ttl-cache_}
             (pci/register
              [ticker-resolver
               options-data
               ticker-ask
               ticker-bid
               ticker-open
               ticker-high
               ticker-low
               ticker-close
               ticker-volume
               options
               call-options
               put-options
               total-call-delta
               total-put-delta
               net-option-delta
               nope])))

(ns ribelo.cboe
  (:require
   [hato.client :as http]
   [ribelo.pathos :as p]
   [taoensso.encore :as e]
   [cuerdas.core :as str]
   [meander.epsilon :as m]
   [jsonista.core :as json]
   [lambdaisland.regal :as regal]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(def kebab-case-object-mapper
  (json/object-mapper
   {:encode-key-fn (comp str/snake name)
    :decode-key-fn str/keyword}))

(defn- add-ns-to-map [ns m]
  (persistent!
   (reduce-kv
    (fn [acc k v]
      (assoc! acc (e/merge-keywords [ns k]) v))
    (transient {})
    m)))

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

(p/reg-resolver-eq :cboe/ticker :ticker)

(p/reg-resolver ::options-data
  [{:cboe/keys [ticker]}]
  {:memoize [(e/ms :mins 5)]}
  (let [data (-> (http/get (format "https://cdn.cboe.com/api/global/delayed_quotes/options/%s.json"
                                   (str/upper (name ticker))))
                 :body
                 (json/read-value kebab-case-object-mapper)
                 :data)]
    {:cboe/options-data (add-ns-to-map :cboe.options data)}))

(p/reg-resolver ::ticker-ask
  [{:cboe/keys [options-data]}]
  {:cboe/ask (options-data :cboe.options/ask)})

(p/reg-resolver ::ticker-bid
  [{:cboe/keys [options-data]}]
  {:cboe/bid (options-data :cboe.options/bid)})

(p/reg-resolver ::ticker-open
  [{:cboe/keys [options-data]}]
  {:cboe/open (options-data :cboe.options/open)})

(p/reg-resolver ::ticker-high
  [{:cboe/keys [options-data]}]
  {:cboe/high (options-data :cboe.options/high)})

(p/reg-resolver ::ticker-low
  [{:cboe/keys [options-data]}]
  {:cboe/low (options-data :cboe.options/low)})

(p/reg-resolver ::ticker-close
  [{:cboe/keys [options-data]}]
  {:cboe/close (options-data :cboe.options/close)})

(p/reg-resolver ::ticker-volume
  [{:cboe/keys [options-data]}]
  {:cboe/volume (options-data :cboe.options/volume)})

(p/reg-resolver ::options
  [{:cboe/keys [options-data]}]
  {:cboe/options
   (into []
         (comp (map (partial add-ns-to-map :cboe.option))
               (map (fn [m] (e/merge m (-option-str->data (:cboe.option/option m))))))
         (:cboe.options/options options-data))})

(comment
  (p/eql {:cboe/ticker :spy} [:cboe/options]))

(p/reg-resolver ::call-options
  [{:cboe/keys [options]}]
  {:cboe.options/call-options
   (into [] (filter (fn [m] (#{:call} (:cboe.option/type m)))) options)})

(comment
  (p/eql {:cboe/ticker :spy} [:cboe.options/call-options]))

(p/reg-resolver ::put-options
  [{:cboe/keys [options]}]
  {:cboe.options/put-options
   (into [] (filter (fn [m] (#{:put} (:cboe.option/type m)))) options)})

(comment
  (p/eql {:cboe/ticker :spy} [:cboe.options/put-options]))

(p/reg-resolver ::total-call-delta
  [{:cboe.options/keys [call-options]}]
  {:cboe.options/total-call-delta
   (reduce
    (fn [^double acc m]
      (+ acc (* ^double (:cboe.option/delta m)
                ^double (:cboe.option/volume m))))
    0.0
    call-options)})

(comment
  (p/eql {:cboe/ticker :spy} [:cboe.options/total-call-delta]))

(p/reg-resolver ::total-put-delta
  [{:cboe.options/keys [put-options]}]
  {:cboe.options/total-put-delta
   (reduce
    (fn [^double acc m]
      (+ acc (* ^double (:cboe.option/delta m)
                ^double (:cboe.option/volume m))))
    0.0
    put-options)})

(comment
  (p/eql {:cboe/ticker :spy} [:cboe.options/total-put-delta]))

(p/reg-resolver ::net-option-delta
  [{:cboe.options/keys [^double total-call-delta ^double total-put-delta]}]
  {:cboe.options/net-option-delta (+ total-call-delta (Math/abs total-put-delta))})

(comment
  (p/eql {:cboe/ticker :spy} [:cboe.options/net-option-delta]))

(p/reg-resolver ::nope
  [{^double net-delta :cboe.options/net-option-delta ^double volume :cboe/volume}]
  {:cboe.options/nope (/ net-delta volume)})

(comment
  (p/eql {:ticker :spy} [:cboe.options/nope]))

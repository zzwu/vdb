(ns vdb.client
  (:import [voldemort.client ClientConfig SocketStoreClientFactory StoreClient StoreClientFactory]
           [voldemort.versioning Versioned])
  (:refer-clojure :exclude [get with-open]))

(def test-spec
  {:url "tcp://localhost:6666" :name "test"})

(def ^:dynamic *client*)

(defmacro with-open
  [client-sepc & body]
  `(binding [*client* (client (factory (:url ~client-sepc)) (:name ~client-sepc))]
     ~@body))

(def factory
  (memoize
    (fn [url]
    (let [config (ClientConfig.)
        factory (SocketStoreClientFactory. (.setBootstrapUrls config (into-array String (vector url))))]
      factory))))

(defn close-factory
  [url]
  (let [f (factory url)]
    (if f 
      (do
      (println "stop client factory ...")
      (.close f)))))

(defn close-client
  [client-spec]
  (close-factory (:url client-spec)))

(def client
  (memoize
    (fn [factory store-name]
      (.getStoreClient factory store-name))))

(defn put
  [k v]
  (.put *client* k v))

(defn get
  [k]
  (.getValue (.get *client* k)))
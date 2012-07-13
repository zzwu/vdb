(ns vdb.client
  (:import [voldemort.client ClientConfig SocketStoreClientFactory StoreClient StoreClientFactory]
           [voldemort.versioning Versioned])
  (:refer-clojure :exclude [get with-open]))

;;{:url "tcp://localhost:6666" :name "test" :client-config {"bootstrap_urls" "tcp://localhost:6666""socket_timeout_ms" "5000"}}
;;{:url "tcp://localhost:6666" :name "test" :client-config nil}
(def test-spec
  {:url "tcp://localhost:6666" :name "test"})

(def ^:dynamic *client*)

(defmacro with-open
  [sepc & body]
  `(binding [*client* (client (factory ~sepc) (:name ~sepc))]
     ~@body))

(defmulti make-factory (fn [spec] (:client-config spec)))

(defmethod make-factory nil
  [spec]
  (let [config (ClientConfig.)
        url (:url spec)
        factory (SocketStoreClientFactory. (.setBootstrapUrls config (into-array String (vector url))))]
      factory))

(defn props-from-map
  [config]
  (let [c (into {} (for [k (keys config)] (if (clojure.core/get config k) [k (clojure.core/get config k)])))]
    (doto (java.util.Properties.)
      (.putAll c))))

(defmethod make-factory :default
  [spec]
  (let [config (ClientConfig. (props-from-map (:client-config spec)))
        factory (SocketStoreClientFactory. config)]
      factory))

(def factory
  (memoize make-factory))

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
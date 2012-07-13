(ns vdb.server
  (:import [voldemort.server VoldemortConfig VoldemortServer]))

(def server (atom nil))

(defn stop
  []
  (if @server
    (do (.stop @server))
    (println "server stoped ..."))
  (reset! server nil))

(defn start
  [conf-path]
  (if @server
      (do
        (println "stop old server ...")
        (stop)))
  (let [vconfig (VoldemortConfig/loadFromVoldemortHome conf-path)
        new-server (VoldemortServer. vconfig)]
    (reset! server new-server)
    (.start @server)
    (println "server started ...")))

(def path "config/single_node_cluster")
(ns haproxy-log-statsd.core)

(def sample-string "Nov 28 18:28:58 localhost haproxy[19044]: 127.0.0.1:40598 [28/Nov/2015:18:28:58.483] http-in http-in/cluster-1 0/0/0/11/11 200 402 - - ---- 0/0/0/0/0 0/0 \"POST /solr/blabla/select HTTP/1.0\"")

(def parse-re #"^(\w+ \d+ \S+) (\S+) (\S+)\[(\d+)\]: (\S+):(\d+) \[(\S+)\] (\S+) (\S+)/(\S+) (\S+) (\S+) (\S+) *(\S+) (\S+) (\S+) (\S+) (\S+) \"(\S+) ([^\"]+) (\S+)\" *$")

(def parsed-keys
  [:timestamp
   :host
   :pname
   :pid
   :client_ip
   :client_port
   :accept_date
   :frontend_name
   :backend_name
   :backend_server
   :time_report
   :status_code
   :bytes_read
   :ignore1
   :ignore2
   :ignore3
   :connections_report
   :queue_report
   :method
   :path
   :protocol])

(defn string->report [s]
  (re-matches parse-re s))

(defn report->map [[_ & data]]
  (zipmap parsed-keys data))

(->> sample-string
     string->report
     report->map)

(defn process-lines! [lines]
  (println "Processing " (count lines) " lines")
  (doall (map println lines)))

(defn -main [& [fname & _]]
  (when fname
    (let [rdr (clojure.java.io/reader fname)]
      (doall (line-seq rdr)) ; ighore existing data
      (loop [s (line-seq rdr)]
        (if (seq s)
          (process-lines! s)
          (Thread/sleep 1000))
        (recur (line-seq rdr))))))

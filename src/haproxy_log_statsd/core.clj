(ns haproxy-log-statsd.core
  (:require [clj-statsd :as stats]
            [clj-time.format :as tfmt]))

; log format documentation https://www.haproxy.com/static/media/uploads/eng/resources/aloha_load_balancer_memo_log.pdf

(def sample-string "Nov 28 18:28:58 localhost haproxy[19044]: 127.0.0.1:40598 [28/Nov/2015:18:28:58.483] http-in http-in/cluster-1 0/0/0/11/11 200 402 - - ---- 0/0/0/0/0 0/0 \"POST /solr/blabla/select HTTP/1.0\"")

(def parse-re #"^(\w+ \d+ \S+) (\S+) (\S+)\[(\d+)\]: (\S+):(\d+) \[(\S+)\] (\S+) (\S+)/(\S+) (\S+) (\S+) (\S+) *(\S+) (\S+) (\S+) (\S+) (\S+) \"(\S+) ([^\"]+) (\S+)\" *$")

(def parsed-keys
  [:timestamp
   :host
   :pname
   :pid
   :client-ip
   :client-port
   :accept-time
   :frontend-name
   :backend-name
   :backend-server
   :time-report
   :status-code
   :bytes-read
   :ignore1
   :ignore2
   :ignore3
   :connections-report
   :queue-report
   :method
   :path
   :protocol])

(def accet-time-formatter (tfmt/formatter "d/MMM/y:k:m:s.SSS"))

(defn string->timing-map [s]
  (zipmap
   [:tq :tw :tc :tr :tt]
   (clojure.string/split s #"/")))

(defn string->report [s]
  (re-matches parse-re s))

(defn report->report-map [[_ & data]]
  (zipmap parsed-keys data))

(defn submit-report! [{:keys [backend-name backend-server time-report bytes-read]}]
  (let [tt (:tt (string->timing-map time-report))
        k (format "haproxy.%s.%s" backend-name backend-server)]
    (stats/timing k (read-string tt))
    (stats/gauge k  (read-string bytes-read))))

(defn process-line! [line]
  (try
    (-> line
        string->report
        report->report-map
        submit-report!)
    (catch Exception e
      (println (format "Exception \"%s\" while processing line \"%s\"" (.getMessage e) line)))))

(defn process-lines! [lines]
  (println "Processing " (count lines) " lines")
  (doall (map process-line! lines)))

(defn -main [& [fname & _]]
  (stats/setup "localhost" 8125)
  (when fname
    (let [rdr (clojure.java.io/reader fname)]
      (doall (line-seq rdr)) ; ighore existing data
      (println "Ready!")
      (loop [s (line-seq rdr)]
        (if (seq s)
          (process-lines! s)
          (Thread/sleep 1000))
        (recur (line-seq rdr))))))

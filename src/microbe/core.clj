(ns microbe.core
  (:require
   [microbe.util :as util]
   [clojure.java.io :as io]
   [clojure.java.shell :as shell]
   [clojure.string :as str])
  (:import
   java.text.SimpleDateFormat
   java.text.DecimalFormat
   [org.HdrHistogram Histogram SynchronizedHistogram]))

(def
  ^{:doc     "HDR Histogram being used"
    :private true
    :tag     Histogram}
  histo
  (SynchronizedHistogram. (.toMicros java.util.concurrent.TimeUnit/HOURS 1) 3))

(defn- make-gnuplot-input
  [template values]
  {:pre [(map? values)]}
  (loop [template template
         pairs (seq values)]
    (let [[pair & pairs] pairs]
      (if pair
        (let [[k v] pair
              pat (format "{{ %s }}" (name k))
              rep (if (string? v)
                    (str \" (str/escape (str v) {\" "\\\""}) \")
                    (str v))]
          (recur (str/replace template pat rep) pairs))
        template))))

(defn- round
  [num precision]
  (let [scale (Math/pow 10 precision)]
    (/ (Math/round (* scale num)) scale)))

(defn- us->ms
  [us]
  (round (/ us 1000) 3))

(declare default-options)

(let [session (atom 0)
      barrier (atom (promise))
      date-format (let [fmt (SimpleDateFormat. "HH:mm:ss.SSS")]
                    #(.format fmt %))]

  (defn console-logger
    [point]
    (binding [*out* (io/writer System/out)]
      (println (str "\u001b[33;1m" (:time point)
                    " \u001b[34;1m" (dissoc point :time) "\u001b[m"))))

  (defn start-reporting
    [options]
    (let [options (merge default-options options)
          {:keys [interval logger output-dir title template]} options
          output-dir (io/file output-dir)
          labels [:time :total :tps :mean :0 :99 :99.9 :100]
          getter (apply juxt labels)
          csv (io/file output-dir "report.csv")
          gpi (io/file output-dir "report.gpi")
          svg (io/file output-dir "report.svg")
          ->row #(str (str/join "\t" %) "\n")
          current-session (swap! session inc)]
      (reset! barrier (promise))
      (.reset histo)
      (.mkdirs output-dir)
      (spit gpi (make-gnuplot-input
                 template
                 (assoc (select-keys options [:title :width :height])
                        :input (.getAbsolutePath csv))))
      (spit csv (->row (map name labels)))
      (future
        (try
          (loop [total 0
                 prev-time (System/nanoTime)
                 ticker (rest (util/ticker (/ 1 interval)))
                 cont true]
            (let [now (System/nanoTime)
                  tdiff (/ (- now prev-time) (Math/pow 10 9))
                  cnt (.getTotalCount histo)
                  total (+ total cnt)
                  point (into {:time  (date-format (java.util.Date.))
                               :total total
                               :tps   (round (/ cnt tdiff) 3)
                               :mean  (us->ms (.getMean histo))}
                              (map (juxt (comp keyword str)
                                         #(us->ms (.getValueAtPercentile histo %)))
                                   [0 99 99.9 100]))]
              (.reset histo)
              (when logger (logger point))
              (spit csv (->row (getter point)) :append true)
              (when cont
                (recur total now (rest ticker) (= @session current-session)))))
          (catch Exception e
            (deliver @barrier e))
          (finally
            (let [{:keys [exit out]} (shell/sh "gnuplot" (.getPath gpi))]
              (when (zero? exit)
                (spit svg out)))
            (deliver @barrier nil))))))

  (defn stop-reporting
    []
    (swap! session inc)
    @@barrier))

(def default-options
  {:interval 5
   :logger console-logger
   :template (slurp (io/resource "report.gpi"))
   :title "Benchmark Result"
   :width 1000
   :height 700
   :output-dir "result"})

(defmacro run
  "Starts and stops console metric reporter while evaluating the forms and
  generates CSV and SVG output of the metrics during the run. If the first
  argument is a map, it is recognized as the options. See
  microbe.core/default-options for all supported options."
  [& forms]
  (let [[opts forms] (if (map? (first forms))
                       [(first forms) (rest forms)]
                       [{} forms])]
    `(try
       (start-reporting ~opts)
       ~@forms
       (finally
         (stop-reporting)))))

(defmacro <>
  "Executes the forms and records the response time with HDR histogram"
  [& forms]
  `(let [st#  (System/nanoTime)
         ret# (do ~@forms)]
     (.recordValue ^Histogram @#'histo
                   (max 0 (quot (- (System/nanoTime) st#) 1000)))
     ret#))

(comment
  (require '[microbe.core :as mcb :refer [<>]])
  (mcb/run
   {:interval 1 :output-dir "result" :title "Sleep Benchmark"}
   (dotimes [i 2000]
     (<> (Thread/sleep
          (rand-int (- 10 (* 10 (Math/sin (* Math/PI (/ i 1000)))))))))))

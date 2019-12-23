(ns microbe.core
  (:require
   [microbe.util :as util]
   [clojure.java.io :as io]
   [clojure.java.shell :as shell]
   [clojure.string :as str])
  (:import
   java.util.Date
   java.text.SimpleDateFormat
   java.text.DecimalFormat
   [org.HdrHistogram AbstractHistogram Histogram SynchronizedHistogram]))

(defn create-histo
  []
  (SynchronizedHistogram. (.toMicros java.util.concurrent.TimeUnit/HOURS 1) 3))

(def
  ^{:doc     "HDR Histogram being used"
    :private true
    :tag     Histogram}
  histo
  (create-histo))

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
  [precision num]
  (let [scale (Math/pow 10 precision)]
    (/ (Math/round (* scale num)) scale)))

(defn- us->ms
  [us]
  (round 3 (/ us 1000)))

(defn- format-elapsed
  [ns-diff]
  (let [ms-diff (quot ns-diff (Math/pow 10 6))
        ms (int (rem ms-diff 1000))
        s (int (rem (quot ms-diff 1000) 60))
        m (int (rem (quot ms-diff (* 60 1000)) 60))
        h (int (rem (quot ms-diff (* 60 60 1000)) 60))]
    (format "%02d:%02d:%02d.%03d" h m s ms)))

(defn- hhmmss
  []
  (.format (SimpleDateFormat. "HH:mm:ss.SSS") (Date.)))

(let [session (atom 0)
      barrier (atom (promise))]

  (defn console-logger
    [point]
    (binding [*out* (io/writer System/out)]
      (println (str
                "\u001b[33;1m" (hhmmss) " "
                "\u001b[34;1m" point "\u001b[m"))))

  (defn start-reporting
    [options]
    (let [{:keys [interval accum logger output-dir title template]} options
          output-dir (io/file output-dir)
          labels [:time :elapsed :total :tps :mean :0 :99 :99.9 :99.99 :100]
          getter (apply juxt (drop 2 labels))
          csv (io/file output-dir "report.csv")
          gpi (io/file output-dir "report.gpi")
          svg (io/file output-dir "report.svg")
          ->row #(str (str/join "\t" %) "\n")
          current-session (swap! session inc)
          started-at (System/nanoTime)]
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
                  point (into {:total total
                               :tps   (round 3 (/ cnt tdiff))
                               :mean  (us->ms (.getMean histo))}
                              (map (juxt (comp keyword str)
                                         #(us->ms (.getValueAtPercentile histo %)))
                                   [0 99 99.9 99.99 100]))
                  time (hhmmss)]
              (when accum (.add ^AbstractHistogram accum histo))
              (.reset histo)
              (when logger (logger point))
              (spit csv (->row (into [time (format-elapsed (- now started-at))]
                                     (getter point)))
                    :append true)
              (when cont
                (recur total now (rest ticker) (= @session current-session)))))
          (catch Exception e
            (deliver @barrier e))
          (finally
            (deliver @barrier nil)
            (let [{:keys [exit out]} (shell/sh "gnuplot" (.getPath gpi))]
              (when (zero? exit)
                (spit svg out))))))))

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

(defn- summarize
  [ns-diff ^Histogram histo]
  (let [s-diff (/ ns-diff (Math/pow 10 9))
        total (.getTotalCount histo)]
    (into {:elapsed (round 3 s-diff)
           :total total
           :tps (round 3 (/ total s-diff))
           :mean (us->ms (.getMean histo))}
          (map (juxt (comp keyword str)
                     #(us->ms (.getValueAtPercentile histo %)))
               [0 99 99.9 100]))))

(defmacro run
  "Starts and stops console metric reporter while evaluating the forms and
  generates CSV and SVG output of the metrics during the run. If the first
  argument is a map, it is recognized as the options. See
  microbe.core/default-options for all supported options."
  [& forms]
  (let [[opts forms] (if (map? (first forms))
                       [(first forms) (rest forms)]
                       [{} forms])]
    `(let [histo# (create-histo)
           opts# (assoc (merge default-options ~opts) :accum histo#)
           started# (System/nanoTime)]
       (try
         (start-reporting opts#)
         ~@forms
         (finally
           (let [elapsed# (- (System/nanoTime) started#)]
             (stop-reporting)
             (when-let [logger# (:logger opts#)]
               (logger# (#'summarize elapsed# histo#)))))))))

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

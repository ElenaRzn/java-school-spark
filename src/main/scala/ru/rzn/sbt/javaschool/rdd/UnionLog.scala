package ru.rzn.sbt.javaschool.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 *"in/log_19950701.tsv" файл содержит 10000 строк лога с одного из серверов за 1 июля 1995 года.
 * "in/log_19950801.tsv" файл содержит 10000 строк лога с одного из серверов за 1 августа 1995 года.
 *  Создайте Spark программу для генерации нового RDD, который содержит лог за оба эти дня.
 *  Возьмите выборку с параметром fraction: 0.1 из полученного RDD и сохраните в файл  "out/logs.tsv"
 *  Учтите, что в исходных файлах содержатся строки с заголовками.
 *  host	logname	time	method	url	response	bytes
 *  Удалите заголовки из результирующего файла.
 */
object UnionLog extends App {
  //Обратите внимание, что на выходе вы получите файлики с партициями.
  //Подробнее можно почитать в документации:
  //https://spark.apache.org/docs/2.3.0/configuration.html
  //Local mode: number of cores on the local machine
  //Others: total number of cores on all executor nodes or 2, whichever is larger
  val conf = new SparkConf().setAppName("unionLogs").setMaster("local[*]")

  val sc = new SparkContext(conf)

  val julyFirstLogs = sc.textFile("src/main/resource/in/log_19950701.tsv")
  val augustFirstLogs = sc.textFile("src/main/resource/in/log_19950801.tsv")

  val aggregatedLogLines = julyFirstLogs.union(augustFirstLogs)

  val cleanLogLines = aggregatedLogLines.filter(line => isNotHeader(line))

  val sample = cleanLogLines.sample(withReplacement = true, fraction = 0.1)

  sample.saveAsTextFile("out/logs.csv")


  def isNotHeader(line: String): Boolean = !(line.startsWith("host") && line.contains("bytes"))
}

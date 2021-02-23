package ru.rzn.sbt.javaschool.rdd

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark._

/**
 * Запустите класс WordCount. Проанализируйте вывод, полученный на консоль.
 * что означает параметр 3 в методе .setMaster("local[3]")?
 * что делает метод countByValue()?
 */
object WordCount extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
  val sc = new SparkContext(conf)

  val lines = sc.textFile("src/main/resource/in/word_count.txt")
  val words = lines.flatMap(line => line.split(" "))

  val wordCounts = words.countByValue()
  for ((word, count) <- wordCounts) println(word + " : " + count)

}

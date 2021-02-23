package ru.rzn.sbt.javaschool.rdd.pair

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Сделайте четыре вида join двух rdd ages и addresses.
 * Результаты запишите в текстовые файлы.
 * out/age_address_join.text
 * out/age_address_left_out_join.text
 * out/age_address_right_out_join.text
 * out/age_address_full_out_join.text
 * Просмотрите и объясните результаты.
 */
object JoinOperations extends App {
  val conf = new SparkConf().setAppName("JoinOperations").setMaster("local[1]")
  val sc = new SparkContext(conf)

  val ages = sc.parallelize(List(("Tom", 29),("John", 22), ("Nina", 25)))
  val addresses = sc.parallelize(List(("James", "USA"), ("John", "UK"), ("Nina", "Russia")))

}

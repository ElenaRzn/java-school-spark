package ru.rzn.sbt.javaschool.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * Рассмотрите файл 2016-stack-overflow-survey-responses.csv, выполните задания, для вывода используйте метод show()
 */
object TypedDataset extends App {
  val AGE_MIDPOINT = "age_midpoint"
  val SALARY_MIDPOINT = "salary_midpoint"
  val SALARY_MIDPOINT_BUCKET = "salaryMidpointBucket"

  Logger.getLogger("org").setLevel(Level.ERROR)
  val session = SparkSession.builder().appName("StackOverFlowSurvey").master("local[*]").getOrCreate()
  val dataFrameReader = session.read

  val responses = dataFrameReader
    .option("header", "true")
    .option("inferSchema", value = true)
    .csv("src/main/resource/in/2016-stack-overflow-survey-responses.csv")

  val responseWithSelectedColumns = responses.select("country", "age_midpoint", "occupation", "salary_midpoint")

  import session.implicits._
  val typedDataset = responseWithSelectedColumns.as[Response]

  // TODO выведите схему
  System.out.println("=== Schema ===")

  // TODO выведите первые 20 записей
  System.out.println("=== 20 records ===")

  // TODO отфильтруйте все ответы из Австралии
  System.out.println("=== responses from Australia ===")

  // TODO посчитайте количество родов занятий (occupations)
  System.out.println("=== count of occupations ===")

  // TODO отфильтруйте ответы со значением age_midpoint меньше 20
  System.out.println("=== responses with average mid age less than 20 ===")

  // TODO отсортируйте ответы по полю salary_midpoint по убывающей
  System.out.println("=== print the result by salary middle point in descending order ===")

  // TODO сгруппируйте ответы по стране и среднему значению поля salary_midpoint
  System.out.println("=== group by country and aggregate by average salary middle point ===")

}

package com.qf.test02

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

object Demo1 {

  //班级 学号 性别 姓名 出生年月 血型 家庭住址 身高 手机号
  case class Student(clazz: String, sno: String, sex: String, name: String, birth: String, bloodType: String, adress: String, height: Int, phone: String)

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("test1").master("local").getOrCreate()

    val rdd: RDD[String] = spark.sparkContext.textFile("data/student.txt")
    val originRdd: RDD[Student] = rdd.mapPartitions(
      _.map(
        it => {
          val infos: Array[String] = it.split("\t")
          Student(infos(0), infos(1), infos(2), infos(3), infos(4), infos(5), infos(6), infos(7).toInt, infos(8))
        }
      )
    )

    println("1. 按照身高排序")
    originRdd.keyBy(_.height).sortByKey(false).values.foreach(println(_))

    println("2. 求平均年龄")
    originRdd.keyBy(
      it => {
        2020 - it.birth.split("-")(0).toInt
      }
    ).mapPartitions(
      _.map(
        it => {
          (1, (it._1, 1))
        }
      )
    ).reduceByKey(
      (it1, it2) => {
        (it1._1 + it2._1, it1._2 + it2._2)
      }
    ).map(
      it => {
        (it._2._1 / it._2._2)
      }
    ).foreach(println(_))

    println("3. 求学生中出现的所有姓氏")
    originRdd.keyBy(_.name.split("")(0)).reduceByKey(
      (it1, it2) => {
        it1
      }
    ).keys.foreach(println(_))

    println("4. 返回出生在每月最大天数的3人")
    originRdd.keyBy(
      _.birth.split("-")(2).toInt
    ).sortByKey(false).values.take(3).foreach(println(_))

    println("5. 索引出相同生日下同学的姓名链表")
    originRdd.map(
      it => {
        (it.birth.substring(5, 10).replace("-", ""), it.name)
      }
    ).groupByKey().map(
      it => {
        (it._1, it._2.toList)
      }
    ).foreach(println(_))

  }
  
}

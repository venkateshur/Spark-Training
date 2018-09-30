package com.spark.programs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

case class AirPorts(airPortName: String, country: String,airportNum:Int,airportCode:String)

object ReadCSV {

  def main(args:Array[String]) {

  //  val master = args(0)
    val path = args(0)
    val numberOfShows = args(1).toInt

    println("input path:" + path)
    println("recordstoShow" + numberOfShows)

    //Create String
    val String = "Ident Type Name Coordinates Elevation_ft Continent ISO_country iso_region Municipality Gps_code iata_code local_code"
    //Create Schema
    val Airport_Schema = StructType(String.split(" ").map(fieldName ⇒ StructField(fieldName, StringType, true)))

    val spark = SparkSession.builder().appName("ReadCSV").getOrCreate()

    val readCSV = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("quote", ",")
      .option("ignoreLeadingWhiteSpace", "true")
      .option("ignoreTrailingWhiteSpace", "true")
      .schema(Airport_Schema).csv(path)

    readCSV.show(numberOfShows,false)
    spark.stop()
  }

}

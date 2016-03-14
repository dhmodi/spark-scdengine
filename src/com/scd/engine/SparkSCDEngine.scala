/**
 * @author Dhaval Modi
 *
 * 
 */

package com.scd.engine

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.reflect.runtime.universe
//import org.apache.spark.sql.sqlContext.implicits._
import java.text.DecimalFormat
import org.apache.spark.sql.types._
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.sql.Date
import scala.util.{Try, Success, Failure}


/** Factory for [[com.scd.engine.SparkSCDEngine]] instances. */
object SparkSCDEngine {
	/** This is a brief description of what's being documented.
	 *
	 * This is further documentation of what we're documenting.  It should
	 * provide more details as to how this works and what it does. 
	 */
	def main(args: Array[String]) {
		if (args.length < 2) {
			System.err.println("Usage: SparkSCDEngine <host> <config_file>")
			System.exit(1)
		}
		//System.setProperty("hadoop.home.dir", "C:/Users/dhmodi/Downloads/hadoop-common-2.2.0-bin-master/hadoop-common-2.2.0-bin-master")
		val conf = new SparkConf().setAppName("SparkSCDEngine").setMaster(args(0));
		val sc = new SparkContext(conf);
		val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc);
		import sqlContext.implicits._
		val bufferedSource = scala.io.Source.fromFile(args(1));
		var srcDatabase = ""; 
		var srcTable = "";
		var tgtDatabase = "";
		var tgtTable = "";
		var tblPrimaryKey = "";
		var scdType = "";
		var loadType = "";
		var mapDataFormat:Map[String,String] = Map();
		for (line <- bufferedSource.getLines) {
			val cols = line.split(",").map(_.trim);
			for (i <- (0 to cols.length - 1)) { 
				i match {
				case 0 => srcDatabase = cols(0);
				case 1 => srcTable = cols(1);
				case 2 => tgtDatabase = cols(2);
				case 3 => tgtTable = cols(3);
				case 4 => tblPrimaryKey = cols(4);
				case 5 => scdType = cols(5);
				case 6 => loadType = cols(6);
				case 7 => { val userDataTypes = cols(7).trim.replaceAll("[\"]", "").split('|');
				for (j <- (0 to userDataTypes.length - 1)) {
					val param = userDataTypes(j).split("=");
					mapDataFormat += ( param(0) -> param(1));
				}
				}
				}
			}
			println(tgtTable);
			println(srcTable);
			println(tblPrimaryKey);
			val md5Value = sc.getConf.get("spark.scdengine.md5ValueColumn");
			val batchId = sc.getConf.get("spark.scdengine.batchIdColumn");
			val currInd = sc.getConf.get("spark.scdengine.currentIndicatorColumn");
			val startDate = sc.getConf.get("spark.scdengine.startDateColumn");
			val endDate = sc.getConf.get("spark.scdengine.endDateColumn");
			val updateDate = sc.getConf.get("spark.scdengine.updateDateColumn");
			var src=sqlContext.sql(s"select * from $srcDatabase.$srcTable");
			val tgt=sqlContext.sql(s"select * from $tgtDatabase.$tgtTable");
			val srcDataTypes = src.dtypes;
			val tgtDataTypes = tgt.drop("md5Value").drop("batchId").drop("currInd").drop("startDate").drop("endDate").drop("updateDate").dtypes;
			
			/////////// Datatype Conversion UDFs ///////////////////
			val toInt    = udf[Int, String]( _.toInt);
			val toDouble = udf[Double, String]( _.toDouble);
			val conToString = udf[String, Any]( _.toString);
			val toDecimal = udf{(out:Any) =>(new Decimal().set(BigDecimal(out.toString)))};
			val toDate = udf{(out:String, form: String) => {
				val format = new SimpleDateFormat(s"$form");
				Try(new Date(format.parse(out.toString()).getTime))match {
				case Success(t) => Some(t)
				case Failure(_) => None
				}}};
			val toTimeStamp = udf{(out:String, form: String) => {
				val format = new SimpleDateFormat(s"$form");
				Try(new Timestamp(format.parse(out.toString()).getTime))match {
				case Success(t) => Some(t)
				case Failure(_) => None
				}}};
			val toByte = udf{(in: String) => (in.toByte)};
			///////////////////////////////////
			
			
			if(srcDataTypes.deep  != tgtDataTypes.deep)
			{
				for ( x <- 0 to (tgtDataTypes.length - 1) ) {
					if (srcDataTypes(x) != tgtDataTypes(x))
					{
						val cols = tgtDataTypes(2).toString().split(",").map(_.trim);
						val columnName = cols(0).replaceAll("[()]","");
						val dataType = cols(1).toString.split("\\(")(0).replaceAll("[)]","");
						dataType match {
						case "IntegerType"  => { src = src.withColumn(s"$columnName", toInt(src(s"$columnName"))) };
						case "StringType" => { src = src.withColumn(s"$columnName", conToString(src(s"$columnName")))};
						case "DecimalType" => { src = src.withColumn(s"$columnName", toDecimal(src(s"$columnName")))};
						case "DateType" => { 
							var typeFormat = "dd-MMM-yy";
							if ( mapDataFormat.contains(s"$columnName")){ typeFormat = mapDataFormat(s"$columnName");}
							src = src.withColumn(s"$columnName", toDate(src(s"$columnName"), lit(s"$typeFormat"))) };
						case "TimestampType" => { 
							var typeFormat = "yyyy-MM-dd' 'HH:mm:ss";
							if ( mapDataFormat.contains(s"$columnName")){ typeFormat = mapDataFormat(s"$columnName");}
							src = src.withColumn(s"$columnName", toTimeStamp(src(s"$columnName"), lit(s"$typeFormat"))) };
						case whoa  => println("Unexpected case: " + whoa.toString);
						}
					}
				}
			}

			scdType match {
			case "Type1" => {
				println("SCD Type1: Unimplemented");
				val newTgt1 = tgt.as('a).join(src.as('b),tgt.col(s"$tblPrimaryKey") === src.col(s"$tblPrimaryKey"));
				var tgtFinal = tgt.except(newTgt1.select("a.*"));
				tgtFinal = tgtFinal.unionAll(src);
				tgtFinal.write.mode(SaveMode.Append).saveAsTable(s"$tgtDatabase.$tgtTable");
			};
			case "Type2" => {
				// SCD Type 2 
				val md5DF = src.map(r => (r.getAs(s"${tblPrimaryKey}").toString, r.hashCode.toString)).toDF(s"${tblPrimaryKey}",s"$md5Value");
				//	md5DF.show();
				val newSrc = src.join(md5DF,s"${tblPrimaryKey}");
				// newSrc.show();
				var tgtFinal=tgt.filter(s"$currInd" + " = 'N'"); //Add to final table
				// tgtFinal.show()
				val tgtActive=tgt.filter(s"$currInd" + " = 'Y'");
				// Check for duplicate in SRC & TGT
				val devSrc = newSrc.except(tgtActive.as('a).join(newSrc.as('b),tgtActive.col(s"$md5Value") === newSrc.col(s"$md5Value")).select("b.*").dropDuplicates());
				// devSrc.show()
				val newTgt2 = tgtActive.as('a).join(devSrc.as('b),tgtActive.col(s"${tblPrimaryKey}") === devSrc.col(s"${tblPrimaryKey}"));
				//newTgt2.show()
				tgtFinal = tgtFinal.unionAll(tgtActive.except(newTgt2.select("a.*")));
				tgtFinal = tgtFinal.unionAll(newTgt2.select("a.*").withColumn(s"$currInd", lit("N")).withColumn(s"$endDate", current_timestamp()).withColumn(s"$updateDate", current_timestamp()));
				val srcInsert = devSrc.withColumn(s"$batchId", lit("13")).withColumn(s"$currInd", lit("Y")).withColumn(s"$startDate", current_timestamp()).withColumn(s"$endDate", date_format(lit("9999-12-31 23:59:59"),"yyyy-MM-dd HH:mm:ss")).withColumn(s"$updateDate", current_timestamp());
				tgtFinal = tgtFinal.unionAll(srcInsert);
				// tgtFinal.write.mode(SaveMode.Append).saveAsTable(s"$tgtDatabase.tgt_table2");
				tgtFinal.registerTempTable(s"$tgtTable"+"_tmp")
				sqlContext.sql(s"insert overwrite table $tgtTable select * from $tgtTable" + "_tmp");
			};
						case "Type3" => {
				// SCD Type 3 
				println("Not Implemented");
			};
						case "Type4" => {
				// SCD Type 4
				println("Not Implemented");
			};
						case "Type5" => {
				// SCD Type 5 
				println("Not Implemented");
			};
						case "Type6" => {
				// SCD Type 6
				println("Not Implemented");
			};
			// catch the default with a variable so you can print it
			case whoa  => println("Unexpected case: " + whoa.toString);
			};
		}
		System.exit(0)
	}
}
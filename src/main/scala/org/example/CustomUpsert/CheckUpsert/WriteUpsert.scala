package org.example.CustomUpsert.CheckUpsert

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.functions.current_timestamp
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.DataSourceWriteOptions.{PAYLOAD_CLASS_OPT_KEY,PARTITIONPATH_FIELD_OPT_KEY,PRECOMBINE_FIELD_OPT_KEY,RECORDKEY_FIELD_OPT_KEY,OPERATION_OPT_KEY,TABLE_NAME_OPT_KEY}

object WriteUpsert {

  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("test").getOrCreate()
    val schema=new StructType().add("name",StringType,true)
                               .add("id",StringType,true)
                                .add("dept",StringType,true)
    var df = spark.read.schema(schema).csv("/Users/biswajit/PycharmProjects/hudicustomupsert/files/new.csv")
    df=df.withColumn("date",current_timestamp())
    df.write.format("org.apache.hudi")
      .options(getQuickstartWriteConfigs)
      .option("hoodie.index.type", "SIMPLE")
      .option(PAYLOAD_CLASS_OPT_KEY, "org.example.CustomUpsert.DataRecordPayload")
      .option(RECORDKEY_FIELD_OPT_KEY, "id")
      .option(PARTITIONPATH_FIELD_OPT_KEY, "dept")
      .option(TABLE_NAME_OPT_KEY, "test")
      .option(PRECOMBINE_FIELD_OPT_KEY, "date").save("/Users/biswajit/PycharmProjects/hudicustomupsert/output/")
  }
}

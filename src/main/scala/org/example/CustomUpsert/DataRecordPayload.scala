package org.example.CustomUpsert

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord, IndexedRecord}
import org.apache.hudi.common.model.{HoodieRecordPayload, OverwriteWithLatestAvroPayload}
import org.apache.hudi.common.util.Option


import java.util.Properties

class DataRecordPayload(record:GenericRecord,comparable: Comparable[_]) extends OverwriteWithLatestAvroPayload(record,comparable) {

  override def combineAndGetUpdateValue(currentValue: IndexedRecord, schema: Schema, properties: Properties): Option[IndexedRecord] = {

    val currentRecord=currentValue.asInstanceOf[GenericRecord]

    val newRecord=getInsertValue(schema).get().asInstanceOf[GenericRecord]

    val name=newRecord.get("name")
    currentRecord.put("name",name)
    Option.of(currentRecord)
  }

}
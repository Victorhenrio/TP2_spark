package fr.esme.gdpr.utils.schemas

import fr.esme.gdpr.configuration.Field
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}

object DataFrameSchema {
  def buildDataframeSchema(fields: Seq[Field]): StructType = {
    val structFieldsList = fields.map(field => {
      StructField(field.name, mapPrimitiveTypesToSparkTypes(field.`type`))
    })

    StructType(structFieldsList)
  }

  def mapPrimitiveTypesToSparkTypes(`type`: String): DataType = {
    `type` match {
      case "String" => StringType
      case "Double" => DoubleType
      case "Long" => LongType
      case "Integer" => IntegerType
      case _ => StringType
    }
  }
}

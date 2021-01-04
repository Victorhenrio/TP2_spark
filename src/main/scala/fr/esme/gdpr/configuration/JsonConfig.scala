package fr.esme.gdpr.configuration

import spray.json.{DefaultJsonProtocol, JsonFormat}


case class Field(date: String, `fillWithDaysAgo`: String)

object JsonConfigProtocol extends DefaultJsonProtocol {
  implicit val field: JsonFormat[Field] = lazyFormat(jsonFormat2(Field))
}
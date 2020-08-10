package models

final case class Country(
  id: String,
  nameEs: Option[String] = None,
  nameEn: Option[String] = None,
  icon: String,
  status: String
)

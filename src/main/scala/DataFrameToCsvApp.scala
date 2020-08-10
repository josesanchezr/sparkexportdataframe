import context.Context._
import models.{City, Country}
import org.apache.log4j.{Level, Logger}
import sparkSession.implicits._

object DataFrameToCsvApp extends App {
  // Configurar el nivel del logger
  Logger.getLogger("org").setLevel(Level.WARN)

  val countriesEn = sparkSession
    .read
    .format("csv")
    .option("header", value = true)
    .option("inferSchema", "true")
    .load("src/main/resources/countries_en.csv")

  val countriesEs = sparkSession
    .read
    .format("csv")
    .option("header", value = true)
    .option("inferSchema", "true")
    .load("src/main/resources/countries_es.csv")

  val joinCountries =
    countriesEn
      .join(countriesEs, countriesEn.col("alpha2") === countriesEs.col("alpha2"))
      .select(
        countriesEn.col("alpha2"),
        countriesEs.col("name"),
        countriesEn.col("name")
      )

  val countriesDS = joinCountries.map(country =>
    Country(
      id = country.getString(0).toUpperCase,
      nameEs = Some(country.getString(1)),
      nameEn = Some(country.getString(2)),
      icon = s"https://viajesescapados.com/icons/country/${country.getString(0)}.icon",
      status = "ACTIVE"
    )
  )

  countriesDS.show()

  countriesDS.write.format("csv").save("/path")

  val cities = sparkSession
    .read
    .format("csv")
    .option("header", value = true)
    .option("inferSchema", "true")
    .load("src/main/resources/worldcities.csv")

  val citiesDS = cities.map(city =>
    City(
      id = city.getInt(10).toString,
      nameEs = city.getString(0),
      nameEn = city.getString(0),
      countryId = city.getString(5),
      status = "ACTIVE"
    )
  )

  citiesDS.show()

  citiesDS.write.format("csv").save("/path")

  sparkSession.sparkContext.stop()
}

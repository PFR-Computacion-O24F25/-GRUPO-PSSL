# 📽 Proyecto Integrador - Programación Funcional y Reactiva

# Documentación del Proceso

## Índice
1. [Descripción de los Datos](#descripción-de-los-datos)
2. [Lectura del CSV](#lectura-del-csv)
3. [Análisis Exploratorio](#análisis-exploratorio)
4. [Limpieza](#limpieza)
5. [Trabajo con Base de Datos](#trabajo-con-base-de-datos)

---

## Descripción de los Datos
## Tablas de Datos 


| Nombre de Columna         | Tipo                     | Propósito                                        | Observaciones                                      |
|---------------------------|--------------------------|--------------------------------------------------|---------------------------------------------------|
| `adult`                   | Booleano (true/false)    | Determina si la película está destinada a un público adulto.  | Los valores posibles son "true" o "false".        |
| `belongs_to_collection`   | Cadena de texto (JSON)   | Información sobre la colección a la que pertenece la película. | Puede ser un objeto JSON o estar vacío.           |
| `budget`                  | entero            | Indica el presupuesto de la película en dólares.  | En algunos casos puede ser "0".                   |
| `genres`                  | Cadena de texto (JSON)   | Listado de géneros de la película.                | Es un array JSON que contiene objetos de géneros. |
| `homepage`                | Cadena          | URL oficial del sitio web de la película.        | Este campo puede estar vacío.                     |
| `id`                      | entero            | Identificador único de la película en la base de datos. | Actúa como clave primaria.                        |
| `imdb_id`                 | Cadena         | ID de la película en IMDb.                       | Formato "tt#####".                                |
| `original_language`       | Cadena          | Lengua original en que se realizó la película.   | Usualmente es un código de dos letras según el estándar ISO 639-1 (por ejemplo, 'en' para inglés, 'es' para español).|
| `original_title`          | Cadena           | Título original de la película antes de cualquier cambio. | Puede diferir del título oficial en algunos casos.|
| `overview`                | Cadena           | Descripción breve de la trama de la película.    | Puede estar vacío.                                |
| `popularity`              | Número decimal           | Mide el nivel de popularidad de la película.     | Es una escala relativa en base a TMDb.           |
| `poster_path`             | Cadena           | Ruta que dirige al póster de la película.        | Debe usarse con la URL base de TMDb para acceso.  |
| `production_companies`    | Cadena de texto (JSON)   | Lista de compañías que participaron en la producción. | Es un array JSON con detalles de las compañías.   |
| `production_countries`    | Cadena de texto (JSON)   | Países en los que se produjo la película.        | Es un array JSON con objetos representando países.|
| `release_date`            | entero            | Fecha en la que se estrenó la película.          | En formato "YYYY-MM-DD".                          |
| `revenue`                 | entero            | Ingresos generados por la película.              | Puede ser "0" si no ha generado ingresos.         |
| `runtime`                 | Número decimal           | Duración total de la película en minutos.        | Puede ser "0" si no se especifica la duración.    |
| `spoken_languages`        | Cadena de texto (JSON)   | Idiomas hablados en la película.                 | Es un array JSON con códigos de los idiomas.      |
| `status`                  | Cadena          | Estado actual de la película en producción.      | Ejemplos: "Released", "Post Production".          |
| `tagline`                 | Cadena          | Eslogan o lema asociado a la película.           | Puede estar vacío.                                |
| `title`                   | Cadena          | Nombre comercial utilizado para la película.     | Es el título usado para su promoción y distribución.|
| `video`                   | Booleano (true/false)    | Indica si existe contenido de video relacionado con la película. | Generalmente tiene el valor "false".             |
| `vote_average`            | Número decimal           | Promedio de las calificaciones recibidas.        | Utilizado para evaluar el éxito de la película.   |
| `vote_count`              | Número entero            | Número total de votos recibidos por la película. | Puede ser "0" si no se ha votado aún.             |
| `keywords`                | Cadena de texto (JSON)   | Palabras clave relacionadas con la película.     | Es un array JSON de palabras clave.               |
| `cast`                    | Cadena de texto (JSON)   | Lista de los actores principales de la película. | Es un array JSON con la información de los actores.|
| `crew`                    | Cadena de texto (JSON)   | Información del equipo técnico y de producción.  | Es un array JSON con datos del equipo de producción.|
| `ratings`                 | Cadena de texto (JSON)   | Opiniones y calificaciones de los usuarios.      | Incluye usuarios, sus calificaciones y fechas.    |


## Lectura del CSV
```scala
import cats.effect.IO
import kantan.csv._
import kantan.csv.ops._
import kantan.csv.generic._
import java.io.File
import scala.io.Source

import models.Movie

object ValidacionLectura extends App {
 val pathToCsv = "src/main/resources/data/pi_movies_small_cleaned.csv"

 println(s"Verificando archivo en: $pathToCsv")
 val file = new File(pathToCsv)

 if (!file.exists()) {
   println(s"❌ Error: El archivo CSV no se encuentra en la ruta especificada.")
   System.exit(1)
 }

 val csvConfig = rfc.withHeader(true).withCellSeparator(';')

 def loadCSVData(filePath: String): List[Movie] = {
   try {
     println("\n📄 Mostrando primeras 5 líneas del archivo CSV para verificar contenido:")
     Source.fromFile(filePath).getLines().take(5).foreach(println)

     println("\n🔄 Intentando leer el archivo CSV...")

     val dataSource = file.readCsv[List, Movie](csvConfig)
     println("✅ Archivo leído, procesando datos...")

     val moviesList = dataSource.collect {
       case Right(movie) =>
         println(s"✔ Película procesada correctamente: ${movie.title}")
         Some(movie)
       case Left(error) =>
         println(s" Error al procesar película: $error")
         println(s" Tipo de error: ${error.getClass.getName}")
         None
     }.flatten

     println(s"\n📊 Datos procesados. Se encontraron ${moviesList.size} películas")
     moviesList
   } catch {
     case e: Exception =>
       println(s"⚠ Error al cargar el archivo CSV: ${e.getMessage}")
       println(s" Stack trace: ${e.getStackTrace.mkString("\n")}")
       List.empty[Movie]
   }
 }

 val movies = loadCSVData(pathToCsv)

 if (movies.nonEmpty) {
   println(s"\n Total de películas cargadas: ${movies.size}")
   println("\n🎬 Primeras 3 películas:")
   movies.take(3).foreach { m =>
     println(f"""
                |ID: ${m.id}%6d
                |Título: ${m.title}%-50s
                |Fecha: ${m.release_date}%-10s
                |Rating: ${m.vote_average}%.1f
                |Géneros: ${m.genres}
                |""".stripMargin)
   }
 } else {
   println("\n No se cargaron películas.")
 }
}
```

## Análisis Exploratorio
![image](https://github.com/user-attachments/assets/c1c67967-b407-414c-8281-d601153f028e)
## Análisis de Datos Textuales

| **Etiquetas de fila**                                                  | **Cuenta de original_title** |
|------------------------------------------------------------------------|------------------------------|
| à¤°à¤•à¥à¤¤                                                           | 1                            |
| Ã€ travers la forÃªt                                                   | 1                            |
| All Access: Front Row. Backstage. Live!                                | 1                            |
| Ant-Man                                                                | 1                            |
| August Evening                                                         | 1                            |
| Baby Snakes                                                            | 1                            |
| Bachelor Flat                                                          | 1                            |
| Boris and Natasha                                                      | 1                            |
| Cambridge Spies                                                        | 1                            |
| Camp de Thiaroye                                                       | 1                            |
| Carcasses                                                              | 1                            |
| China Gate                                                             | 1                            |
| Copper Canyon                                                          | 1                            |
| Ð¨Ð¿Ð¸Ð¾Ð½                                                             | 1                            |
| Ð‘Ð°Ð±Ð½Ð¸Ðº                                                           | 1                            |
| Dear Heart                                                             | 1                            |
| Dumb and Dumber To                                                     | 1                            |
| é’çº¢                                                                 | 1                            |
| Eddie: The Sleepwalking Cannibal                                       | 1                            |
| El tren de la memoria                                                  | 1                            |
| Elsker dig for evigt                                                   | 1                            |
| Fate Is the Hunter                                                     | 1                            |
| Fighting Back                                                          | 1                            |
| Flight Command                                                         | 1                            |
| Follow Me: The Yoni Netanyahu Story                                    | 1                            |
| Gertie the Dinosaur                                                    | 1                            |
| God is the Bigger Elvis                                                | 1                            |
| Grandma Lo-Fi                                                          | 1                            |
| Harold's Going Stiff                                                   | 1                            |
| Heavenly Creatures                                                     | 1                            |
| Hillside Cannibals                                                     | 1                            |
| HistÃ³rias que sÃ³ Existem Quando Lembradas                            | 1                            |
| Holy Motors                                                            | 1                            |
| Hotel Reserve                                                          | 1                            |
| I cacciatori del cobra d'oro                                           | 1                            |
| Î†Î´Î¹ÎºÎ¿Ï‚ ÎšÏŒÏƒÎ¼Î¿Ï‚                                              | 1                            |
| Informant                                                              | 1                            |
| Julian Po                                                              | 1                            |
| La fille seule                                                         | 1                            |
| L'AntisÃ©mite                                                          | 1                            |
| Lekarstwo na miÅ‚oÅ›Ä‡                                                 | 1                            |
| Lenny Bruce: Swear to Tell the Truth                                   | 1                            |
| Live Once, Die Twice                                                   | 1                            |
| Liverpool                                                              | 1                            |
| Lobos de Arga                                                          | 1                            |
| Lola Versus                                                            | 1                            |
| Madea's Witness Protection                                             | 1                            |
| Min Avatar og mig                                                      | 1                            |
| Mock Up on Mu                                                          | 1                            |
| Mulholland Drive                                                       | 1                            |
| Nazis at the Center of the Earth                                       | 1                            |
| No Way Home                                                            | 1                            |
| Nuit #1                                                                | 1                            |
| Outpost: Black Sun                                                     | 1                            |
| Quartet                                                                | 1                            |
| Quints                                                                 | 1                            |
| Rabbit Test                                                            | 1                            |
| Radiostars                                                             | 1                            |
| Rage                                                                   | 1                            |
| Return Home                                                            | 1                            |
| Rok ÄÃ¡bla                                                            | 1                            |
| Sam Peckinpah: Man of Iron                                             | 1                            |
| Scarlett                                                               | 1                            |
| Sebastian Maniscalco: What's Wrong with People?                        | 1                            |
| Second Best                                                            | 1                            |
| See Here, Private Hargrove                                             | 1                            |
| Seven                                                                  | 1                            |
| Something to Sing About                                                | 1                            |
| Spinning Boris                                                         | 1                            |
| Stormy Weather                                                         | 1                            |
| Superman vs. The Elite                                                 | 1                            |
| Suspiros de EspaÃ±a                                                    | 1                            |
| Taxi Driver                                                            | 1                            |
| The Butterfly Room                                                     | 1                            |
| The Deep Six                                                           | 1                            |
| The Fourth Dimension                                                   | 1                            |
| The Giant Mechanical Man                                               | 1                            |
| The Gnomes' Great Adventure                                            | 1                            |
| The Great Los Angeles Earthquake                                       | 1                            |
| The Hunger Games: Catching Fire                                        | 1                            |
| The Kill Hole                                                          | 1                            |
| The Magnificent Yankee                                                 | 1                            |
| The Mountain Road                                                      | 1                            |
| The Patent Leather Kid                                                 | 1                            |
| Traces of Red                                                          | 1                            |
| Trapped in the Closet: Chapters 1-12                                   | 1                            |
| Une bouteille Ã  la mer                                                | 1                            |
| Unguarded                                                              | 1                            |
| Unicorn City                                                           | 1                            |
| Ushiro kara mae kara                                                   | 1                            |
| Verfolkt                                                               | 1                            |
| Vicky Donor                                                            | 1                            |
| Vivi o, preferibilmente, morti                                         | 1                            |
| White Cargo                                                            | 1                            |
| With Byrd at the South Pole                                            | 1                            |
| (en blanco)                                                            |                              |
| **Total general**                                                      | 95                           |



## Analisis de Peliculas 
```scala
import kantan.csv._
import kantan.csv.ops._
import kantan.csv.generic._
import java.io.File
import java.nio.file.{Files, Paths}
import scala.util.matching.Regex
import scala.math._

case class Pelicula(
                     adult: Boolean,
                     belongsToCollection: String,
                     budget: Int,
                     genres: String,
                     homepage: String,
                     id: Int,
                     imdbId: String,
                     originalLanguage: String,
                     originalTitle: String,
                     overview: String,
                     popularity: Double,
                     posterPath: String,
                     productionCompanies: String,
                     productionCountries: String,
                     releaseDate: String,
                     revenue: Int,
                     runtime: Option[Double],
                     spokenLanguages: String,
                     status: String,
                     tagline: String,
                     title: String,
                     video: Boolean,
                     voteAverage: Double,
                     voteCount: Int,
                     keywords: String,
                     cast: String,
                     crew: String,
                     ratings: String
                   )

object AnalisisPeliculas extends App {
  val dataFilePath = "data/pi_movies_complete.csv"

  // Leer el archivo CSV y filtrar las filas válidas
  val dataSource = new File(dataFilePath).readCsv[List, Pelicula](rfc.withHeader.withCellSeparator(';'))
  val validMovies = dataSource.collect { case Right(movie) => movie }
  println(s" Películas cargadas correctamente: ${validMovies.size}")

  // Eliminar duplicados por ID
  val uniqueMovies = validMovies.groupBy(_.id).map(_._2.head).toList
  println(s"✅ Películas únicas (sin duplicados): ${uniqueMovies.size} de ${validMovies.size}")

  // Función para limpiar valores JSON vacíos o inválidos
  def limpiarJson(valor: String): String =
    if (valor.trim.isEmpty || valor == "null" || valor == "[]" || valor == "{}") "" else valor

  // Expresión regular para extraer los nombres de los géneros
  val genreRegex: Regex = """'name': '([^']+)'""".r

  // Función para extraer los nombres de los géneros desde la cadena JSON-like
  def extraerNombresGeneros(jsonString: String): List[String] = {
    genreRegex.findAllMatchIn(jsonString).map(_.group(1)).toList
  }

  // Aplicar limpieza de datos y extraer nombres de géneros
  val cleanedMovies = uniqueMovies.map { movie =>
    movie.copy(
      genres = extraerNombresGeneros(limpiarJson(movie.genres)).mkString(", "),
      belongsToCollection = limpiarJson(movie.belongsToCollection),
      productionCompanies = limpiarJson(movie.productionCompanies),
      productionCountries = limpiarJson(movie.productionCountries),
      spokenLanguages = limpiarJson(movie.spokenLanguages),
      keywords = limpiarJson(movie.keywords),
      cast = limpiarJson(movie.cast),
      crew = limpiarJson(movie.crew),
      ratings = limpiarJson(movie.ratings)
    )
  }

  // Funciones estadísticas
  def promedio(lista: List[Double]): Double =
    if (lista.nonEmpty) lista.sum / lista.size else 0.0

  def desviacionEstandar(lista: List[Double]): Double = {
    val media = promedio(lista)
    val variancia = lista.map(x => pow(x - media, 2)).sum / lista.size
    sqrt(variancia)
  }

  // Extraer datos numéricos
  val runtimes = cleanedMovies.flatMap(_.runtime)
  val budgets = cleanedMovies.map(_.budget.toDouble)
  val revenues = cleanedMovies.map(_.revenue.toDouble)
  val voteAverages = cleanedMovies.map(_.voteAverage)

  println("\n Estadísticas Generales:")
  println(f" Tiempo de Ejecución: Promedio = ${promedio(runtimes)}%.2f, Desviacion = ${desviacionEstandar(runtimes)}%.2f, Min = ${runtimes.minOption.getOrElse(0.0)}, Max = ${runtimes.maxOption.getOrElse(0.0)}")
  println(f" Presupuesto: Promedio = ${promedio(budgets)}%.2f, Desviacion = ${desviacionEstandar(budgets)}%.2f, Min = ${budgets.minOption.getOrElse(0.0)}, Max = ${budgets.maxOption.getOrElse(0.0)}")
  println(f" Ingresos: Promedio = ${promedio(revenues)}%.2f, Desviacion = ${desviacionEstandar(revenues)}%.2f, Min = ${revenues.minOption.getOrElse(0.0)}, Max = ${revenues.maxOption.getOrElse(0.0)}")
  println(f" Promedios de Votos: Promedio = ${promedio(voteAverages)}%.2f, Desviacion = ${desviacionEstandar(voteAverages)}%.2f, Min = ${voteAverages.minOption.getOrElse(0.0)}, Max = ${voteAverages.maxOption.getOrElse(0.0)}")

  // Análisis de géneros
  val allGenres = cleanedMovies.flatMap(_.genres.split(", ").map(_.trim).filter(_.nonEmpty))
  val genreCounts = allGenres.groupBy(identity).mapValues(_.size).toList.sortBy(-_._2)

  val mostCommonGenre = genreCounts.headOption.map { case (name, count) => s"$name ($count películas)" }.getOrElse("No disponible")
  val leastCommonGenre = genreCounts.lastOption.map { case (name, count) => s"$name ($count películas)" }.getOrElse("No disponible")

  println("\n Análisis de Géneros:")
  println(s" Género más común: $mostCommonGenre")
  println(s" Género menos frecuente: $leastCommonGenre")

  // Guardar datos limpios en CSV
  val outputFilePath = "data/pi_movies_cleaned.csv"
  val cleanedCsv = cleanedMovies.map { movie =>
    movie.productIterator.map {
      case Some(value) => value.toString
      case None => ""
      case value => value.toString
    }.mkString(";")
  }.mkString("\n")

  Files.write(Paths.get(outputFilePath), cleanedCsv.getBytes)
  println(s"\n✅ Archivo limpio guardado en: $outputFilePath")
}
```
## Resultado
![image](https://github.com/user-attachments/assets/ea235f76-3b11-43e7-bb6f-4553d0789f6d)

## Limpieza
```scala
package services

import kantan.csv._
import kantan.csv.ops._
import kantan.csv.generic._
import models.movie
import java.io.File
import play.api.libs.json._

object DataCleaner extends App {

  // Función para limpiar JSON y eliminar duplicados
  def cleanJsonArray(jsonStr: String): String = {
    try {
      val jsonArray = Json.parse(jsonStr).as[List[JsValue]]
      Json.toJson(jsonArray.distinct).toString()
    } catch {
      case _: Exception => "[]" // Si hay error en el JSON, devuelve lista vacía
    }
  }

  try {
    // Definir la ruta del archivo CSV de entrada
    val pathToCsv = "src/main/resources/data/pi_movies_complete.csv"

    // Leer el archivo CSV y convertirlo en una lista de objetos Movie
    val dataSource = new java.io.File(pathToCsv).readCsv[List, movie](rfc.withHeader(true).withCellSeparator(';'))

    // Separar los datos correctos e incorrectos durante la lectura
    val movies_ok = dataSource.collect { case Right(movie) => movie }  // Filas válidas
    val movies_fail = dataSource.collect { case Left(error) => error } // Filas con errores de formato

    // Mostrar estadísticas sobre la lectura del archivo
    println(s"Filas totales: ${dataSource.size}")
    println(s"Filas correctamente leidas: ${movies_ok.size}")
    println(s"Filas incorrectamente leidas: ${movies_fail.size}")

    // Eliminar duplicados basándose en el ID de la película
    val movies_no_duplicates = movies_ok.distinct.groupBy(.id).map { case (, movies) =>
      movies.head // Se mantiene solo la primera aparición de cada ID
    }.toList
    println(s"Filas después de eliminar duplicados: ${movies_no_duplicates.size}")

    // Limpieza de datos: manejo de valores nulos, espacios en blanco e inconsistencias
    val movies_cleaned = movies_no_duplicates.map(movie =>
      movie.copy(
        adult = movie.adult.toString.toLowerCase match {
          case "true" => true
          case "false" => false
          case _ => false
        },
        belongsToCollection = movie.belongsToCollection.trim match {
          case "" => "[]"
          case s if s.startsWith("{") && s.endsWith("}") => s"[$s]"
          case s => s
        },
        // Asigna el presupuesto solo si es mayor o igual a 0, de lo contrario, asigna 0
        budget = if (movie.budget >= 0) movie.budget else 0,

        genres = cleanJsonArray(movie.genres),

        // Convierte los ingresos a String, elimina los puntos (para manejar separadores de miles) y los convierte a Long
        // Si ocurre un error de formato, asigna 0L
        revenue = try movie.revenue.toString.replace(".", "").toLong catch { case _: NumberFormatException => 0L },

        // Convierte la duración de la película a Int, si hay un error asigna 0
        runtime = try movie.runtime.toString.toInt catch { case _: NumberFormatException => 0 },

        // Convierte la calificación promedio a Double, si hay un error asigna 0.0
        voteAverage = try movie.voteAverage.toString.toDouble catch { case _: NumberFormatException => 0.0 },

        // Convierte el número de votos a Int, si hay un error asigna 0
        voteCount = try movie.voteCount.toString.toInt catch { case _: NumberFormatException => 0 },

        // Verifica si la página web no es nula ni vacía, si lo es, asigna "No especificado"
        homepage = Option(movie.homepage).filter(_.trim.nonEmpty).getOrElse("No especificado"),

        // Verifica si el idioma original tiene exactamente dos letras minúsculas, si no, asigna "Desconocido"
        originalLanguage = Option(movie.originalLanguage).filter(_.matches("^[a-z]{2}$")).getOrElse("Desconocido"),

        // Verifica si el título original no es nulo ni vacío, si lo es, asigna "Título desconocido"
        originalTitle = Option(movie.originalTitle).filter(_.trim.nonEmpty).getOrElse("Título desconocido"),

        // Verifica si la descripción no es nula ni vacía, si lo es, asigna "Sin descripción"
        overview = Option(movie.overview).filter(_.trim.nonEmpty).getOrElse("Sin descripción"),

        // Verifica si la fecha de estreno no es nula ni vacía, si lo es, asigna "Fecha desconocida"
        releaseDate = Option(movie.releaseDate).filter(_.trim.nonEmpty).getOrElse("Fecha desconocida"),

        // Si el estado no está definido, asigna "Released"
        status = Option(movie.status).getOrElse("Released"),

        // Verifica si el tagline no es nulo ni vacío, si lo es, asigna "Sin tagline"
        tagline = Option(movie.tagline).filter(_.trim.nonEmpty).getOrElse("Sin tagline"),

        // Verifica si el ID de IMDb sigue el formato "tt" seguido de 7 dígitos, si no, asigna "Sin IMDB"
        imdbId = Option(movie.imdbId).filter(_.matches("^tt\\d{7}$")).getOrElse("Sin IMDB"),

        productionCompanies = cleanJsonArray(movie.productionCompanies),

        productionCountries = cleanJsonArray(movie.productionCountries),

        spokenLanguages = cleanJsonArray(movie.spokenLanguages),

        keywords = cleanJsonArray(movie.keywords),


        cast = cleanJsonArray(movie.cast),

        crew = cleanJsonArray(movie.crew),

        ratings = cleanJsonArray(movie.ratings)
      )
    )

    println("Valores nulos o inconsistentes reemplazados.")

    case class MovieBasicClean(
                                id: Int,
                                title: String,
                                original_title: String,
                                original_language: String,
                                release_date: String,
                                overview: String,
                                budget: Int,
                                revenue: Long,
                                runtime: Int,
                                popularity: Double,
                                vote_average: Double,
                                vote_count: Int,
                                adult: Boolean,
                                video: Boolean,
                                status: String,
                                tagline: String,
                                homepage: String,
                                imdb_id: String,
                                belongs_to_collection: String,
                                genres: String,
                                production_companies: String,
                                production_countries: String,
                                spoken_languages: String,
                                keywords: String,
                                cast: String,
                                crew: String,
                                ratings: String
                              )

    val movies_cleaned_transformed = movies_cleaned.map(movie => MovieBasicClean(
      movie.id,
      movie.title,
      movie.originalTitle,
      movie.originalLanguage,
      movie.releaseDate,
      movie.overview,
      movie.budget,
      movie.revenue,
      movie.runtime,
      movie.popularity,
      movie.voteAverage,
      movie.voteCount,
      movie.adult,
      movie.video,
      movie.status,
      movie.tagline,
      movie.homepage,
      movie.imdbId,
      movie.belongsToCollection,
      movie.genres,
      movie.productionCompanies,
      movie.productionCountries,
      movie.spokenLanguages,
      movie.keywords,
      movie.cast,
      movie.crew,
      movie.ratings)
    )

    // Mostrar los datos limpios en pantalla
    movies_cleaned_transformed.take(15).foreach(println)

    val outputPath = "src/main/resources/data/pi_movies_finish.csv"
    new File(outputPath).writeCsv(movies_cleaned_transformed, rfc.withHeader(
      "id", "title", "original_title", "original_language", "release_date",
      "overview", "budget", "revenue", "runtime", "popularity", "vote_average", "vote_count",
      "adult", "video", "status", "tagline", "homepage", "imdb_id", "belongs_to_collection", "genres",
      "production_companies", "production_countries", "spoken_languages", "keywords", "cast", "crew", "ratings"
    ).withCellSeparator(';'))

    println(s"Archivo limpio guardado en $outputPath.")

  } catch {
    case e: Exception =>
      println(s"Se produjo un error inesperado en Basic Clean: ${e.getMessage}")
      e.printStackTrace()
  }
}
```
## Trabajo con Base de Datos
## Insercion de datos Ratings
```scala
import com.opencsv.{CSVParserBuilder, CSVReaderBuilder}

import java.io.FileReader
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import scala.util.Try

case class Rating(
                   movie_id: Int,
                   userId: Int,
                   rating: Double,
                   timestamp: Long)

object insertRatings {
  def main(args: Array[String]): Unit = {
    val jdbcUrl = "jdbc:mysql://localhost:3306/DataBaseMoviesPROG"
    val username = "root"
    val password = "Sirio.1203"
    val csvFilePath = "src/main/resources/data/ratings.csv" // Ruta del archivo CSV
    var connection: Connection = null

    try {
      connection = DriverManager.getConnection(jdbcUrl, username, password)
      val parser = new CSVParserBuilder().withSeparator(';').build()
      val reader = new CSVReaderBuilder(new FileReader(csvFilePath))
        .withCSVParser(parser)
        .build()

      reader.readNext() // Saltar la cabecera del archivo CSV

      var nextLine: Array[String] = null
      while ({ nextLine = reader.readNext(); nextLine != null }) {
        Try {
          // Normalizamos los datos de la fila (rellenar columnas faltantes)
          val normalizedLine = normalizeRow(nextLine, 4, "0") // Aseguramos 4 columnas: movie_id, userId, rating, timestamp

          // Creamos una instancia de Rating
          val ratingRecord = Rating(
            movie_id = safeToInt(normalizedLine(0).trim),
            userId = safeToInt(normalizedLine(1).trim),
            rating = safeToDouble(normalizedLine(2).trim),
            timestamp = safeToLong(normalizedLine(3).trim)
          )

          // Insertar el registro en la base de datos
          insertRatingRecord(connection, ratingRecord)
        }.recover {
          case ex: Exception =>
            println(s"Error procesando la fila: ${nextLine.mkString(";")}")
            ex.printStackTrace()
        }
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (connection != null) connection.close()
    }
  }

  /**
   * Inserta un registro de tipo Rating en la base de datos si no existe.
   */
  def insertRatingRecord(connection: Connection, rating: Rating): Unit = {
    // Verificar si el registro ya existe
    if (!recordExists(connection, rating.movie_id, rating.userId, rating.timestamp)) {
      val sql = """
        INSERT INTO ratings (
          `movie_id`, `user_id`, `rating`, `timestamp`
        ) VALUES (?, ?, ?, ?)
      """
      val statement: PreparedStatement = connection.prepareStatement(sql)
      statement.setInt(1, rating.movie_id)
      statement.setInt(2, rating.userId)
      statement.setDouble(3, rating.rating)
      statement.setLong(4, rating.timestamp)
      statement.executeUpdate()
      println(s"Registro insertado: $rating")
    } else {
      println(s"Registro duplicado, no se insertará: $rating")
    }
  }

  /**
   * Verifica si ya existe un registro en la tabla `ratings` con una combinación dada de `movie_id`, `userId`, y `timestamp`.
   */
  def recordExists(connection: Connection, movie_id: Int, userId: Int, timestamp: Long): Boolean = {
    val checkSql = """
      SELECT 1 FROM ratings WHERE movie_id = ? AND user_id = ? AND timestamp = ? LIMIT 1
    """
    val statement: PreparedStatement = connection.prepareStatement(checkSql)
    statement.setInt(1, movie_id)
    statement.setInt(2, userId)
    statement.setLong(3, timestamp)
    val resultSet: ResultSet = statement.executeQuery()
    val exists = resultSet.next()
    resultSet.close()
    statement.close()
    exists
  }

  /**
   * Normaliza un arreglo para que tenga un número fijo de columnas. Llena las columnas faltantes con un valor predeterminado.
   */
  def normalizeRow(row: Array[String], requiredLength: Int, defaultValue: String): Array[String] = {
    if (row.length >= requiredLength) row
    else row ++ Array.fill(requiredLength - row.length)(defaultValue)
  }

  /**
   * Convierte una cadena de texto en un entero de manera segura, devolviendo 0 si no es válido.
   */
  def safeToInt(value: String): Int = {
    try {
      if (value == null || value.trim.isEmpty) 0 else value.trim.toInt
    } catch {
      case _: NumberFormatException => 0
    }
  }

  /**
   * Convierte una cadena de texto en un número decimal de manera segura, devolviendo 0.0 si no es válido.
   */
  def safeToDouble(value: String): Double = {
    try {
      if (value == null || value.trim.isEmpty) 0.0 else value.trim.toDouble
    } catch {
      case _: NumberFormatException => 0.0
    }
  }

  /**
   * Convierte una cadena de texto en un entero largo (long) de manera segura, devolviendo 0L si no es válido.
   */
  def safeToLong(value: String): Long = {
    try {
      if (value == null || value.trim.isEmpty) 0L else value.trim.toLong
    } catch {
      case _: NumberFormatException => 0L
    }
  }
}
```

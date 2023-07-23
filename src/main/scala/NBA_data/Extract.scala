package NBA_data

import java.io.{File, PrintWriter}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.explode
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.URI
import java.time.Duration

object Extract extends App {
  // Initialize SparkSession to enable Spark functionalities
  val spark = SparkSession
    .builder()
    .appName("Extract")
    .config("spark.master", "local") // without this I get an error message
    .getOrCreate()

  import spark.implicits._

  // Function to fetch API response headers for understanding the structure of the API's response
  def fetchAPIresponse(url: String): Unit = {
    // Create a new HTTP client
    val client = HttpClient.newHttpClient()
    // Build the HTTP request
    val request = HttpRequest.newBuilder()
      .uri(URI.create(url))
      .timeout(Duration.ofSeconds(5))
      .GET()
      .build()

    // Send the request and get the response
    val response = client.send(request, HttpResponse.BodyHandlers.ofString())
    // Print all headers
    println(response.headers().map())
  }

  val url2 = "https://www.balldontlie.io/api/v1/stats?per_page=100&game_ids[]=473779&page=1"
  fetchAPIresponse(url2)
  // There are no headers like X-RateLimit-Limit, X-RateLimit-Remaining, or X-RateLimit-Reset, which are typically used to convey rate limit information.

  // Function to fetch JSON data from a given URL, with retry mechanism for HTTP 429 errors
  def fetchJsonFromUrl(url: String): String = {
    // Create a new HTTP client
    val client = HttpClient.newHttpClient()
    // Build the HTTP request
    val request = HttpRequest.newBuilder()
      .uri(URI.create(url))
      .timeout(Duration.ofSeconds(5))
      .GET()
      .build()

    // Initialize retry counter
    var retries = 0
    val maxRetries = 5

    // Retry loop
    while (retries < maxRetries) {
      // Send the request and get the response
      val response = client.send(request, HttpResponse.BodyHandlers.ofString())

      // If HTTP 429 error, wait and retry
      if (response.statusCode() == 429) {
        retries += 1
        println(s"Got HTTP 429 response. Waiting and retrying... ($retries/$maxRetries)")
        Thread.sleep(10000) // wait for 10 seconds before retrying
      } else {
        // If no error, return the response body
        return response.body()
      }
    }
    // return an empty string if fetching fails after maxRetries
    ""
  }

  // Function to read a JSON file and return the total number of pages from the metadata
  def total_pages(fileName: String): Int = {
    val readJson = ujson.read(fileName)
    val totalPages = readJson("meta")("total_pages").num.toInt
    return totalPages
  }

  // Function to save a JSON string to a local file
  // jsonFileString: the name of the json file as string to save
  // fileNameOutput: the name of the output file
  // directory is name of the folder were to write in
  def saveLocally(jsonFileString: String, fileNameOutput:String, directory: String): Unit = {
    val file = new File(directory, fileNameOutput) // the file will be crated inside this directory
    val writer = new PrintWriter(file)
    writer.write(jsonFileString)
    writer.close()
  }

  // Function to extract team ID and name from a JSON string
  def extract_team_id_name(team_page_json: String): List[(Int, String)] = {
    val readJson = ujson.read(team_page_json)
    val dataArr = readJson("data").arr
    //println(dataList)

    val dataList = dataArr.map { item =>
      val teamId = item("id").num.toInt
      val fullName = item("full_name").str
      (teamId, fullName)
    }.toList

    dataList

  }

  // Define the base URL for fetching team data
  val teams_url = "https://www.balldontlie.io/api/v1/teams?per_page=100"

  // Fetch the first page of team data
  val url_first_team = teams_url + "&page=1"
  val teams_first_page_1 = fetchJsonFromUrl(url_first_team)

  // Extract team ID and name from the first page of team data
  var teamsList: List[(Int, String)] = extract_team_id_name(teams_first_page_1)

  // Get the total number of pages of team data
  val teams_nb_pages = total_pages(teams_first_page_1)

  // Fetch all pages of team data and append to the team list

  // Start from page 2
  var page = 2

  while (page<=teams_nb_pages){
    // Update url
    val url_i = teams_url + s"&page=$page"

    // Fetch json
    val teams_page_i = fetchJsonFromUrl(url_i)

    // Create list from the fetched json
    val teamsList_i = extract_team_id_name(teams_page_i)

    // Append new data to list
    teamsList = teamsList ++ teamsList_i

    page += 1
  }

  // Print the final list of teams
  println(teamsList)

  // Define the list of team names we're interested in
  val teamNames = List("Phoenix Suns", "Atlanta Hawks", "LA Clippers", "Milwaukee Bucks")

  // Filter the team list to only include the teams we're interested in
  val selectedTeamIds = teamsList.filter{ case (_, name) => teamNames.contains(name) }.map{ case (id, _) => id }

  // Define the base URL for fetching game data
  val Games_url = "https://www.balldontlie.io/api/v1/games?per_page=100"

  // Define the seasons we're interested in
  val seasons = List(2020, 2021)

  // For each team and season, generate a list of URLs to fetch game data
  val list_game_urls: List[(Int, Int, String)] = selectedTeamIds
    .flatMap(teamId => seasons
      .map(season => (teamId, season, s"$Games_url&seasons[]=$season&team_ids[]=$teamId&page=1")
      )
    )

  // For each team and season, return a tuple: team_id, season, total nb pages
  val games_total_pages = list_game_urls
    .map{ case (team_id, season, first_url) => (team_id, season, total_pages(fetchJsonFromUrl(first_url)))}
  println(games_total_pages) // List((1,2020,1), (1,2021,1), (13,2020,1), (13,2021,1), (17,2020,1), (17,2021,1), (24,2020,1), (24,2021,2))

  // Create a directory for storing game data
   val gamesDirectory = os.pwd / "Games"
   if (!os.exists(gamesDirectory)) {
     os.makeDir.all(gamesDirectory)
     println(s"Directory $gamesDirectory created.")
   } else {
     println(s"Directory $gamesDirectory already exists.")
   }

  // For each team and season, generate a list of URLs for all pages of game data
  val games_total_pages_with_range = games_total_pages
    .map{case(team_id, season, total_pages)=>(team_id, season, 1 until total_pages + 1)}
  println(games_total_pages_with_range) // List((1,2020,Range 1 until 2), (1,2021,Range 1 until 2),

  // Flatten the list of URLs
  val flatten_games_total_pages_with_range = games_total_pages_with_range
    .flatMap{ case (a, b, range) => range.map(c => (a, b, c)) }
  println(flatten_games_total_pages_with_range)

  // Fetch game data from each URL and save it locally
  flatten_games_total_pages_with_range.map{case(team_id, season, page) => saveLocally(
      fetchJsonFromUrl(s"$Games_url&seasons[]=$season&team_ids[]=$team_id&page=$page"),
        s"Game_team${team_id}_page_${page}.json","Games")}

  // Create a directory for storing stats data
  val statsDirectory = os.pwd / "Stats"
  if (!os.exists(statsDirectory)) {
    os.makeDir.all(statsDirectory)
    println(s"Directory $statsDirectory created.")
  } else {
    println(s"Directory $statsDirectory already exists.")
  }

  // Read the local game data files using Spark
  val directory = "Games"
  val gameDF: DataFrame = spark.read.json(s"$directory/*.json")
  gameDF.show(10)
  gameDF.printSchema()

  // Explode the data in the DataFrame to flatten it
  val gameDFexplode = gameDF.select((explode($"data")))
    .withColumn("game_id", $"col.id")
    .withColumn("home_team_name", $"col.home_team.full_name")
    .withColumn("home_team_id", $"col.home_team.id")
    .withColumn("home_team_score", $"col.home_team_score")
    .withColumn("visitor_team_name", $"col.visitor_team.full_name")
    .withColumn("visitor_team_id", $"col.visitor_team.id")
    .withColumn("visitor_team_score", $"col.visitor_team_score")
    .drop($"col")
  gameDFexplode.printSchema()

  // Fetch stats data for each game and save it locally
  val Stats_url = "https://www.balldontlie.io/api/v1/stats?per_page=100"

  // Create a list of game ids related to the selected teams and seasons from gameDFexplode
  // For each game, return a tuple: team_id, season, total nb pages
  val selectedGameIds = gameDFexplode.select("game_id").rdd.map(row => row.getLong(0)).collect().toList
  println(selectedGameIds)
  println(selectedGameIds.size)

  // todo I'm getting HTTP 429 for the code below therefore I will consider the first 5 games for now
  // Create a list of URLs to fetch stats data for the first 5 games
  val first_5_games = selectedGameIds.take(5)

  // Create a list of tuples (gameId, url, total_pages) with links to first page
  val list_stats_urls: List[(Long, String, Int)] = first_5_games
    .map(gameId => (gameId, s"$Stats_url&game_ids[]=$gameId",total_pages(fetchJsonFromUrl(s"$Stats_url&game_ids[]=$gameId")))
    )
  println(list_stats_urls)

  // For each game, generate a list of URLs for all pages of stats data (adding until)
  val list_stats_urls_with_range = list_stats_urls
    .map { case (gameId, url, total_pages) => (gameId, url, 1 until total_pages + 1) }
  println(list_stats_urls_with_range) // List((1,2020,Range 1 until 2), (1,2021,Range 1 until 2),

  // Flatten the list of URLs
  val flatten_list_stats_urls_with_range = list_stats_urls_with_range
    .flatMap { case (a, b, range) => range.map(c => (a, b, c)) }
  println(flatten_list_stats_urls_with_range)

  // Fetch stats data from each URL and save it locally
  flatten_list_stats_urls_with_range.map { case (gameId, url, page) =>
    saveLocally(
     fetchJsonFromUrl(url),
     s"Stats_game${gameId}_page_${page}.json",
    "Stats")
   }

  // Fetch and save stats data in batches to avoid overloading the API
  val batchSize = 2
  val pauseBetweenBatches = 5000 // pause for 5 seconds between batches

  flatten_list_stats_urls_with_range.grouped(batchSize).foreach { case batch =>
     batch.foreach { case (gameId, url, page) =>
       println("Now processing gameId" + gameId)

       saveLocally(
         fetchJsonFromUrl(url),
         s"Stats_game${gameId}_page_${page}.json",
         "Stats")
     }
    Thread.sleep(pauseBetweenBatches)

  }

  // Stop the SparkSession to free up resources
  spark.stop()
}
package NBA_data

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.explode

object Transform extends App {
  // Create a SparkSession

  val spark = SparkSession
    .builder()
    .appName("Transform")
    .config("spark.master", "local") // without this I get an error message
    .getOrCreate()

  import spark.implicits._

  // Define the directory where game data is stored
  val directory = "Games"

  // Read the game data from JSON files into a DataFrame
  val gamesDF: DataFrame = spark.read.json(s"$directory/*.json")
  gamesDF.show(10)
  gamesDF.printSchema()

  // Explode the data in the DataFrame to flatten it
  val gameDFexplode = gamesDF.select((explode($"data")))
    .withColumn("game_id", $"col.id")
    .withColumn("home_team_name", $"col.home_team.full_name")
    .withColumn("home_team_id", $"col.home_team.id")
    .withColumn("home_team_score", $"col.home_team_score")
    .withColumn("visitor_team_name", $"col.visitor_team.full_name")
    .withColumn("visitor_team_id", $"col.visitor_team.id")
    .withColumn("visitor_team_score", $"col.visitor_team_score")
    .drop($"col")
  gameDFexplode.printSchema()

  // Define the directory where stats data is stored
  val directory_stats = "Stats"

  // Read the stats data from JSON files into a DataFrame
  val statsDF: DataFrame = spark.read.json(s"$directory_stats/*.json")
  statsDF.show(10)
  statsDF.printSchema()

  // Explode the data in the DataFrame to flatten it
  val statsDFexplode = statsDF.select((explode($"data")))
    .withColumn("game_id_stats", $"col.game.id")
    .withColumn("player_id", $"col.player.id")
    .withColumn("player_team_id", $"col.team.id")
    .withColumn("total_pts_scored", $"col.pts")
    .withColumn("nb_steals", $"col.stl")
    .drop($"col")

  statsDFexplode.show(5)
  statsDFexplode.printSchema()

  // Merge the game and stats DataFrames on the game ID
  val merged_df_games = gameDFexplode
    .join(statsDFexplode,gameDFexplode("game_id")===statsDFexplode("game_id_stats"))
    .drop("game_id_stats")
    .drop("player_team_id")

  // Merge the game and stats DataFrames on the game ID
  merged_df_games.show(5)

  // Save the merged DataFrame as a CSV file
  val outputPath = "TransformMergeDF.csv"
  merged_df_games.write
    .format("csv")
    .option("header", "true") // Set to "false" if your data doesn't have a header
    .option("delimiter", ",") // Change this if you want a different delimiter
    .save(outputPath)

  // Stop the SparkSession
  spark.stop()

}

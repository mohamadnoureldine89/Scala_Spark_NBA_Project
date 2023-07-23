# NBA Data Extraction and Transformation

This project consists of two applications, `Extract` and `Transform`, that fetch NBA game and stats data from balldontlie's API, save it locally, and then transform and merge the data for further analysis.

## Extract

The `Extract` application fetches data from the `balldontlie.io` API. It fetches game and team data for specific teams and seasons, handling API rate limits by implementing retries. The fetched data is saved locally as JSON files.

### Running Extract

To run the `Extract` application, you need to have Spark installed and configured on your machine. Once you have Spark set up, you can run the application using the `spark-submit` command.

## Transform

The `Transform` application reads the locally saved JSON files into Spark DataFrames. It then explodes the data to flatten it and creates new DataFrames with specific columns of interest. It merges the game and stats DataFrames based on the game ID. The merged DataFrame is then saved locally as a CSV file.

### Running Transform

Similar to the `Extract` application, you need to have Spark installed and configured on your machine to run the `Transform` application. Once you have Spark set up, you can run the application using the `spark-submit` command.

## Dependencies

- scala Version 2.12.15
- sbt.version = 1.9.2
- Apache Spark Core 3.2.1
- Apache Spark SQL 3.3.2
- uPickle 3.1.0
- OS-Lib 0.9.0

## Future Work

- Implement error handling for API requests.
- Add functionality to fetch data for any selected teams and seasons.
- Implement data cleaning in the `Transform` application.
- Add functionality to load the transformed data into a database for easy querying and analysis.

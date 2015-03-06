##### Initialization

rm(list = ls())                 # Clear the history from the previous run (i.e. variables)
cat("\014")                     # Clear console
closeAllConnections()           # Close any file connections if any
dev.off()                       # Clear all graphs in the plot area

#Set the system parameters and environments variables
hcmd <-system("which hadoop", intern = TRUE)  # Find hadoop location and store it in hcmd
Sys.setenv(HADOOP_CMD=hcmd)                   # Set HADOOP_CMD environment variable to the hadoop location

hstreaming <- system("find /usr -name hadoop-streaming*jar", intern=TRUE)     # Find hadoop streaming java application's location
Sys.setenv(HADOOP_STREAMING= hstreaming[1])                                   # Set HADOOP_STREAMING environment variable to the hadoop streaming location

Sys.getenv("HADOOP_CMD")          # Retrieve environment variable from HADOOP_CMD
Sys.getenv("HADOOP_STREAMING")    # Retrieve environment variable from HADOOP_STREAMING

library(rmr2)   # Load rmr2 library
library (rhdfs) # Load rhdfs library
hdfs.init()     # Initialize hadoop

##### Initialization.END

##### Data Initialization

# Directories

# Two possible chess move formats. UCI seems easier for analysis,
# SAN is easier for display. These should be in your hadoop directory.
gamedata <- "/user/chess/data.pgn"
gamedata_uci <- "/user/chess/traindata_uci.pgn"
engine_score <- "/user/chess/stockfish.csv"

# Definition of Data Format

gamedata_uci.classes <-
  c(
    GameNumber  = "character",  Source    = "factor",	
    Date  = "factor",			Round       = "factor",	
    WhiteName = "factor", 		BlackName = "factor",
    Result      = "factor",		WhiteElo  = "character",  	
    BlackElo 	= "character",	Moves = "character"
  )

# Custom format for MapReduce tasks

gamedata_uci.format <-
  make.input.format(
    "csv",
    sep = ",",
    colClasses = gamedata_uci.classes,
    col.names = names(gamedata_uci.classes)
  )

##### Data Initialization.END

##### Data Processing

## Read Data 

#TODO Decided between mapreduce and from.dfs reading method. Or take this out.

# Start MapReduce function to read
mapreduce.chess <- mapreduce(
  input = gamedata_uci,       	    # Input File
  input.format = gamedata_uci.format	# Input Format
)

# # Store values from MapReduce output
mapreduce.chess.values <- values(from.dfs(mapreduce.chess))

# # View Data
View(mapreduce.chess.values)

## Read Data.END

## Filter Data

#TODO 	Make filter remove extraneous characters (result, Square brackets,
#		quotes...)
map.chess.filter<-function(.,lines){
  value <- cbind( lines$GameNumber, lines$WhiteElo, lines$BlackElo, lines$Moves)
  keyval(lines$GameNumber, value)
}

reduce.chess.filter <- function(key,values)
{
  keyval(key, values)
}

# Start MapReduce function
mapreduce.chess.filter <- mapreduce(
  input = gamedata_uci,       	    # Input File
  input.format = gamedata_uci.format,	# Input Format
  map = map.chess.filter#,             # Map Function
  #reduce = reduce.chess.filter 		# Reduce Function
)

# Store keys and values from MapReduce output
mapreduce.chess.filter.values <- values(from.dfs(mapreduce.chess.filter))
mapreduce.chess.filter.keys <-  keys(from.dfs(mapreduce.chess.filter))

# View Data
View(mapreduce.chess.filter.values)

## Filter Data.END

##### Data Processing.END



# Define the delta average mapreduce function.
findDeltaAverages = function(input)
{
  # Define the mapper.
  findDeltaAverages_map = function(., lines)
  {
    # Get the ELOs.
    whiteElo <- lines$WhiteElo;
    blackElo <- lines$BlackElo;
    
    # Split the Scores up into a vector.
    movesVector <- strsplit(lines$Scores, " ");
    
    # Determine the deltas based on the scores.
    deltasVector <- c();
    for(i in seq(2, count(movesVector), 1))
    {
      deltasVector[i] <- movesVector[i] - movesVector[i-1];
    }
    
    # Determine the average delta for each player.
    numWhiteMoves <- 0;
    numBlackMoves <- 0;
    totalWhiteDelta <- 0;
    totalBlackDelta <- 0;
    for(i in 1:count(deltasVector))
    {
      # Black Move
      if (i %% 2 != 0)
      {
        numBlackMoves <- numBlackMoves + 1;
        totalBlackDelta <- totalBlackDelta + deltasVector[i];
      }
      # White Move
      else
      {
        numWhiteMoves <- numWhiteMoves + 1;
        totalWhiteDelta <- totalWhiteDelta + deltasVector[i];
      }
    }
    averageWhiteDelta <- totalWhiteDelta / numWhiteMoves;
    averageBlackDelta <- totalBlackDelta / numBlackMoves;
    
    # Key = Elo, Value = Average Delta
    keyvalList <- c.keyval();
    keyvalList <- c.keyval(keyvalList, keyval(whiteElo, averageWhiteDelta));
    keyvalList <- c.keyval(keyvalList, keyval(blackElo, averageBlackDelta));
    return(keyvalList);
  }
  
  # Define the reducer.
  findDeltaAverages_reduce = function(key, values)
  {
    # Key = Elo, Value = Average(Average Delta)
    if (count(values) != 0)
    {
      averageDeltas <- sum(values, na.rm = TRUE) / count(values);
    }
    else
    {
      averageDeltas <- NA;
    }
    value <- cbind(key, averageDeltas);
    keyval(key, value);
  }
  
  # Bind the mapper and reducer to a mapreduce call.
  mapreduce(
    input         = input,
    input.format  = gamedata_uci.format,
    map           = findDeltaAverages_map,
    reduce        = findDeltaAverages_reduce
  )
}

# Run the mapreduce to get the data.
#results <- findDeltaAverages(gamedata_uci);
#results_keys <- keys(from.dfs(results));
#results_values <- values(from.dfs(results));
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
    GameNumber  = "factor",  Source    = "factor",   Date  = "factor",
    Round       = "factor",   WhiteName = "factor",   BlackName = "factor",
    Result      = "factor",   WhiteElo  = "integer",  BlackElo = "factor",
    EmptyLine   = "factor",   Moves     = "character",EmptyLine2 = "factor"
  )

# Custom format for MapReduce tasks

gamedata_uci.format <-
  make.input.format(
    "csv",
    sep = "\n",
    colClasses = gamedata_uci.classes,
    col.names = names(gamedata_uci.classes)
  )

##### Data Initialization.END

##### Data Reading

gamedata_uci.hdfs<-from.dfs(gamedata_uci,format=gamedata_uci.format)


#stream_chess <-from.dfs(
#  mapreduce(
#    input=gamedata_uci,
#    input.format=gamedata_uci.format
#  )
#)
#stream_chess_df<-values(stream_chess) ## display the data




##### Data Reading.END


##### Data Processing

# Test, get highest rate white player.

# map.chess<-function(.,lines){
#   keyval("WhiteElo", lines[8])        # Return the 8th element of the lines (WhiteElo)
# }

# reduce.chess<-function (key,values){
#   keyval("WhiteElo", max(values))     # Return the max value of the WhiteElo
# }

# # Start MapReduce function
# mapreduce.chess <- mapreduce(
#   input = gamedata,                     # Input File
#   input.format = game.format,           # Input Format
#   map = map.chess,                      # Map Function
#   reduce = reduce.chess                 # Reduce Function
# )

# Store keys and values from MapReduce output
# mapreduce.chess.values <- values(from.dfs(mapreduce.chess))
# mapreduce.chess.keys <-  keys(from.dfs(mapreduce.chess))


##### Data Processing.END
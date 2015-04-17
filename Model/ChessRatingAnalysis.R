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


########################################
### Read in the scores for the games.
########################################

# Define the score file format.
scoreData_classes <- c(GameNumber = "character", Score = "character");
scoreData_format <- make.input.format(
  "csv",
  sep = ",",
  colClasses = scoreData_classes,
  col.names = names(scoreData_classes)
);

# Define the delta average mapreduce function.
# TODO: Ignore the first line (headers).
findDeltaAverages = function(input)
{
  # Define the mapper.
  findDeltaAverages_map = function(., lines)
  {
    # Get the line inputs.
    gameNumber <- as.character(lines$GameNumber);
    scoreList <- as.character(lines$Score);
    
    return(keyval(gameNumber, scoreList));
  }
  
  # Define the reducer.
  findDeltaAverages_reduce = function(key, values)
  {
    # Key = GameNumber, Values = ScoreList
    gameNumber <- as.character(key);
    scoreList <- as.character(values);
    
    # Split the scores up into a vector.
    scoresVector <- c();
    if (is.na(scoreList) == FALSE)
    {
      scoresVector <- unlist(strsplit(scoreList, " "));
    }
    
    if (length(scoresVector) > 2)
    {
      # Determine the deltas based on the scores.
      deltasVector <- c();
      deltasVector[1] <- as.numeric(scoresVector[1]);
      for(i in 2:length(scoresVector))
      {
        deltasVector[i] <- as.numeric(scoresVector[i]) - as.numeric(scoresVector[i-1]);
      }
      
      # Split the deltas by player.
      whiteDeltas <- deltasVector[seq(1, length(deltasVector), 2)];
      blackDeltas <- deltasVector[seq(2, length(deltasVector), 2)];
      
      # Get the mean and standard deviation for each player.
      meanWhiteDelta <- mean(whiteDeltas);
      meanBlackDelta <- -mean(blackDeltas);
      stdDevWhiteDelta <- sd(whiteDeltas);
      stdDevBlackDelta <- -sd(blackDeltas);
      
      # Get the average of each player's lowest 10 scores.
      lowestWhiteDeltas <- whiteDeltas[which(whiteDeltas %in% head(sort(whiteDeltas), 10))];
      lowestBlackDeltas <- blackDeltas[which(blackDeltas %in% tail(sort(blackDeltas), 10))];
      meanLowestWhiteDeltas <- mean(lowestWhiteDeltas);
      meanLowestBlackDeltas <- -mean(lowestBlackDeltas);
      
      # Encapsulate the values in a data frame.
      retVals <- data.frame(WhiteAverageDeltaScore = meanWhiteDelta, BlackAverageDeltaScore = meanBlackDelta,
                            WhiteStdDevDeltaScore = stdDevWhiteDelta, BlackStdDevDeltaScore = stdDevBlackDelta,
                            WhiteLowestDeltaScore = meanLowestWhiteDeltas, BlackLowestDeltaScore = meanLowestBlackDeltas);
      
      #return(keyval(gameNumber, averageWhiteDelta));
      return(keyval(gameNumber, retVals));
    }
    else
    {
      # Encapsulate the values in a data frame.
      retVals <- data.frame(WhiteAverageDeltaScore = NA, BlackAverageDeltaScore = NA,
                            WhiteStdDevDeltaScore = NA, BlackStdDevDeltaScore = NA,
                            WhiteLowestDeltaScore = NA, BlackLowestDeltaScore = NA);
      
      # TODO: Use NA as default value return.
      return(keyval(gameNumber, retVals));
    }
  }
  
  # Bind the mapper and reducer to a mapreduce call.
  mapreduce(
    input         = input,
    input.format  = scoreData_format,
    map           = findDeltaAverages_map,
    reduce        = findDeltaAverages_reduce
  )
}

# Run the mapreduce to get the data.
results <- findDeltaAverages(engine_score);
results_values <- values(from.dfs(results));

# TODO: Limit the file read to only the first 24999 entries.
results_values <- results_values[1:24999,];


########################################
### Prep the data for model generation.
########################################

# Convert the values from a matrix into a data frame.
mapreduce.chess.filter.values <- data.frame(mapreduce.chess.filter.values);

# Get the number of rows from the data set.
numRows <- nrow(mapreduce.chess.filter.values);

# Set the column names.
colnames(mapreduce.chess.filter.values) <- c("EventId", "WhiteElo", "BlackElo", "MoveList");

# Extract the number values from the ELO strings.  This requires creating a temporary data frame
# where the columns are integers so that the conversion works ok.
tempArray <- data.frame(WhiteElo = numeric(0), BlackElo = numeric(0));
for (i in 1:numRows)
{
  tempArray[i, "WhiteElo"] <- gsub("[^0-9]", "", sapply(mapreduce.chess.filter.values[i, "WhiteElo"], as.character));
  tempArray[i, "BlackElo"] <- gsub("[^0-9]", "", sapply(mapreduce.chess.filter.values[i, "BlackElo"], as.character));
}
mapreduce.chess.filter.values$WhiteElo = tempArray[,"WhiteElo"];
mapreduce.chess.filter.values$BlackElo = tempArray[,"BlackElo"];
#mapreduce.chess.filter.values <- transform(mapreduce.chess.filter.values, WhiteElo = as.numeric(WhiteElo), BlackElo = as.numeric(BlackElo));

# Add the average delta scores of each game to the data set.
mapreduce.chess.filter.values$WhiteAverageDeltaScore <- results_values$WhiteAverageDeltaScore;
mapreduce.chess.filter.values$BlackAverageDeltaScore <- results_values$BlackAverageDeltaScore;
mapreduce.chess.filter.values$WhiteStdDevDeltaScore <- results_values$WhiteStdDevDeltaScore;
mapreduce.chess.filter.values$BlackStdDevDeltaScore <- results_values$BlackStdDevDeltaScore;
mapreduce.chess.filter.values$WhiteLowestMeanDeltaScore <- results_values$WhiteLowestDeltaScore;
mapreduce.chess.filter.values$BlackLowestMeanDeltaScore <- results_values$BlackLowestDeltaScore;

# Copy all delta scores and ELOs into a new data frame where white and black do not factor in so we can make the model.
allElos <- as.numeric(c(mapreduce.chess.filter.values[,"WhiteElo"], mapreduce.chess.filter.values[,"BlackElo"]));
allAverageDeltaScores <- as.numeric(c(mapreduce.chess.filter.values[,"WhiteAverageDeltaScore"], mapreduce.chess.filter.values[,"BlackAverageDeltaScore"]));
allStdDevDeltaScores <- as.numeric(c(mapreduce.chess.filter.values[,"WhiteStdDevDeltaScore"], mapreduce.chess.filter.values[,"BlackStdDevDeltaScore"]));
allLowestMeanDeltaScores <- as.numeric(c(mapreduce.chess.filter.values[,"WhiteLowestMeanDeltaScore"], mapreduce.chess.filter.values[,"BlackLowestMeanDeltaScore"]));
scoresToElos <- data.frame(AverageDeltaScore = allAverageDeltaScores, StdDevDeltaScore = allStdDevDeltaScores, LowestMeanDeltaScore = allLowestMeanDeltaScores, Elo = allElos);

# Remove any rows containing NA.
scoresToElos <- na.omit(scoresToElos);


########################################
### Generate the models for the data.
########################################

# Create linear regression models for mean delta score to Elo, std dev delta score to Elo.
meanModel <- lm(Elo ~ AverageDeltaScore, data = scoresToElos);
stdDevModel <- lm(Elo ~ StdDevDeltaScore, data = scoresToElos);
lowestMeanModel <- lm(Elo ~ LowestMeanDeltaScore, data = scoresToElos);

# Get the linear models in the form Y = aX + b.
meanModelCoefficients = coefficients(meanModel);
stdDevModelCoefficients = coefficients(stdDevModel);
lowestMeanModelCoefficients = coefficients(lowestMeanModel);
print(meanModelCoefficients);
print(stdDevModelCoefficients);
print(lowestMeanModelCoefficients);
#duration = coeffs[1] + coeffs[2]*waiting;


########################################
### Evaluate the accuracy of the models.
########################################

# Create a prediction with a 95% confidence interval.
meanPredictedData_CI95 <- predict(meanModel, newdata = scoresToElos, interval = "confidence", level = 0.95);
stdDevPredictedData_CI95 <- predict(stdDevModel, newdata = scoresToElos, interval = "confidence", level = 0.95);
lowestMeanDevPredictedData_CI95 <- predict(lowestMeanModel, newdata = scoresToElos, interval = "confidence", level = 0.95);

# Plot the 95% CI models.
### Plotter used from DataModelingApproaches.R ###
OrdIn <- order(scoresToElos$AverageDeltaScore);
par(mfrow = c(1,1));
plot(scoresToElos$AverageDeltaScore, scoresToElos$Elo, pch = 19, col = "blue", xlab = "Mean Delta Score", ylab = "Elo");
matlines(scoresToElos$AverageDeltaScore[OrdIn], meanPredictedData_CI95[OrdIn,], type = "l",col = c(1,2,2), lty = c(1,1,1), lwd=3);
legend("topleft", c("95% CI","FittedLine","ActualData"), pch=15, col = c("red","black","blue") );

OrdIn <- order(scoresToElos$StdDevDeltaScore);
par(mfrow = c(1,1));
plot(scoresToElos$StdDevDeltaScore, scoresToElos$Elo, pch = 19, col = "blue", xlab = "Std Dev Delta Score", ylab = "Elo");
matlines(scoresToElos$StdDevDeltaScore[OrdIn], stdDevPredictedData_CI95[OrdIn,], type = "l",col = c(1,2,2), lty = c(1,1,1), lwd=3);
legend("topleft", c("95% CI","FittedLine","ActualData"), pch=15, col = c("red","black","blue") );

OrdIn <- order(scoresToElos$LowestMeanDeltaScore);
par(mfrow = c(1,1));
plot(scoresToElos$LowestMeanDeltaScore, scoresToElos$Elo, pch = 19, col = "blue", xlab = "Lowest Mean Delta Score", ylab = "Elo");
matlines(scoresToElos$LowestMeanDeltaScore[OrdIn], lowestMeanDevPredictedData_CI95[OrdIn,], type = "l",col = c(1,2,2), lty = c(1,1,1), lwd=3);
legend("topleft", c("95% CI","FittedLine","ActualData"), pch=15, col = c("red","black","blue") );

# # Create a prediction with a 95% prediction interval.
# predictedData_Pred95 <- predict(model, newdata = productionData, interval = "prediction", level = 0.95);
# 
# # Plot the 95% prediction model.
# ### Plotter used from DataModelingApproaches.R ###
# OrdIn <- order(scoresToElos$AverageDeltaScore);
# par(mfrow = c(1,1));
# plot(scoresToElos$AverageDeltaScore, scoresToElos$Elo, pch = 19, col = "blue", xlab = "Avg Delta Score", ylab = "Elo");
# matlines(scoresToElos$AverageDeltaScore[OrdIn], predictedData_Pred95[OrdIn,], type = "l",col = c(1,2,2), lty = c(1,1,1), lwd=3);
# legend("topleft", c("95% Pred","FittedLine","ActualData"), pch=15, col = c("red","black","blue") );
# 
# #anova(model);
# 
# # Combine the fitted values and residuals into a data frame.
# modelFittedValues <- fitted.values(model);
# modelResiduals <- residuals(model);
# modelValuesAndResiduals <- data.frame(modelFittedValues, modelResiduals);
# colnames(modelValuesAndResiduals) <- c("Fitted Value", "Residual");
# print(modelValuesAndResiduals);
# 
# # Plot the distribution of the residual error.
# source("plotForecastErrors.R");
# plotForecastErrors(predictedData_CI95, "95% CI");
# plotForecastErrors(predictedData_Pred95, "95% Pred");

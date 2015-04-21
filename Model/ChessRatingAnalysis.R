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

# Define the location of the input files in the HDFS.
gamedata_uci <- "/user/chess/traindata_uci.pgn";
engine_score <- "/user/chess/stockfish.csv";


### Read in the game metadata.

# Define the game file format.
gamedata_uci.classes <- c(
  GameNumber = "character", Source = "factor", Date = "factor", Round = "factor", WhiteName = "factor", BlackName = "factor",
  Result = "factor", WhiteElo = "character", BlackElo = "character", Moves = "character"
);
gamedata_uci.format <- make.input.format(
  "csv",
  sep = ",",
  colClasses = gamedata_uci.classes,
  col.names = names(gamedata_uci.classes)
);

# Read the game metadata.
mapreduce.chess <- mapreduce(
  input = gamedata_uci,
  input.format = gamedata_uci.format
);
mapreduce.chess.values <- values(from.dfs(mapreduce.chess));
View(mapreduce.chess.values);

# Filter out the extraneous characters (result, square brackets, quotes, etc.).
map.chess.filter <- function(.,lines) {
  value <- cbind(lines$GameNumber, lines$WhiteElo, lines$BlackElo, lines$Moves);
  keyval(lines$GameNumber, value);
};
reduce.chess.filter <- function(key,values)
{
  keyval(key, values);
}
mapreduce.chess.filter <- mapreduce(
  input = gamedata_uci,
  input.format = gamedata_uci.format,
  map = map.chess.filter
)
mapreduce.chess.filter.values <- values(from.dfs(mapreduce.chess.filter));
View(mapreduce.chess.filter.values);


### Read in the scores for the games.

# Define the score file format.
scoreData_classes <- c(GameNumber = "character", Score = "character");
scoreData_format <- make.input.format(
  "csv",
  sep = ",",
  colClasses = scoreData_classes,
  col.names = names(scoreData_classes)
);

# Define the delta average mapreduce function.
findDeltaAverages = function(input)
{
  # Define the mapper.
  findDeltaAverages_map = function(., lines)
  {
    return(keyval(as.character(lines$GameNumber), as.character(lines$Score)));
  }
  
  # Define the reducer.
  findDeltaAverages_reduce = function(key, values)
  {
    # Split the scores up into a vector.
    scoresVector <- c();
    scoreList <- as.character(values);
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
      
      # Negate the black deltas since they go in the opposite direction.
      blackDeltas <- -blackDeltas;
      
      # Get the mean, standard deviation, and mean of the lowest 10 scores for each player.
      meanWhiteDelta <- mean(whiteDeltas);
      meanBlackDelta <- mean(blackDeltas);
      stdDevWhiteDelta <- sd(whiteDeltas);
      stdDevBlackDelta <- sd(blackDeltas);
      meanLowestWhiteDeltas <- mean(whiteDeltas[which(whiteDeltas %in% head(sort(whiteDeltas), 10))]);
      meanLowestBlackDeltas <- mean(blackDeltas[which(blackDeltas %in% head(sort(blackDeltas), 10))]);
      
      # Return all the score data for this game.
      return(keyval(as.character(key), data.frame(WhiteAverageDeltaScore = meanWhiteDelta, BlackAverageDeltaScore = meanBlackDelta,
                                           WhiteStdDevDeltaScore = stdDevWhiteDelta, BlackStdDevDeltaScore = stdDevBlackDelta,
                                           WhiteLowestDeltaScore = meanLowestWhiteDeltas, BlackLowestDeltaScore = meanLowestBlackDeltas)));
    }
    else
    {
      # Return NA for all values since this line is not a valid game.
      return(keyval(as.character(key), data.frame(WhiteAverageDeltaScore = NA, BlackAverageDeltaScore = NA,
                                           WhiteStdDevDeltaScore = NA, BlackStdDevDeltaScore = NA,
                                           WhiteLowestDeltaScore = NA, BlackLowestDeltaScore = NA)));
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

# Limit the score data to only the first 24999 entries to match the training data.
results_values <- results_values[1:24999,];


### Prep the data for model generation.

# Convert the values from a matrix into a data frame.
mapreduce.chess.filter.values <- data.frame(mapreduce.chess.filter.values);
colnames(mapreduce.chess.filter.values) <- c("EventId", "WhiteElo", "BlackElo", "MoveList");

# Extract the number values from the ELO strings.  This requires creating a temporary data frame
# where the columns are integers so that the conversion works ok.
tempArray <- data.frame(WhiteElo = numeric(0), BlackElo = numeric(0));
for (i in 1:nrow(mapreduce.chess.filter.values))
{
  tempArray[i, "WhiteElo"] <- gsub("[^0-9]", "", sapply(mapreduce.chess.filter.values[i, "WhiteElo"], as.character));
  tempArray[i, "BlackElo"] <- gsub("[^0-9]", "", sapply(mapreduce.chess.filter.values[i, "BlackElo"], as.character));
}
mapreduce.chess.filter.values$WhiteElo = tempArray[,"WhiteElo"];
mapreduce.chess.filter.values$BlackElo = tempArray[,"BlackElo"];

# Add the average delta scores of each game to the data set.
mapreduce.chess.filter.values$WhiteAverageDeltaScore <- results_values$WhiteAverageDeltaScore;
mapreduce.chess.filter.values$BlackAverageDeltaScore <- results_values$BlackAverageDeltaScore;
mapreduce.chess.filter.values$WhiteStdDevDeltaScore <- results_values$WhiteStdDevDeltaScore;
mapreduce.chess.filter.values$BlackStdDevDeltaScore <- results_values$BlackStdDevDeltaScore;
mapreduce.chess.filter.values$WhiteLowestMeanDeltaScore <- results_values$WhiteLowestDeltaScore;
mapreduce.chess.filter.values$BlackLowestMeanDeltaScore <- results_values$BlackLowestDeltaScore;

# Copy all delta scores and ELOs into a new data frame where white and black do not factor in so we can make the model.
scoresToElos <- data.frame(
  AverageDeltaScore = as.numeric(c(mapreduce.chess.filter.values[,"WhiteAverageDeltaScore"], mapreduce.chess.filter.values[,"BlackAverageDeltaScore"])),
  StdDevDeltaScore = as.numeric(c(mapreduce.chess.filter.values[,"WhiteStdDevDeltaScore"], mapreduce.chess.filter.values[,"BlackStdDevDeltaScore"])),
  LowestMeanDeltaScore = as.numeric(c(mapreduce.chess.filter.values[,"WhiteLowestMeanDeltaScore"], mapreduce.chess.filter.values[,"BlackLowestMeanDeltaScore"])),
  Elo = as.numeric(c(mapreduce.chess.filter.values[,"WhiteElo"], mapreduce.chess.filter.values[,"BlackElo"]))
);

# Remove any rows containing NA.
scoresToElos <- na.omit(scoresToElos);


### Generate the models for the data.

# Summarize the data.
summary(scoresToElos);
cor(scoresToElos);

# Plot the variables against each other.
#pairs(scoresToElos);

# Create linear regression models for mean delta score to Elo, std dev delta score to Elo.
meanModel <- lm(Elo ~ AverageDeltaScore, data = scoresToElos);
stdDevModel <- lm(Elo ~ StdDevDeltaScore, data = scoresToElos);
lowestMeanModel <- lm(Elo ~ LowestMeanDeltaScore, data = scoresToElos);
multiModel <- lm(Elo ~ AverageDeltaScore * StdDevDeltaScore * LowestMeanDeltaScore, data = scoresToElos);

# Get the linear models in the form Y = aX + b.
print(coefficients(meanModel));
print(coefficients(stdDevModel));
print(coefficients(lowestMeanModel));
print(coefficients(multiModel));

# Get the summaries of the models.
summary(meanModel);
summary(stdDevModel);
summary(lowestMeanModel);
summary(multiModel);
anova(meanModel);
anova(stdDevModel);
anova(lowestMeanModel);
anova(multiModel);

# Compare the fitted values and residuals for each model.
summary(data.frame(FittedValue = fitted.values(meanModel), Residual = residuals(meanModel)));
summary(data.frame(FittedValue = fitted.values(stdDevModel), Residual = residuals(stdDevModel)));
summary(data.frame(FittedValue = fitted.values(lowestMeanModel), Residual = residuals(lowestMeanModel)));
summary(data.frame(FittedValue = fitted.values(multiModel), Residual = residuals(multiModel)));


### Evaluate the accuracy of the models.

# Create a prediction with a 95% confidence interval.
meanPredictedData_CI95 <- predict(meanModel, newdata = scoresToElos, interval = "confidence", level = 0.95);
stdDevPredictedData_CI95 <- predict(stdDevModel, newdata = scoresToElos, interval = "confidence", level = 0.95);
lowestMeanPredictedData_CI95 <- predict(lowestMeanModel, newdata = scoresToElos, interval = "confidence", level = 0.95);
multiPredictedData_CI95 <- predict(multiModel, newdata = scoresToElos, interval = "confidence", level = 0.95);


# Plot the 95% CI models.
# NOTE: Plotter used from DataModelingApproaches.R
# Average Delta Score Model
OrdIn <- order(scoresToElos$AverageDeltaScore);
par(mfrow = c(1,1));
plot(scoresToElos$AverageDeltaScore, scoresToElos$Elo, pch = 19, col = "blue", xlab = "Mean Delta Score", ylab = "Elo");
matlines(scoresToElos$AverageDeltaScore[OrdIn], meanPredictedData_CI95[OrdIn,], type = "l",col = c(1,2,2), lty = c(1,1,1), lwd=3);
legend("topleft", c("95% CI","FittedLine","ActualData"), pch=15, col = c("red","black","blue") );

# Standard Deviation Delta Score Model
OrdIn <- order(scoresToElos$StdDevDeltaScore);
par(mfrow = c(1,1));
plot(scoresToElos$StdDevDeltaScore, scoresToElos$Elo, pch = 19, col = "blue", xlab = "Std Dev Delta Score", ylab = "Elo");
matlines(scoresToElos$StdDevDeltaScore[OrdIn], stdDevPredictedData_CI95[OrdIn,], type = "l",col = c(1,2,2), lty = c(1,1,1), lwd=3);
legend("topleft", c("95% CI","FittedLine","ActualData"), pch=15, col = c("red","black","blue") );

# Lowest Mean Delta Score Model
OrdIn <- order(scoresToElos$LowestMeanDeltaScore);
par(mfrow = c(1,1));
plot(scoresToElos$LowestMeanDeltaScore, scoresToElos$Elo, pch = 19, col = "blue", xlab = "Lowest Mean Delta Score", ylab = "Elo");
matlines(scoresToElos$LowestMeanDeltaScore[OrdIn], lowestMeanPredictedData_CI95[OrdIn,], type = "l",col = c(1,2,2), lty = c(1,1,1), lwd=3);
legend("topleft", c("95% CI","FittedLine","ActualData"), pch=15, col = c("red","black","blue") );

# Multi-Model
OrdIn <- order(scoresToElos$AverageDeltaScore);
par(mfrow = c(1,1));
plot(scoresToElos$AverageDeltaScore * scoresToElos$StdDevDeltaScore * scoresToElos$LowestMeanDeltaScore, scoresToElos$Elo, pch = 19, col = "blue", xlab = "Multi Score", ylab = "Elo");
matlines(scoresToElos$AverageDeltaScore[OrdIn], multiPredictedData_CI95[OrdIn,], type = "l",col = c(1,2,2), lty = c(1,1,1), lwd=3);
legend("topleft", c("95% CI","FittedLine","ActualData"), pch=15, col = c("red","black","blue") );

# Plot the distribution of the residual error.
source("plotForecastErrors.R");
plotForecastErrors(meanPredictedData_CI95, "95% CI - Mean");
plotForecastErrors(stdDevPredictedData_CI95, "95% CI - Std Dev");
plotForecastErrors(lowestMeanPredictedData_CI95, "95% CI - Lowest Mean");

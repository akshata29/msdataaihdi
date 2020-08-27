### 0.3.1 Set compute context to MapReduce and set input/output to HDFS
## This diverts outputs and messages to a sink file on the R-server node (where R-studio server is installed)
system("rm sinkFile.txt")
sinkFile <- file("sinkFile.txt", open = "wt")
sink(sinkFile)

bigDataDirRoot <- "" ;
myNameNode <- "default";
myPort <- 0;
myHadoopCluster <- RxHadoopMR(
  hdfsShareDir = bigDataDirRoot,
  nameNode = myNameNode,
  port= myPort,
  hadoopSwitches = '-Dmapred.task.timeout=86400000 -Dmapreduce.input.fileinputformat.split.minsize=110000000 -libjars /etc/hadoop/conf',
  consoleOutput    = TRUE);

rxSetComputeContext(myHadoopCluster);
hdfsFS <- RxHdfsFileSystem(hostName=myNameNode, port=myPort)

### 0.3.2 Preparation of xdf files in HDFS
# Specify path to input file in HDFS
inputFile <-file.path(bigDataDirRoot,"nyctaxiraw/rdata/rmlData.csv");
xdfOutFile <- file.path(bigDataDirRoot,"nyctaxiraw/rdata/taxiDSXdf");

# Define coumn classes
taxiColClasses <- c(medallion = "character", hack_license = "character",
                    vendor_id =  "factor", rate_code = "factor",
                    store_and_fwd_flag = "character", pickup_datetime = "character",
                    dropoff_datetime = "character", pickup_hour = "numeric",
                    pickup_week = "numeric", weekday = "numeric",
                    passenger_count = "numeric", trip_time_in_secs = "numeric",
                    trip_distance = "numeric", pickup_longitude = "numeric",
                    pickup_latitude = "numeric", dropoff_longitude = "numeric",
                    dropoff_latitude = "numeric", direct_distance = "numeric",
                    payment_type = "factor", fare_amount = "numeric",
                    surcharge = "numeric", mta_tax = "numeric", tip_amount = "numeric",
                    tolls_amount = "numeric", total_amount = "numeric",
                    tipped = "factor", tip_class = "factor");

# Create xdf file
taxiDS <- RxTextData(file = inputFile, colClasses  = taxiColClasses,
                     fileSystem = hdfsFS, delimiter = ",", firstRowIsColNames = TRUE);
xdfOut <- RxXdfData(file = xdfOutFile, fileSystem = hdfsFS);
capture.output(taxiDSXdf <- rxImport(inData = taxiDS, outFile = xdfOut,
                                     fileSystem = hdfsFS, createCompositeSet = TRUE,
                                     overwrite = TRUE),
               file=sinkFile);

#0.3.3 Split data into train and test sets
# Assign each observation randomly to training or testing (75% training and 25% testing)
# We also delete some variables not used for modeling, and filter observations which are likely to be invalid or outliers.
taxiSplitXdfFile <- file.path(bigDataDirRoot,"nyctaxiraw/rdata/taxiSplitXdf");
taxiSplitXdf <- RxXdfData(file = taxiSplitXdfFile, fileSystem = hdfsFS);
capture.output(
  rxDataStep(inData = taxiDSXdf, outFile = taxiSplitXdf,
             varsToDrop = c("medallion", "hack_license","store_and_fwd_flag",
                            "pickup_datetime", "rate_code",
                            "dropoff_datetime","pickup_longitude",
                            "pickup_latitude", "dropoff_longitude",
                            "dropoff_latitude ", "direct_distance", "surcharge",
                            "mta_tax", "tolls_amount", "tip_class", "total_amount"),
             rowSelection = (passenger_count > 0 & passenger_count < 8 &
                               tip_amount >= 0 & tip_amount <= 40 &
                               fare_amount > 0 & fare_amount <= 200 &
                               trip_distance > 0 & trip_distance <= 100 &
                               trip_time_in_secs > 10 & trip_time_in_secs <= 7200),
             transforms = list( testSplitVar = ( runif( .rxNumRows ) > 0.25 ) ),
             # 25% test, %75 into training
             overwrite = TRUE),
  file=sinkFile
);

# Create training data xdf
taxiTrainXdfFile <- file.path(bigDataDirRoot,"nyctaxiraw/rdata/taxiTrainXdf");
trainDS <- RxXdfData(file = taxiTrainXdfFile,  fileSystem = hdfsFS);
capture.output(
  rxDataStep( inData = taxiSplitXdf, outFile = trainDS,
              varsToDrop = c( "testSplitVar"),
              rowSelection = ( testSplitVar == 1),
              overwrite = TRUE), file=sinkFile
);

# Create testing data xdf
taxiTestXdfFile <- file.path(bigDataDirRoot,"nyctaxiraw/rdata/taxiTestXdf");
testDS <- RxXdfData(file= taxiTestXdfFile,  fileSystem = hdfsFS);
capture.output(
  rxDataStep( inData = taxiSplitXdf, outFile = testDS,
              varsToDrop = c( "testSplitVar"),
              rowSelection = ( testSplitVar == 0),
              overwrite = TRUE), file=sinkFile
)

#0.3.4 Get summary of variable information from training file
capture.output(fileInfo <- rxGetInfo (trainDS, getVarInfo = TRUE,
                                      computeInfo=TRUE, getBlockSizes = TRUE),
               file = sinkFile
);
fileInfo

#Data exploration through plotting: Generate a histogram of tip amount grouped by passenger counts
capture.output(
  histPlot <- rxHistogram(~tip_amount | passenger_count, numBreaks=20, data = trainDS,
                          title = "Histogram of Tip Amount"),
  file = sinkFile
)

#Regression: Perform linear regression and compute correlation between predicted and actual tip amounts
## Model building
pt1 <- proc.time();
capture.output (
  model.rxLinMod <- rxLinMod(tip_amount ~ fare_amount + vendor_id +
                               pickup_hour + pickup_week + weekday +
                               passenger_count  + trip_time_in_secs +
                               trip_distance + payment_type, data = trainDS),
  file=sinkFile
)

## Get model summary
summary(model.rxLinMod);

## Get elapsed run time on training data: Elapsed time is reported in seconds
pt2 <- proc.time();
runtime_rxLinMod <- pt2-pt1; runtime_rxLinMod;

# Predict on test data-set, get AUC, accuracy etc.
outputLinMod  <- RxXdfData(file.path(bigDataDirRoot, "Results/PredictedLinMod"), fileSystem=RxFileSystem(fileSystem = "hdfs"));

capture.output (
  taxiDxPredictLinMod <- rxPredict(modelObject = model.rxLinMod, checkFactorLevels = TRUE,
                                   data = testDS, outData = outputLinMod,
                                   type = "response",
                                   extraVarsToWrite = as.vector(c("tip_amount")),
                                   predVarNames = "predicted_tipped_amount",
                                   overwrite = TRUE),
  file = sinkFile
)

capture.output (linModDF <- rxImport(inData = outputLinMod, outFile = NULL), file = sinkFile);

rxSetComputeContext("local");
## Sample a subset of rows for plotting, otherwise plot looks busy
overallCorr <- round(cor.test(linModDF$tip_amount, linModDF$predicted_tipped_amount)$estimate, 3);
linModDFSampled <- linModDF[sample(dim(linModDF)[1], 10000),];
linePlot <- rxLinePlot(predicted_tipped_amount ~ tip_amount,
                       data = linModDFSampled, type = 'p',
                       title = 'Actual vs. Predicted Tip Amount',
                       xTitle = 'Actual Tip Amount',
                       yTitle = 'Predicted Tip Amount',
                       subtitle = paste0('Corr: ', overallCorr)
)

#0.3.7 Binary Classification: Create Boosted decision tree model and compute AUC on test data
## Model building
pt1 <- proc.time();
rxSetComputeContext(myHadoopCluster);

capture.output (
  model.gbm <- rxBTrees(tipped ~ fare_amount + vendor_id +
                          pickup_hour + pickup_week + weekday +
                          passenger_count + trip_time_in_secs +
                          trip_distance + payment_type,
                        nTree = 5, maxDepth = 3, mTry = 4,
                        minSplit = 10000, minBucket = 3300,
                        learningRate = 0.1,
                        seed  = 1, data = trainDS),
  file = sinkFile
)

## Get elapsed run time on training data: Elapsed time is reported in seconds
pt2 <- proc.time();
runtime_gbm <- pt2-pt1; runtime_gbm;

# Predict on test data-set, get AUC, accuracy etc.
outputGBM  <- RxXdfData(file.path(bigDataDirRoot, "Results/PredictedGBM"), fileSystem=RxFileSystem(fileSystem = "hdfs"));

capture.output (
  taxiDxPredictGBM <- rxPredict(modelObject = model.gbm, data = testDS,
                                outData = outputGBM , type = "prob",
                                extraVarsToWrite = as.vector(c("tipped")),
                                predVarNames = "predicted_tipped_prob",
                                overwrite = TRUE),
  file = sinkFile
);

capture.output (gbmDF <- rxImport(inData = taxiDxPredictGBM, outFile = NULL), file=sinkFile);
gbmDF$tipped <- as.numeric(gbmDF$tipped);
gbmDF$tipped <- ifelse(gbmDF$tipped == 1, 0, 1);

rxSetComputeContext("local");
rocData <- rxRocCurve(actualVarName = "tipped", predVarNames = "predicted_tipped_prob", data = gbmDF)

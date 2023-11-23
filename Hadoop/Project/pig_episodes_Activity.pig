inputDialogues4 = LOAD 'hdfs:///user/monica/inputs/episodeIV_dialogues.txt' USING PigStorage('\t') AS (name:chararray, line:chararray);
inputDialogues5 = LOAD 'hdfs:///user/monica/inputs/episodeV_dialogues.txt' USING PigStorage('\t') AS (name:chararray, line:chararray);
inputDialogues6 = LOAD 'hdfs:///user/monica/inputs/episodeVI_dialogues.txt' USING PigStorage('\t') AS (name:chararray, line:chararray);

ranked4 = RANK inputDialogues4;
OnlyDialogues4 = FILTER ranked4 BY (rank_inputDialogues4 > 2);
ranked5 = RANK inputDialogues5;
OnlyDialogues5 = FILTER ranked5 BY (rank_inputDialogues5 > 2);
ranked6 = RANK inputDialogues6;
OnlyDialogues6 = FILTER ranked6 BY (rank_inputDialogues6 > 2);

inputData = UNION OnlyDialogues4, OnlyDialogues5, OnlyDialogues6;

groupByName = GROUP inputData BY name;

names = FOREACH groupByName GENERATE $0 as name, COUNT($1) as no_of_lines;
namesOrdered = ORDER names BY no_of_lines DESC;

rmf hdfs:///user/monica/outputs;

STORE namesOrdered INTO 'hdfs:///user/monica/outputs' USING PigStorage('\t');
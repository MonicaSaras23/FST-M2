inputFile = LOAD 'hdfs:///user/monica/input.txt' AS (line:chararray);
words =  FOREACH inputFile GENERATE FLATTEN(TOKENIZE(line)) AS word;
grpd = GROUP words BY word;
totalCount = FOREACH grpd GENERATE group, COUNT(words);
STORE totalCount INTO 'hdfs:///user/monica/results';
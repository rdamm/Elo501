#!/bin/bash

## Split into training data and test data
sed -n "1, 299988p" KaggleData/data_uci.pgn > tempdata1.pgn
tail -n+299989 KaggleData/data_uci.pgn > tempdata2.pgn

## Split.END

# Remove empty lines
sed '/^$/d' tempdata1.pgn > temp1_data.pgn	
sed '/^$/d' tempdata2.pgn > temp2_data.pgn	

# Replace newlines with commas and write it to clean_data_uci.pgn
tr '\n' , < temp1_data.pgn > temp1.pgn
tr '\n' , < temp2_data.pgn > temp2.pgn

sed 's/\[Event/\n\[Event/g' temp1.pgn > temp3.pgn
sed 's/\[Event/\n\[Event/g' temp2.pgn > temp4.pgn

sed '/^$/d' temp3.pgn > FormattedData/traindata_uci1.pgn
sed '/^$/d' temp4.pgn > FormattedData/testdata_uci1.pgn

rm temp*

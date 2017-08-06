#!/bin/sh

#
DATA_DIR=../ti-data-samples/data

if [ ! -d ${DATA_DIR} ]
then
  echo
  echo "Error: the sample data directory is not in the expected location: '${DATA_DIR}'"
  echo "Hint: clone the ti-data-samples project, and re-test:"
  echo "git clone https://github.com/bom4v/ti-data-samples.git && mv ti-data-samples .."
  echo
  exit -1
fi

#
mkdir -p data/cdr

#
cd data/cdr
ln -s ../../${DATA_DIR}/cdr/CDR-sample.csv


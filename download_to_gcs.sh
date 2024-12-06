#!/bin/bash

set -e

LOCAL_PATH=$1
TARGET_GCS_PATH=$2
URL_PREFIX="https://d37ci6vzurychx.cloudfront.net/trip-data"

for MONTH in {1..11}; do
  FMONTH=`printf "%02d" ${MONTH}`

  URL_YELLOW="${URL_PREFIX}/yellow_tripdata_2023-${FMONTH}.parquet"
  URL_GREEN="${URL_PREFIX}/green_tripdata_2023-${FMONTH}.parquet"

  FILE_YELLOW="yellow_tripdata_2023-${FMONTH}.parquet"
  FILE_GREEN="green_tripdata_2023-${FMONTH}.parquet"

  PATH_YELLOW="${LOCAL_PATH}/${FILE_YELLOW}"
  PATH_GREEN="${LOCAL_PATH}/${FILE_GREEN}"

  echo "downloading ${URL_YELLOW} to ${LOCAL_PATH}"
  wget ${URL_YELLOW} -O ${PATH_YELLOW}
  gcloud storage cp ${PATH_YELLOW} ${TARGET_GCS_PATH}/${FILE_YELLOW}
  rm -f ${PATH_YELLOW}

  echo "downloading ${URL_GREEN} to ${LOCAL_PATH}"
  wget ${URL_GREEN} -O ${PATH_GREEN}
  gcloud storage cp ${PATH_GREEN} ${TARGET_GCS_PATH}/${FILE_GREEN}
  rm -f ${PATH_GREEN}

done
version 1.0

workflow LoadBigQueryData {
  input {
    String project_id
    String dataset_name
    String storage_location
    String datatype
    Int max_table_id
    File? service_account_json

    File schema
    String numbered = "true"
    String partitioned = "true"
    String load = "true"
    String uuid = ""
    Array[String] done
    Int? preemptible_tries
    String docker
    String? for_testing_only
  }

  call CreateTables {
  	input:
      project_id = project_id,
      dataset_name = dataset_name,
      storage_location = storage_location,
      datatype = datatype,
      max_table_id = max_table_id,
      schema = schema,
      numbered = numbered,
      partitioned = partitioned,
      load = load,
      uuid = uuid,
      done = done,
      service_account_json = service_account_json,
      preemptible_tries = preemptible_tries,
      docker = docker,
      for_testing_only = for_testing_only
  }

  scatter (table_dir_files_str in CreateTables.table_dir_files_list) {
    call LoadTable {
      input:
        table_dir_files_str = table_dir_files_str,
        project_id = project_id,
        schema = schema,
        load = load,
        service_account_json = service_account_json,
        preemptible_tries = preemptible_tries,
        docker = docker
    }
  }

  output {
    Array[String] table_dir_files_list = CreateTables.table_dir_files_list
  }
}

task LoadTable {
  meta {
    volatile: true
  }

  input {
    String table_dir_files_str
    String project_id
    File schema
    String load
    File? service_account_json

    Int? preemptible_tries
    String docker
  }

    String has_service_account_file = if (defined(service_account_json)) then 'true' else 'false'

  command <<<
  if [ ~{has_service_account_file} = 'true' ]; then
      gcloud auth activate-service-account --key-file='~{service_account_json}'
  fi

    TABLE=$(echo ~{table_dir_files_str} | cut -d, -f1)
    DIR=$(echo ~{table_dir_files_str} | cut -d, -f2)
    FILES=$(echo ~{table_dir_files_str} | cut -d, -f3)

    #load should be false if using Google Storage Transfer so that the tables will be created by this script, but no data will be uploaded.
    if [ ~{load} = true ]; then
      bq load --location=US --project_id=~{project_id} --skip_leading_rows=1 --source_format=CSV -F "\t" $TABLE $DIR$FILES ~{schema}
      echo "ingested ${FILES} file from $DIR into table $TABLE"
      gsutil mv $DIR$FILES ${DIR}done/
    else
      echo "${FILES} will be ingested from $DIR by Google Storage Transfer"
    fi
  >>>

  runtime {
    docker: docker
    memory: "3 GB"
    disks: "local-disk 10 HDD"
    preemptible: select_first([preemptible_tries, 5])
    cpu: 1
  }
}

# Creates all the tables necessary for the LoadData operation
# As an optimization, I also generate a (table, dir, files) csv file which contains
# most of inputs necessary for the following LoadTable task.
task CreateTables {
	meta {
    	volatile: true
  	}

	input {
      String project_id
      String dataset_name
      String storage_location
      String datatype
      Int max_table_id
      File schema
      String numbered
      String partitioned
      String load
      String uuid
      File? service_account_json

      #input from previous task needed to delay task from running until the other is complete
      Array[String] done

      # runtime
      Int? preemptible_tries
      String docker

      String? for_testing_only
    }

    String has_service_account_file = if (defined(service_account_json)) then 'true' else 'false'

  command <<<
    set -x
    set -e
    ~{for_testing_only}

    DIR="~{storage_location}/~{datatype}_tsvs/"

    if [ ~{has_service_account_file} = 'true' ]; then
      gcloud auth activate-service-account --key-file='~{service_account_json}'
    fi

    for TABLE_ID in $(seq 1 ~{max_table_id}); do
      PARTITION_STRING=""
      if [ ~{partitioned} == "true" ]; then
        let "PARTITION_START=(${TABLE_ID}-1)*4000+1"
        let "PARTITION_END=$PARTITION_START+3999"
        let "PARTITION_STEP=1"
        PARTITION_FIELD="sample_id"
        PARTITION_STRING="--range_partitioning=$PARTITION_FIELD,$PARTITION_START,$PARTITION_END,$PARTITION_STEP"
      fi

      printf -v PADDED_TABLE_ID "_%03d" ${TABLE_ID}
      FILES="~{datatype}${PADDED_TABLE_ID}_*"

      NUM_FILES=$(gsutil ls $DIR$FILES | wc -l)

      # create the table
      PREFIX=""
      if [ -n "~{uuid}" ]; then
          PREFIX="~{uuid}_"
      fi

      if [ $NUM_FILES -gt 0 ]; then
        if [ ~{numbered} != "true" ]; then
          PADDED_TABLE_ID=""  #override table id to empty string, but it is needed to get the files
        fi

        TABLE="~{dataset_name}.${PREFIX}~{datatype}${PADDED_TABLE_ID}"

        # Checks that the table has not been created yet
        if [ ! -f table_dir_files.csv ] || [ "$(cat table_dir_files.csv | cut -d, -f1 | grep -c $TABLE)" = "0" ]; then
          echo "making table $TABLE"
          bq --location=US mk ${PARTITION_STRING} --project_id=~{project_id} $TABLE ~{schema}
        fi

        echo "$TABLE,$DIR,$FILES" >> table_dir_files.csv
      else
        echo "no ${FILES} files to process"
      fi

    done
  >>>

  output {
    Array[String] table_dir_files_list = read_lines("table_dir_files.csv")
  }

  runtime {
    docker: docker
    memory: "3 GB"
    disks: "local-disk 10 HDD"
    preemptible: select_first([preemptible_tries, 5])
    cpu: 1
  }
}

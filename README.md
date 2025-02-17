<br />
<div align="center">
  <h3 align="center">README - Transactions</h3>
</div>



<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a></li>
    <li><a href="#code">Code</a></li>
    <li>
      <a href="#getting-started">Getting Started</a>
    </li>
    <li><a href="#improvements">Improvements</a></li>

  </ol>
</details>




## About The Project
This is basically an exercise (please read the exercise.md to get a proper idea of the description).
This project is split in several directories:


- `Configuration` contains the json file needed for the ETL proccess. This way whenever you want to make changes to the parameters you don't need to make changes in the code
- `main.py` is the principal file and contains the main logic steps for executing the exercise proposed
- `etl` contains the logic for Dataframes management, which involves the whole ETL
- `input` has the input files to be used in the ETL
- `output` will be the destination of the ETL output files
- `requirements.txt` contains a list of packages needed to make the code work
- `tests` holds the testing battery to use for data quality

## Installation
In case you want to try this exercise in your local PC, then I would suggerst you to follow the [next steps ](https://medium.com/@dipan.saha/getting-started-with-pyspark-day-1-37e5e6fdc14b). But I would recommend to run the program in docker since no dependencies will be needed and you will avoid innecessary headaches. 


## Code

By running the `main.py` we will go thorugh the following steps:

- Extraction of the claims and contracts files
- Creation of the transactions dataframe from previous claims and contracts dataframes. Since I was not sure about which keys use for the Join, I left out contracts source system:

  `(df_claims.CONTRACT_SOURCE_SYSTEM == df_contracts.SOURCE_SYSTEM)`

- Apply transformations over the transactions dataframe
- Export of the resulting transactions into the output folder in a compressed format such as parquet

I will also mention here the different tests that will be used in this project.

- Transactions: 
  - Check that there are no duplicates for the primary keys
  - Check that there are not null values for not nullable columns
  - Check the schema with the proposed by the Data Architect
- Claims: Check the schema of the input files
- Contracts: Check the schema of the input files


## Improvements

I will mention here some remarks to what I would do in case this would be a real case.

Since, we would be using any of the Clouds, we would need to `Extract` the files from different and more difficult places such as a folder inside of the raw Blob Container of your Azure Storage Account in Production.

The same hapens with the write task, the output location is not very likely to happen in productive use cases. Instead, we should need to write to a Bronze or Silver/Gold (in our case) folder inside of a blob in Azure and then some external tables in Databricks could be pointing to this folders, so Data Engineers, Analysts, or other users would use them in that platform.

I would have create intermediate outputs folder for claims and contracts where this stage would represent the bronze layer (files in parquet format but still without transformations applied)

Finally, in case we would be getting daily transactions I would do to not overwrite the transactions every time the script runs, but be to upsert the data ( Insert in case the data doesn't already exist, or update some fields in case it exists)

## License

Private repository - All rights reserved

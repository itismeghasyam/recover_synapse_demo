######################################
## Code to access parquet datasets for RECOVER project
######################################

########
# Required Libraries
########
library(arrow)
library(synapser)
library(tidyverse)
library(githubr) # install this package from https://github.com/brian-bot/githubr

########
# Login into Synapse
########
synapser::synLogin()
ARCHIVE_VERSION <- '2023-09-21'
# To get a list of possible ARCHIVE_VERSION (dates), look at syn52506069 in Synapse.
# It will have a list of possible dates as subfolders

########
#### Set up access and Get list of valid datasets
#### archived versions of the external parquet dataset (syn52506069)
########

## Set up Token access
sts_token <- synapser::synGetStsStorageToken(entity = 'syn52506069', # sts enabled destination folder
                                             permission = 'read_only',   # request a read only token
                                             output_format = 'json')

s3_external <- arrow::S3FileSystem$create(access_key = sts_token$accessKeyId,
                                          secret_key = sts_token$secretAccessKey,
                                          session_token = sts_token$sessionToken,
                                          region="us-east-1")

## Get list of datasets in the S3 bucket
base_s3_uri_external <- paste0(sts_token$bucket, "/", sts_token$baseKey,'/',ARCHIVE_VERSION)
parquet_datasets_external <- s3_external$GetFileInfo(arrow::FileSelector$create(base_s3_uri_external, recursive=F))

## Get all valid datasets
i <- 0
valid_paths <- character()
for (dataset in parquet_datasets_external) {
  if (grepl('recover-main-project/main/archive/', dataset$path, perl = T, ignore.case = T)) {
    i <- i+1
    cat(i)
    cat(":", dataset$path, "\n")
    valid_paths <- c(valid_paths, dataset$path)
  }
}

## Get dataset type (for eg., dataset_enrolledparticipants)
valid_paths_ext_df <- valid_paths %>% 
  as.data.frame() %>% 
  `colnames<-`('parquet_path_external') %>% 
  dplyr::rowwise() %>% 
  dplyr::mutate(datasetType = str_split(parquet_path_external,'/')[[1]][5]) %>%
  dplyr::ungroup()


########
#### Read a dataset
########
# Let's read the dataset_enrolledparticipants dataset
dataset_path <- valid_paths_ext_df %>% 
  dplyr::filter(datasetType == 'dataset_enrolledparticipants')

enrolled.df <- arrow::open_dataset(s3_external$path(as.character(dataset_path$parquet_path_external))) %>% dplyr::collect()

# Let's read dataset_symptomlog
dataset_path <- valid_paths_ext_df %>% 
  dplyr::filter(datasetType == 'dataset_symptomlog')

symptomlog.df <- arrow::open_dataset(s3_external$path(as.character(dataset_path$parquet_path_external))) %>% dplyr::collect()

# Merge datasets
merged.df <- symptomlog.df %>% 
  dplyr::left_join(enrolled.df %>% 
                     dplyr::select(-InsertedDate, -export_end_date,-cohort))


########
#### Store a preliminary result in Synapse
########

##
# Some analysis
##

out.df <- merged.df %>% 
  dplyr::filter(age > 60, age < 64) %>% 
  dplyr::group_by(age) %>% 
  dplyr::count()

##
## write to a CSV file (or any other file type)
##
write.csv(out.df, 'test_upload.csv')

# Upload data to Synapse
# upload file to Synapse with provenance
# to learn more about provenance in Synapse, go to https://help.synapse.org/docs/Provenance.1972470373.html

##
## Github link
# Copy paste your github token string and store it as 'github_token.txt' file
##

### Commenting out - doesnt work across users without 
gtToken = 'github_token.txt'
githubr::setGithubToken(as.character(read.table(gtToken)$V1))
thisFileName <- "parquet_access_archive.R" # location of file inside github repo
thisRepo <- githubr::getRepo(repository = "itismeghasyam/recover_synapse_demo",
                             ref="branch",
                             refName="main")
thisFile <- githubr::getPermlink(repository = thisRepo, repositoryPath=thisFileName)

##
### name and describe this activity
##
activityName = "Data Access Template"
activityDescription = "Example code to access parquet datasets and upload results to Synapse"

##
## list of all synapse Ids used for this analysis (if you want to link any synapse folders/files in provenance)
##
all.used.ids = c('syn52538628','syn52538608')

##
### upload 
# Please create a specific folder for your team under Analysis Sandbox (syn51712113)
##

synapse.folder.id <- "syn52540337" # synId of folder to upload your file to
# I created a folder under there called synapse_demo to which I will be uploading the results (out.df)

OUTPUT_FILE <- "test_upload.csv" # name your file
synapser::synStore(File(OUTPUT_FILE, parentId=synapse.folder.id),
                   activityName = activityName,
                   activityDescription = activityDescription,
                   used = all.used.ids,
                   executed = thisFile)
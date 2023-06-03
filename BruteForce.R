##############
# Useful Links
# 1. haversine distance: https://www.igismap.com/haversine-formula-calculate-geographic-distance-earth/
# 2. parallel searching: https://unc-libraries-data.github.io/R-Open-Labs/Extras/Parallel/foreach.html
##############


################
# Library Import
library(grid)
library(REdaS)
library(tidyverse)
library(foreach)
library(iterators)
library(parallel)
library(doParallel)
library(rlist)
library(ggplot2)
library(dplyr)
library(hrbrthemes)
library(reshape)
library(vroom)
library(glue)
library(stringr)
library(tictoc)
library(data.table)
################

####################
# Data Import (local)
# table1 <- read.table("/Users/suyeonju101/Desktop/milliquas77RADECName.txt", sep=",", header=TRUE, fill=TRUE)
# table2 <- read.table("/Users/suyeonju101/Desktop/milliquas77cRADECName.txt", sep=",", header=TRUE, fill=TRUE)

# Data Import (clotho)
table1 <- read.table("/sharedata/fastdisk/suyeonju101/milliquas77RADECName.txt", sep=",", header=TRUE, fill=TRUE)
table2 <- read.table("/sharedata/fastdisk/suyeonju101/milliquas77cRADECName.txt", sep=",", header=TRUE, fill=TRUE)

# Data Subset (debugging and testing purpose)
subset_table1_100 <- table1[1:100, ]
subset_table2_100 <- table2[1:100, ]

subset_table1_200 <- table1[1:200, ]
subset_table2_200 <- table2[1:200, ]

subset_table1_500 <- table1[1:500, ]
subset_table2_500 <- table2[1:500, ]

subset_table1_1000 <- table1[1:1000, ]
subset_table2_1000 <- table2[1:1000, ]

subset_table1_10000 <- table1[1:10000, ]
subset_table2_10000 <- table2[1:10000, ]

subset_table1_20000 <- table1[1:20000, ]
subset_table2_20000 <- table2[1:20000, ]

subset_table1_30000 <- table1[1:30000, ]
subset_table2_30000 <- table2[1:30000, ]

subset_table1_50000 <- table1[1:50000, ]
subset_table2_50000 <- table2[1:50000, ]

subset_table1_100000 <- table1[1:100000, ]
subset_table2_100000 <- table2[1:100000, ]

# Data Subset (in case if needed)
# delimiter <- floor(nrow(table2) / 4)
# sub1_table2 <- table2[1:(delimiter),]
# sub2_table2 <- table2[(delimiter+1):(2*delimiter),]
# sub3_table2 <- table2[(2*delimiter+1):(3*delimiter),]
# sub4_table2 <- table2[(3*delimiter+1):(nrow(table2)),]

# subset1_table2 <- subset_table2[1:5,]
# subset2_table2 <- subset_table2[6:10,]
# subset3_table2 <- subset_table2[11:15,]
# subset4_table2 <- subset_table2[16:20,]


##########################################################
## Propositional Crossmatch Methods for Two Large Catalogs
# 0. Tools
haversine.dist <- function(lat_1, lat_2, lon_1, lon_2)
{
  R <- 6371 # km
  lat_diff <- deg2rad(lat_2 - lat_1)
  lon_diff <- deg2rad(lon_2 - lon_1)
  
  a <- as.numeric((sin(lat_diff/2) * sin(lat_diff/2)) + cos(deg2rad(lat_1)) * cos(deg2rad(lat_2)) * (sin(lon_diff/2) * sin(lon_diff/2)))
  c <- 2 * atan2(sqrt(a), sqrt(1-a))
  d <- R * c
  
  return(d)
}

create.dataframe <- function(query_id, lists)
{
  temp_df <- as.data.frame(do.call(rbind, lists))
  df <- temp_df %>% 
    add_column(match_id_table1 = query_id, .before = 1)
  
  return (df)
}

combine.dataframe <- function(data_lists)
{
  return(as.data.frame(do.call(rbind, data_lists)))
}


# [1]. Brute Force
bruteforce.crossmatch.two.catalogs <- function(table_1, table_2, query_id, dist)
{
  lists <- list()
  place_1 <- table_1[query_id, ]
  lon_1 <- place_1[1]
  lat_1 <- place_1[2]
  
  # from different table
  table_2_len <- nrow(table_2)
  for(i in 1:table_2_len)
  {
    place_2 <- table_2[i, ]
    lon_2 <- place_2[1]
    lat_2 <- place_2[2]
    
    distance <- haversine.dist(lat_1, lat_2, lon_1, lon_2)
    
    if(distance < dist)
    {
      lists_len <- length(lists)
      lists[[lists_len+1]] <- table_2[i, ]
    }
  }
  
  result <- create.dataframe(query_id, lists)
  
  return(result)
}


entire.bruteforce.crossmatch.two.catalogs <- function(table_1, table_2, dist)
{
  data_lists <- list()
  
  table_1_len <- nrow(table_1)
  for(i in 1:table_1_len)
  {
    data <- bruteforce.crossmatch.two.catalogs(table_1, table_2, i, dist)
    len <- length(data_lists)
    data_lists[[len+1]] <- data
  }
  
  return(combine.dataframe(data_lists))
}


# [2]. Parallel Computing 1
# Idea: do the same job as the function entire.bruteforce.crossmatch.two.catalogs
parallel.computing.crossmatch.two.catalogs <- function(table_1, table_2, dist, num_cores=NULL)
{
  num_cores <- detectCores()
  registerDoParallel(num_cores)
  
  # still using bruteforce.crossmatch.two.catelogs
  foreach(i = 1:(nrow(table_1)), .combine=rbind) %dopar%
    {
      return(bruteforce.crossmatch.two.catalogs(table_1, table_2, i, dist))
    }
}

# [2]. Parallel Computing 2
# Idea: do the same job as the function entire.bruteforce.crossmatch.two.catalogs
parallel.computing.crossmatch.two.catalogs.idx <- function(table_1, table_2, dist, num_cores=NULL)
{
  num_cores <- detectCores()
  registerDoParallel(num_cores)
  
  delimiter <- floor(nrow(table_1) / num_cores)
  index <- list()
  for(i in 1:num_cores)
  {
    if(i == 1)
    {
      index <- list.append(index, 1)
      index <- list.append(index, delimiter)
    }
    else if (i == num_cores)
    {
      index <- list.append(index, delimiter * (i - 1) + 1)
      index <- list.append(index, nrow(table_1))
    }
    else
    {
      index <- list.append(index, delimiter * (i - 1) + 1)
      index <- list.append(index, delimiter * i)
    }
  }
  
  # still using entire.bruteforce.crossmatch.two.catelogs
  foreach(i = 1:(length(index)/2), .combine=rbind) %dopar%
    {
      first_index <- index[[i*2-1]]
      second_index <- index[[i*2]]
      return(entire.bruteforce.crossmatch.two.catalogs(table_1[first_index:second_index,], table_2, 0.5))
    }
}


# [Compare and Contrast] Non-Parallel vs. Parallel
# TODO: RUN
compare.time <- function(subset_table1, subset_table2)
{
  # with subset
  start <- proc.time()
  realdata_result_brute_force <- entire.bruteforce.crossmatch.two.catalogs(subset_table1, subset_table2, 0.5)
  no_dopar_loop <- proc.time()-start
  
  
  start <- proc.time()
  realdata_result_parellel_not_split <- parallel.computing.crossmatch.two.catalogs(subset_table1, subset_table2, 0.5)
  dopar_loop_no_split <- proc.time()-start
  
  
  start <- proc.time()
  realdata_result_parellel_split <- parallel.computing.crossmatch.two.catalogs.idx(subset_table1, subset_table2, 0.5)
  dopar_loop_split <- proc.time()-start
  
  
  return(rbind(no_dopar_loop, dopar_loop_no_split, dopar_loop_split)[,1])
}


compare.time.subsets.tables <- function(subset_table1_100, subset_table2_100,
                                        subset_table1_200, subset_table2_200,
                                        subset_table1_500, subset_table2_500,
                                        subset_table1_1000, subset_table2_1000,
                                        subset_table1_10000, subset_table2_10000)
{
  rows.100 <- compare.time(subset_table1_100, subset_table2_100)
  rows.200 <- compare.time(subset_table1_200, subset_table2_200)
  rows.500 <- compare.time(subset_table1_500, subset_table2_500)
  rows.1000 <- compare.time(subset_table1_1000, subset_table2_1000)
  rows.10000 <- compare.time(subset_table1_10000, subset_table2_10000)
  
  merged_data <- data.frame(method = c("bruteforce", "parallel", "parallel with splits"), time = cbind(rows.100, 
                                                                                                       rows.200, 
                                                                                                       rows.500,
                                                                                                       rows.1000,
                                                                                                       rows.10000))
  
  vroom_write(merged_data, "timecompared.csv")
  merged_data_long <- melt(merged_data, id.vars = "method")
  
  return(merged_data_long)
}


###################
# TESTING FUNCTIONS
test.dist <- function()
{
  return(haversine.dist(41.507483, 38.504048, -99.436554, -98.315949))
}

compare.time.tictoc.bf <- function()
{
  # BruteForce Timing
  tic('tic100, BruteForce')
  entire.bruteforce.crossmatch.two.catalogs(subset_table1_100, subset_table2_100, 0.5)
  toc(log=TRUE)

  tic('tic200, BruteForce')
  entire.bruteforce.crossmatch.two.catalogs(subset_table1_200, subset_table2_200, 0.5)
  toc(log=TRUE)

  tic('tic500, BruteForce')
  entire.bruteforce.crossmatch.two.catalogs(subset_table1_500, subset_table2_500, 0.5)
  toc(log=TRUE)

  tic('tic1000, BruteForce')
  entire.bruteforce.crossmatch.two.catalogs(subset_table1_1000, subset_table2_1000, 0.5)
  toc(log=TRUE)

  tic('tic10000, BruteForce')
  entire.bruteforce.crossmatch.two.catalogs(subset_table1_10000, subset_table2_10000, 0.5)
  toc(log=TRUE)


  # Parallel
  tic('tic100,Parallel1')
  parallel.computing.crossmatch.two.catalogs(subset_table1_100, subset_table2_100, 0.5)
  toc(log=TRUE)
  tic('tic100,Parallel2')
  parallel.computing.crossmatch.two.catalogs.idx(subset_table1_100, subset_table2_100, 0.5)
  toc(log=TRUE)

  tic('tic200,Parallel1')
  parallel.computing.crossmatch.two.catalogs(subset_table1_200, subset_table2_200, 0.5)
  toc(log=TRUE)
  tic('tic200,Parallel2')
  parallel.computing.crossmatch.two.catalogs.idx(subset_table1_200, subset_table2_200, 0.5)
  toc(log=TRUE)

  tic('tic500,Parallel1')
  parallel.computing.crossmatch.two.catalogs(subset_table1_500, subset_table2_500, 0.5)
  toc(log=TRUE)
  tic('tic500,Parallel2')
  parallel.computing.crossmatch.two.catalogs.idx(subset_table1_500, subset_table2_500, 0.5)
  toc(log=TRUE)

  tic('tic1000,Parallel1')
  parallel.computing.crossmatch.two.catalogs(subset_table1_1000, subset_table2_1000, 0.5)
  toc(log=TRUE)
  tic('tic1000,Parallel2')
  parallel.computing.crossmatch.two.catalogs.idx(subset_table1_1000, subset_table2_1000, 0.5)
  toc(log=TRUE)

  tic('tic10000,Parallel1')
  parallel.computing.crossmatch.two.catalogs(subset_table1_10000, subset_table2_10000, 0.5)
  toc(log=TRUE)
  tic('tic10000,Parallel2')
  parallel.computing.crossmatch.two.catalogs.idx(subset_table1_10000, subset_table2_10000, 0.5)
  toc(log=TRUE)

  tic('tic30000,Parallel1')
  parallel.computing.crossmatch.two.catalogs(subset_table1_30000, subset_table2_30000, 0.5)
  toc(log=TRUE)
  tic('tic30000,Parallel2')
  parallel.computing.crossmatch.two.catalogs.idx(subset_table1_30000, subset_table2_30000, 0.5)
  toc(log=TRUE)
  
  # tic('tic50000,Parallel1')
  # parallel.computing.crossmatch.two.catalogs(subset_table1_50000, subset_table2_50000, 0.5)
  # toc(log=TRUE)
  # tic('tic50000,Parallel2')
  # parallel.computing.crossmatch.two.catalogs.idx(subset_table1_50000, subset_table2_50000, 0.5)
  # toc(log=TRUE)
  # 
  # tic('tic100000,Parallel1')
  # parallel.computing.crossmatch.two.catalogs(subset_table1_100000, subset_table2_100000, 0.5)
  # toc(log=TRUE)
  # tic('tic100000,Parallel2')
  # parallel.computing.crossmatch.two.catalogs.idx(subset_table1_100000, subset_table2_100000, 0.5)
  # toc(log=TRUE)
  
  log.txt <- tic.log(format=TRUE)
  tic.clearlog()
  
  writeLines(unlist(log.txt))

  # 1 line vs. all lines
  # tic100: 0.847 sec elapsed
  # tic200: 2.19 sec elapsed
  # tic500: 4.164 sec elapsed
  # tic1000: 7.839 sec elapsed
  # tic10000: 55.75 sec elapsed
  
  # all lines vs. all lines
  # tic100: 76.32 sec elapsed
  # tic200: 286.849 sec elapsed
  # tic500: 1600.319 sec elapsed
  # tic1000: 6268.242 sec elapsed
  # tic10000: not doable
  
  # tic100,Parallel1: 9.931 sec elapsed
  # tic100,Parallel2: 40.519 sec elapsed
  # tic200,Parallel1: 22.954 sec elapsed
  # tic200,Parallel2: 37.972 sec elapsed
  # tic500,Parallel1: 85.537 sec elapsed
  # tic500,Parallel2: 221.205 sec elapsed
  # tic1000,Parallel1: 210.952 sec elapsed
  # tic1000,Parallel2: 416.302 sec elapsed
  
  # tic10000,Parallel1: 16043.272 sec elapsed
  # tic10000,Parallel2: 16643.011 sec elapsed
  
  # tic20000,Parallel1: 63731.072 sec elapsed
  # tic20000,Parallel2: 69815.195 sec elapsed
  
  
  # tic30000,Parallel1: 168405.406 sec elapsed
  # tic30000,Parallel2: 179454.766 sec elapsed
  
  
  # 1 line vs. all lines
  # tic1000,Parallel1: 9.494 sec elapsed
  # tic1000,Parallel2: 45.231 sec elapsed
  # tic10000,Parallel1: 67.323 sec elapsed
  # tic10000,Parallel2: 116.883 sec elapsed
  # tic10000,Parallel1: 78.507 sec elapsed
  # tic10000,Parallel2: 219.893 sec elapsed
  
  ################# RESULTS
  # tic1: 69.55 sec elapsed  ## built-in functions of writing and reading files
  # tic2: 53.735 sec elapsed ## data.table functions of writing and reading files
  
  ## vroom splitting
  # 1. not controlling num_cores in splitting (num_cores=64)
  # tic3: 80.048 sec elapsed : vroom functions of writing and reading files with using num_cores = null
  # tic4: 40.929 sec elapsed : vroom functions of writing and reading files with using num_cores = 1
  # 2. controlling num_cores=1 in splitting
  # tic5: 35.558 sec elapsed : vroom functions of writing and reading files with using num_cores = 1
  
  ## vroom (Reading) + data.table (Writing) splitting
  # 1. not controlling num_cores in splitting (num_cores=64)
  # tic6: 15.893 sec elapsed : vroom functions of writing and reading files with using num_cores = 1
  # tic7: 56.656 sec elapsed : vroom functions of writing and reading files with using num_cores = null
  # 2. controlling num_cores=1 in splitting
  # tic8: 34.519 sec elapsed : vroom functions of writing and reading files with using num_cores = 1
}


###################
# TEST METHODS
# SAME TABLES? YES
# realdata_result_brute_force <- entire.bruteforce.crossmatch.two.catalogs(subset_table1_200, subset_table2_200, 0.5)
# realdata_result_parellel_not_split <- parallel.computing.crossmatch.two.catalogs(subset_table1_200, subset_table2_200, 0.5)
# realdata_result_parellel_split <- parallel.computing.crossmatch.two.catalogs.idx(subset_table1_200, subset_table2_200, 0.5)
###################
# compare.time.subsets.tables(subset_table1_100, subset_table2_100, subset_table1_200, subset_table2_200, subset_table1_500, subset_table2_500, subset_table1_1000, subset_table2_1000, subset_table1_10000, subset_table2_10000)

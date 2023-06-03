################################################################################################################
# Library
library(glue)
library(stringr)
library(rlist)
library(tictoc)
library(parallel)
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
library(data.table)
################################################################################################################



################################################################################################################
# [Data Import]
table1 <- vroom("/sharedata/fastdisk/suyeonju101/milliquas77RADECName.txt")
table2 <- vroom("/sharedata/fastdisk/suyeonju101/milliquas77cRADECName.txt")

# [Data Transform]
fwrite(table1, "/sharedata/fastdisk/suyeonju101/milliquas77RADECName.csv", row.names=FALSE)
fwrite(table2, "/sharedata/fastdisk/suyeonju101/milliquas77cRADECName.csv", row.names=FALSE)

# Data Paths
table1_path = "/sharedata/fastdisk/suyeonju101/milliquas77RADECName.csv"
table2_path = "/sharedata/fastdisk/suyeonju101/milliquas77cRADECName.csv"
################################################################################################################



################################################################################################################
# Column Names
ra_1 <- colnames(table1)[1]
dec_1 <- colnames(table1)[2]
ra_2 <- colnames(table2)[1]
dec_2 <- colnames(table2)[2]
################################################################################################################



################################################################################################################
## 1. built-in library
#     read.table(), write.csv()
stilts.invoke.command <- function(table_1, table_2, ra_1, ra_2, dec_1, dec_2, dist, i=NULL)
{
  open_stilts_string <- "java -jar stilts.jar "
  
  if (is.null(i))
  {
    tskymatch2_string <- glue("tskymatch2 in1={table_1} in2={table_2} out={dirname(table_1)}/crossmatched_points.csv ra1={ra_1} dec1={dec_1} ra2={ra_2} dec2={dec_2} error={dist}")
  }
  else
  {
    tskymatch2_string <- glue("tskymatch2 in1={table_1} in2={table_2} out={dirname(table_1)}/crossmatched_points{i}.csv ra1={ra_1} dec1={dec_1} ra2={ra_2} dec2={dec_2} error={dist}")
  }
  
  command_string <- open_stilts_string + tskymatch2_string
  print(command_string)
  
  # Except for intern, remain other positional parameters default
  system(command_string, intern = FALSE,
         ignore.stdout = FALSE, ignore.stderr = FALSE,
         wait = TRUE, input = NULL, show.output.on.console = TRUE,
         minimized = FALSE, invisible = TRUE, timeout = 0)
  
  read_table <- read.table(glue("{dirname(table_1)}/crossmatched_points{i}.csv"), sep=",", header=TRUE, fill=TRUE)
}


split.table.1 <- function(table1_path, num_cores=NULL)
{
  # Read table
  table_1 <- read.table(table1_path, sep=",", header=TRUE, fill=TRUE)
  
  # Detect the number of cores
  if (is.null(num_cores))
  {
    num_cores <- detectCores()
  }
  
  # Split the table
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
  
  # Write the table in the disk
  table_1_list <- list()
  
  for(i in 1:(length(index)/2))
  {
    first_index <- index[[i*2-1]]
    second_index <- index[[i*2]]
    
    new_table_1 <- table_1[first_index:second_index,]
    new_path <- paste(dirname(table1_path), "/datasplited/", str_split(basename(table1_path), ".csv", simplify=TRUE)[1], as.character(i), '.csv', sep="")
    #print(new_path)
    write.csv(new_table_1, new_path, row.names=FALSE)
    table_1_list <- list.append(table_1_list, new_path)
  }
  
  return(table_1_list)
}


stilts.invoke.command.parallel <- function(table_1_list, table_2, ra_1, ra_2, dec_1, dec_2, dist, num_cores=NULL)
{
  if (is.null(num_cores))
  {
    num_cores <- detectCores()
  }
  registerDoParallel(num_cores)
  
  
  # still using entire.bruteforce.crossmatch.two.catelogs
  foreach(i = 1:num_cores, .combine=rbind) %dopar%
    {
      return(stilts.invoke.command(table_1_list[[i]], table_2, ra_1, ra_2, dec_1, dec_2, dist, i))
    }
}
################################################################################################################



################################################################################################################
## 2. data.table library
#     fread(), fwrite()
library(data.table)

stilts.invoke.command.fread.fwrite <- function(table_1, table_2, ra_1, ra_2, dec_1, dec_2, dist, i=NULL)
{
  open_stilts_string <- "java -jar stilts.jar "
  
  if (is.null(i))
  {
    tskymatch2_string <- glue("tskymatch2 in1={table_1} in2={table_2} out={dirname(table_1)}/crossmatched_points.csv ra1={ra_1} dec1={dec_1} ra2={ra_2} dec2={dec_2} error={dist}")
  }
  else
  {
    tskymatch2_string <- glue("tskymatch2 in1={table_1} in2={table_2} out={dirname(table_1)}/crossmatched_points{i}.csv ra1={ra_1} dec1={dec_1} ra2={ra_2} dec2={dec_2} error={dist}")
  }
  
  command_string <- open_stilts_string + tskymatch2_string
  print(command_string)
  
  # Except for intern, remain other positional parameters default
  system(command_string, intern = FALSE,
         ignore.stdout = FALSE, ignore.stderr = FALSE,
         wait = TRUE, input = NULL, show.output.on.console = TRUE,
         minimized = FALSE, invisible = TRUE, timeout = 0)
  
  read_table <- fread(glue("{dirname(table_1)}/crossmatched_points{i}.csv"), sep=",", header=TRUE, fill=TRUE)
}



split.table.1.fread.fwrite <- function(table1_path, num_cores=NULL)
{
  # Read table
  table_1 <- fread(table1_path, sep=",", header=TRUE, fill=TRUE)
  
  # Detect the number of cores
  if (is.null(num_cores))
  {
    num_cores <- detectCores()
  }
  
  # Split the table
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
  
  # Write the table in the disk
  table_1_list <- list()
  
  for(i in 1:(length(index)/2))
  {
    first_index <- index[[i*2-1]]
    second_index <- index[[i*2]]
    
    new_table_1 <- table_1[first_index:second_index,]
    new_path <- paste(dirname(table1_path), "/datasplited/", str_split(basename(table1_path), ".csv", simplify=TRUE)[1], as.character(i), '.csv', sep="")
    
    fwrite(new_table_1, new_path, row.names=FALSE)
    table_1_list <- list.append(table_1_list, new_path)
  }
  
  return(table_1_list)
}


stilts.invoke.command.parallel.fread.fwrite <- function(table_1_list, table_2, ra_1, ra_2, dec_1, dec_2, dist, num_cores=NULL)
{
  if (is.null(num_cores))
  {
    num_cores <- detectCores()
  }
  registerDoParallel(num_cores)
  
  
  # still using entire.bruteforce.crossmatch.two.catelogs
  foreach(i = 1:num_cores, .combine=rbind) %dopar%
    {
      return(stilts.invoke.command.fread.fwrite(table_1_list[[i]], table_2, ra_1, ra_2, dec_1, dec_2, dist, i))
    }
}
################################################################################################################



################################################################################################################
## 3. vroom library
#     vroom(), vroom_write()
library(vroom)

stilts.invoke.command.vroom <- function(table_1, table_2, ra_1, ra_2, dec_1, dec_2, dist, i=NULL)
{
  open_stilts_string <- "java -jar stilts.jar "
  
  if (is.null(i))
  {
    tskymatch2_string <- glue("tskymatch2 in1={table_1} in2={table_2} out={dirname(table_1)}/crossmatched_points.csv ra1={ra_1} dec1={dec_1} ra2={ra_2} dec2={dec_2} error={dist}")
  }
  else
  {
    tskymatch2_string <- glue("tskymatch2 in1={table_1} in2={table_2} out={dirname(table_1)}/crossmatched_points{i}.csv ra1={ra_1} dec1={dec_1} ra2={ra_2} dec2={dec_2} error={dist}")
  }
  
  command_string <- open_stilts_string + tskymatch2_string
  print(command_string)
  
  # Except for intern, remain other positional parameters default
  system(command_string, intern = FALSE,
         ignore.stdout = FALSE, ignore.stderr = FALSE,
         wait = TRUE, input = NULL, show.output.on.console = TRUE,
         minimized = FALSE, invisible = TRUE, timeout = 0)
  
  read_table <- vroom(glue("{dirname(table_1)}/crossmatched_points{i}.csv"))
}



split.table.1.vroom <- function(table1_path, num_cores=NULL)
{
  # Read table
  table_1 <- vroom(table1_path)
  
  # Detect the number of cores
  if (is.null(num_cores))
  {
    num_cores <- detectCores()
  }
  
  # Split the table
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
  
  # Write the table in the disk
  table_1_list <- list()
  
  for(i in 1:(length(index)/2))
  {
    first_index <- index[[i*2-1]]
    second_index <- index[[i*2]]
    
    new_table_1 <- table_1[first_index:second_index,]
    new_path <- paste(dirname(table1_path), "/datasplited/", str_split(basename(table1_path), ".csv", simplify=TRUE)[1], as.character(i), '.csv', sep="")
    
    vroom_write(new_table_1, new_path, ",")
    table_1_list <- list.append(table_1_list, new_path)
  }
  
  return(table_1_list)
}


stilts.invoke.command.parallel.vroom <- function(table_1_list, table_2, ra_1, ra_2, dec_1, dec_2, dist, num_cores=NULL)
{
  if (is.null(num_cores))
  {
    num_cores <- detectCores()
  }
  registerDoParallel(num_cores)
  
  
  # still using entire.bruteforce.crossmatch.two.catelogs
  foreach(i = 1:num_cores, .combine=rbind) %dopar%
    {
      return(stilts.invoke.command.vroom(table_1_list[[i]], table_2, ra_1, ra_2, dec_1, dec_2, dist, i))
    }
}
################################################################################################################



################################################################################################################
## 4. mixed library
#     vroom(), fwrite()
split.table.1.vroom.fwrite <- function(table1_path, num_cores=NULL)
{
  # Read table
  table_1 <- vroom(table1_path)
  
  # Detect the number of cores
  if (is.null(num_cores))
  {
    num_cores <- detectCores()
  }
  
  # Split the table
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
  
  # Write the table in the disk
  table_1_list <- list()
  
  for(i in 1:(length(index)/2))
  {
    first_index <- index[[i*2-1]]
    second_index <- index[[i*2]]
    
    new_table_1 <- table_1[first_index:second_index,]
    new_path <- paste(dirname(table1_path), "/datasplited/", str_split(basename(table1_path), ".csv", simplify=TRUE)[1], as.character(i), '.csv', sep="")
    
    fwrite(new_table_1, new_path, row.names=FALSE)
    table_1_list <- list.append(table_1_list, new_path)
  }
  
  return(table_1_list)
}
################################################################################################################



################################################################################################################
# TEST PURPOSE 
# 1. STILTS
# stilts.invoke.command(table1_path, table2_path, ra_1, ra_2, dec_1, dec_2, 0.5)

# 2. PARALLEL STILTS
# table_1_list <- split.table.1(table1_path)
# stilts.invoke.command.parallel(table_1_list, table2_path, ra_1, ra_2, dec_1, dec_2, 0.5, num_cores=NULL)
################################################################################################################



################################################################################################################
# 3. COMPARE TIME
compare.time.stilts.without.splits <- function()
{
  # [NOTE] data table 1 is already splitted.
  # built-in functions of reading and writing files
  start <- proc.time()
  built.in <- stilts.invoke.command.parallel(table_1_list, table2_path, ra_1, ra_2, dec_1, dec_2, 0.5, num_cores=NULL)
  built.in.time <- proc.time()-start
  
  # data.table library functions of reading and writing files
  start <- proc.time()
  data.table.lib <- stilts.invoke.command.parallel.fread.fwrite(table_1_list, table2_path, ra_1, ra_2, dec_1, dec_2, 0.5, num_cores=NULL)
  data.table.lib.time <- proc.time()-start
  
  # vroom library functions of reading and writing files
  start <- proc.time()
  vroom.lib <- stilts.invoke.command.parallel.vroom(table_1_list, table2_path, ra_1, ra_2, dec_1, dec_2, 0.5, num_cores=NULL)
  vroom.time <- proc.time()-start
  
  
  return(rbind(built.in.time, data.table.lib.time, vroom.time)[,1:3])
  
  #######################RESULT##################
  #user.self sys.self elapsed
  #built.in.time           5.931   11.814  39.109
  #data.table.lib.time     1.840   10.850  51.582
  #vroom.time              5.824   11.788  55.507
}
################################################################################################################



################################################################################################################
# 3. COMPARE TIME USING TICTOC
compare.time.stilts.with.splits.tictoc <- function(table1_path, table2_path, ra_1, ra_2, dec_1, dec_2, dist, num_cores=NULL)
{
  for (i in 1:10)
  {
    # built-in functions of reading and writing files
    tic("tic1")
    table_1_list <- split.table.1(table1_path)
    built.in <- stilts.invoke.command.parallel(table_1_list, table2_path, ra_1, ra_2, dec_1, dec_2, dist, num_cores=NULL)
    toc(log=TRUE)
    
    # data.table library functions of reading and writing files
    tic('tic2')
    table_1_list <- split.table.1.fread.fwrite(table1_path)
    data.table.lib <- stilts.invoke.command.parallel.fread.fwrite(table_1_list, table2_path, ra_1, ra_2, dec_1, dec_2, 0.5, num_cores=NULL)
    toc(log=TRUE)
    
    # vroom library functions of reading and writing files
    tic('tic3')
    table_1_list <- split.table.1.vroom(table1_path)
    vroom.lib <- stilts.invoke.command.parallel.vroom(table_1_list, table2_path, ra_1, ra_2, dec_1, dec_2, 0.5, num_cores=NULL)
    toc(log=TRUE)
    
    tic('tic4')
    table_1_list <- split.table.1.vroom(table1_path)
    vroom.lib <- stilts.invoke.command.parallel.vroom(table_1_list, table2_path, ra_1, ra_2, dec_1, dec_2, 0.5, num_cores=1)
    toc(log=TRUE)
    
    tic('tic5')
    table_1_list <- split.table.1.vroom(table1_path, num_cores=1)
    vroom.lib <- stilts.invoke.command.parallel.vroom(table_1_list, table2_path, ra_1, ra_2, dec_1, dec_2, 0.5, num_cores=1)
    toc(log=TRUE)
    
    tic('tic6')
    table_1_list <- split.table.1.vroom.fwrite(table1_path)
    vroom.lib <- stilts.invoke.command.parallel.vroom(table_1_list, table2_path, ra_1, ra_2, dec_1, dec_2, 0.5, num_cores=1)
    toc(log=TRUE)
    
    tic('tic7')
    table_1_list <- split.table.1.vroom.fwrite(table1_path)
    vroom.lib <- stilts.invoke.command.parallel.vroom(table_1_list, table2_path, ra_1, ra_2, dec_1, dec_2, 0.5, num_cores=NULL)
    toc(log=TRUE)
    
    tic('tic8')
    table_1_list <- split.table.1.vroom.fwrite(table1_path, num_cores=1)
    vroom.lib <- stilts.invoke.command.parallel.vroom(table_1_list, table2_path, ra_1, ra_2, dec_1, dec_2, 0.5, num_cores=1)
    toc(log=TRUE)
    
    log.txt <- tic.log(format=TRUE)
    tic.clearlog()
    
    writeLines(unlist(log.txt))
    
    write(unlist(log.txt), file = "./values.txt",
          append = TRUE, sep = ",")
  }
  # tic1: 144.982 sec elapsed
  # tic2: 120.504 sec elapsed
  # tic3: 161.047 sec elapsed
  # tic4: 55.517 sec elapsed
  # tic5: 60.409 sec elapsed
  # tic6: 28.785 sec elapsed
  # tic7: 133.27 sec elapsed
  # tic8: 41.418 sec elapsed
  
  ############ RESULTS
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
###############################################################################################################



##################################### COMPARING DIFFERENT FUNCTIONS OF READING AND WRITING FILES###############
# Q. WRITING?
# A. DATA.TABLE < VROOM < BUILT-IN
compare.writing.functions <- function()
{
  tic('tic1')
  write.csv(table1, "/sharedata/fastdisk/suyeonju101/milliquas77RADECName.csv", row.names=FALSE)
  write.csv(table2, "/sharedata/fastdisk/suyeonju101/milliquas77cRADECName.csv", row.names=FALSE)
  toc(log=TRUE)
  
  tic('tic2')
  fwrite(table1, "/sharedata/fastdisk/suyeonju101/milliquas77RADECName.csv", row.names=FALSE)
  fwrite(table2, "/sharedata/fastdisk/suyeonju101/milliquas77cRADECName.csv", row.names=FALSE)
  toc(log=TRUE)
  
  tic('tic3')
  vroom_write(table1, "/sharedata/fastdisk/suyeonju101/milliquas77RADECName.csv", ",")
  vroom_write(table2, "/sharedata/fastdisk/suyeonju101/milliquas77cRADECName.csv", ",")
  toc(log=TRUE)
  
  log.txt <- tic.log(format=TRUE)
  tic.clearlog()
  
  writeLines(unlist(log.txt))
  
  # ========RESULTS=========
  # tic1: 20.232 sec elapsed
  # tic2: 0.4990 sec elapsed
  # tic3: 0.6300 sec elapsed
}

# Q. READING TXT?
# A. VROOM < DATA.TABLE < BUILT-IN
compare.reading.txt.functions <- function()
{
  tic('tic1')
  table1 <- read.table("/sharedata/fastdisk/suyeonju101/milliquas77RADECName.txt", sep=",", header=TRUE, fill=TRUE)
  table2 <- read.table("/sharedata/fastdisk/suyeonju101/milliquas77cRADECName.txt", sep=",", header=TRUE, fill=TRUE)
  toc(log=TRUE)
  
  tic('tic2')
  table1 <- fread("/sharedata/fastdisk/suyeonju101/milliquas77RADECName.txt", sep=",", header=TRUE, fill=TRUE)
  table2 <- fread("/sharedata/fastdisk/suyeonju101/milliquas77cRADECName.txt", sep=",", header=TRUE, fill=TRUE)
  toc(log=TRUE)
  
  tic('tic3')
  table1 <- vroom("/sharedata/fastdisk/suyeonju101/milliquas77RADECName.txt")
  table2 <- vroom("/sharedata/fastdisk/suyeonju101/milliquas77cRADECName.txt")
  toc(log=TRUE)
  
  log.txt <- tic.log(format=TRUE)
  tic.clearlog()
  
  writeLines(unlist(log.txt))
  
  # ========RESULTS=========
  # tic1: 15.36 sec elapsed
  # tic2: 4.532 sec elapsed
  # tic3: 0.674 sec elapsed
}

# Q. READING CSV?
# A. VROOM < DATA.TABLE < BUILT-IN
compare.reading.csv.functions <- function()
{
  tic('tic1')
  read.table("/sharedata/fastdisk/suyeonju101/milliquas77RADECName.csv", sep=",", header=TRUE, fill=TRUE)
  read.table("/sharedata/fastdisk/suyeonju101/milliquas77cRADECName.csv", sep=",", header=TRUE, fill=TRUE)
  toc(log=TRUE)
  
  tic('tic2')
  fread("/sharedata/fastdisk/suyeonju101/milliquas77RADECName.csv", sep=",", header=TRUE, fill=TRUE)
  fread("/sharedata/fastdisk/suyeonju101/milliquas77cRADECName.csv", sep=",", header=TRUE, fill=TRUE)
  toc(log=TRUE)
  
  tic('tic3')
  vroom("/sharedata/fastdisk/suyeonju101/milliquas77RADECName.csv")
  vroom("/sharedata/fastdisk/suyeonju101/milliquas77cRADECName.csv")
  toc(log=TRUE)
  
  log.txt <- tic.log(format=TRUE)
  tic.clearlog()
  
  writeLines(unlist(log.txt))
  
  # ========RESULTS=========
  # tic1: 13.234 sec elapsed
  # tic2: 4.302 sec elapsed
  # tic3: 0.651 sec elapsed
}
################################################################################################################

# compare.writing.functions()
# compare.reading.txt.functions()
# compare.reading.csv.functions()
# compare.time.stilts.with.splits.tictoc(table1_path, table2_path, ra_1, ra_2, dec_1, dec_2, 0.5)
# Big Data Analysis

## Table of Contents
1. [Introduction](#introduction)
2. [Project Structure](#project-structure)
3. [Data](#data)
4. [Running the Project](#running-the-project)
5. [Data Analysis and Visualization](#data-analysis-and-visualization)
6. [Conclusion](#conclusion)
7. [Team Members](#team-members)
8. [Acknowledgments](#acknowledgments)

## Introduction

Welcome to our Big Data Analysis Project! This project is part of the Big Data course at the Universidad de Chile. The main goal of this project is to analyze the percentage of bicycle usage in Barcelonaover the years 2019, 2020, and 2023. The data is provided in CSV format, and we will be using Apache Hadoop with Java to perform distributed data processing.

## Project Structure

The project is organized into the following directories:

1. `dist`: Contains the mdp-hadoop.jar file created by build.xml file.
2. `lib`: Contains jar files
3. `src`: Contains the java code of the project 

## Data

The data for this project is stored in CSV format. These files have been organized in folders of the year 2019, 2020 and 2023. The main datasets of each folder include:

1. **XXXX_01_STATIONS.csv**: Contains data related to January.
2. **XXXX_02_STATIONS.csv**: Contains data related to February.
3. **XXXX_03_STATIONS.csv**: Contains data related to March.
4. **XXXX_04_STATIONS.csv**: Contains data related to April.
5. **XXXX_05_STATIONS.csv**: Contains data related to May.
6. **XXXX_06_STATIONS.csv**: Contains data related to June.
7. **XXXX_07_STATIONS.csv**: Contains data related to July.
8. **XXXX_08_STATIONS.csv**: Contains data related to August.
9. **XXXX_09_STATIONS.csv**: Contains data related to September.
10. **XXXX_10_STATIONS.csv**: Contains data related to October.
11. **XXXX_11_STATIONS.csv**: Contains data related to November.
12. **XXXX_12_STATIONS.csv**: Contains data related to December.
13. **XXXX_INFO.csv**: Contains data related to the info of the year.

For viewing the data files, you can access them through the following link: [Bike Sharing](https://www.kaggle.com/datasets/edomingo/bicing-stations-dataset-bcn-bike-sharing/data)

## Running the Project

1. Copy the JAR file to your server with the command: scp -P `port` `local-path`/mdp-hadoop.jar `server-name`:/`server-path`/.
2. Copy the data files to your server with the command: scp -P `port` `local-path`/`csv-file-name`.csv `server-name`:/`server-path`/.
3. Now call the Hadoop job with the command: hadoop jar `local-path`/mdp-hadoop.jar CountAvailableDocksByMonth /`server-input-path`/XXXX_XX_STATIONS.csv /`server-output-path`/
4. Watch the results with the command: hdfs dfs -cat /`server-output-path`/part-r-00000

## Data Analysis and Visualization

Once we have gathered and processed the data, we will perform data analysis and visualization.

### Graph of the Analysis
How has the percentage of bicycle usage in Barcelona changed over the years 2019, 2020, and 2023?
<img src="https://github.com/matiasAguirreE/big-data-analysis/blob/main/media/graph.jpg?raw=true" alt="Graph of the Analysis" width="1000">

## Conclusion

Lorem ipsum dolor sit amet. Ea iure dolorem in explicabo doloribus est porro voluptas aut atque aperiam sed adipisci repellat est accusantium amet sed cupiditate consectetur. Aut voluptatibus sapiente et aspernatur omnis ut consequatur saepe ad ducimus architecto et expedita dolore vel nesciunt ipsa.

## Team Members

- [Matías Aguirre Erazo](https://github.com/matiasAguirreE)
- [Tomás Rivas Acuña](https://github.com/Trivas2000)
- [Scarleth Betancurt Contreras](https://github.com/scarleth-bc)

## Acknowledgments

We would like to thank the Universidad de Chile and our professors for their guidance and support throughout this project. We also appreciate the open-source community.

# Reddit-ETL-Pipeline
The project aims to collect **data from Reddit related to cryptocurrency discussions from all over the world**. The collected data will be preprocessed to extract the **sentiment of the persons who posted or commented on the cryptocurrency-related discussions**. The sentiment analysis will help in determining the overall attitude towards cryptocurrencies in different parts of the world.

The project will be implemented as a **data pipeline** that can handle **real-time streaming data from Reddit**. The pipeline will include **data collection, preprocessing**, **sentiment analysis**, and visualization of the results. The visualization will help in identifying the hotspots of cryptocurrency discussions and their associated sentiments across the globe.

Overall, the project will provide insights into the sentiment of the general public towards cryptocurrencies in real-time, which can be useful for making informed decisions related to cryptocurrency investments, marketing, and other related areas. 

<h1>Project Architecture</h1> 

<h8> The project you described is a data pipeline that collects data from Reddit API, processes it, and visualizes the results. Here is a high-level overview of the project:

**Collecting data** from Reddit API using an EC2 instance: The project uses an EC2 instance to connect to the Reddit API and collect data. EC2 is a scalable compute resource that can be configured to run applications and services.

**Storing data in S3**: After the data is collected, it is stored in S3, a highly scalable, durable, and secure object storage service offered by AWS.

**Sentiment analysis with AWS Glue**: AWS Glue is a fully-managed extract, transform, and load (ETL) service that makes it easy to move data between data stores. In this project, AWS Glue is used to perform sentiment analysis on the Reddit data using libraries such as NLTK, TextBlob, or Amazon Comprehend.

**Storing results in Redshift**: Amazon Redshift is a fast, scalable, and cost-effective data warehouse that makes it easy to analyze large amounts of data. The sentiment analysis results are stored in Redshift for further analysis and visualization.


Kafka for real-time streaming: Kafka is a distributed streaming platform that can be used to build real-time streaming applications. In this project, Kafka is used as a producer-consumer architecture to enable real-time streaming of the data.

Overall, this project demonstrates how to build a scalable data pipeline that collects data from an API, processes it using machine learning libraries, and visualizes the results. By leveraging AWS services such as EC2, S3, Glue, Redshift, and Kafka, it is possible to build a highly scalable and cost-effective data pipeline that can handle large amounts of data. </h8>
<a href="https://www.imgur.com"><img src="https://i.imgur.com/nZGMItn.jpeg" alt="Alt Text" title="Click to enlarge" width="800" height="400" /></a>

<h2>Steps for realizing the project</h2>
<h3> Start by scraping data in EC2 instance </h3> 
<a href="https://www.imgur.com"><img src="https://i.imgur.com/yQj0xqS.jpg" alt="Alt Text" title="Click to enlarge" width="800" height="400" /></a>
<a href="https://www.imgur.com"><img src="https://i.imgur.com/ft5wUez.jpg" alt="Alt Text" title="Click to enlarge" width="800" height="400" /></a>

<h3> Data ingestion into S3 Bucket </h3>
<a href="https://www.imgur.com"><img src="https://i.imgur.com/nCcJVuf.jpg" alt="Alt Text" title="Click to enlarge" width="800" height="400" /></a>
<a href="https://www.imgur.com"><img src="https://i.imgur.com/GU0qYWV.jpg" alt="Alt Text" title="Click to enlarge" width="800" height="400" /></a>


<h3> Data preprocessing and sentiment analysis in AWS Glue </h3>
<a href="https://www.imgur.com"><img src="https://i.imgur.com/F5g0ctA.jpg" alt="Alt Text" title="Click to enlarge" width="800" height="400" /></a>

<a href="https://www.imgur.com"><img src="https://i.imgur.com/WpH6QJ6.jpg" alt="Alt Text" title="Click to enlarge" width="800" height="400" /></a>





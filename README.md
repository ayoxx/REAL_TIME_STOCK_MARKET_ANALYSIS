## Project Name: Real Time Stock Market Analysis
    The project implements a real-time data pipeline that extracts stock data from 
    vantage API, streams it through Apache kafka, processes it with Apache Spark, and 
    loads it into a postgres database.

    All components are containerized with Docker for easy deployment.


### Data Pipeline Architecture
![Data Pipeline Architecture](./img/pipeline_architecture.png)


Project Tech Stack and Flow
- kafka UI > inspect topics/messages.
- API > produces JSON events into kafka.
- Spark > consumes from kafka, writes to Postgres.
- Postgres > stores results for analytics.
- pgAdmin > manage Postgres visually.
- Power BI > external (connects to Postgres database).



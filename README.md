# Flink Beam Application

This project sets up a **Kubernetes cluster** and installs **Apache Beam**, **Apache Kafka**, and **Apache Flink**. The main goal is to process temperature data from Kafka using a Beam pipeline and categorize it into different topics based on the values.

Also we use a kafka producer to send temperature values to kafka. This producer running on kubernetes as a pod.

- Sets up **Kubernetes** and deploys **Kafka, Flink, and Beam**.
- Sends producer data to kafka and reads temperature values from a Kafka topic.
- **If the temperature is greater than 20°C**, it is sent to the `high-temperature` topic.
- **If the temperature is 20°C or lower**, it is sent to the `low-temperature` topic.
- Runs the **Beam pipeline on Flink** using **FlinkRunner**.

## Installation

After clone the repo, please run this command: **bash setup.sh**



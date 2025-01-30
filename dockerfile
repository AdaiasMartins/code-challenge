FROM apache/airflow:2.5.1

USER root

# Instalar o Java (necessário para Embulk)
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless

# Baixar o Embulk
RUN curl -O https://dl.bintray.com/embulk/maven/embulk-0.9.25.jar

USER airflow

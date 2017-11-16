FROM ubuntu:14.04

MAINTAINER TripleZ <me@triplez.cn>

WORKDIR /root

# backup old sources.list
RUN cp /etc/apt/sources.list /etc/apt/sources.list.bac

# Change ubuntu repo to mirrors.163.com
RUN echo "deb http://mirrors.163.com/ubuntu/ trusty main restricted universe multiverse" > /etc/apt/sources.list && \
    echo "deb http://mirrors.163.com/ubuntu/ trusty-security main restricted universe multiverse" >> /etc/apt/sources.list && \
    echo "deb http://mirrors.163.com/ubuntu/ trusty-updates main restricted universe multiverse" >> /etc/apt/sources.list && \
    echo "deb http://mirrors.163.com/ubuntu/ trusty-proposed main restricted universe multiverse" >> /etc/apt/sources.list && \
    echo "deb http://mirrors.163.com/ubuntu/ trusty-backports main restricted universe multiverse" >> /etc/apt/sources.list && \
    echo "deb-src http://mirrors.163.com/ubuntu/ trusty main restricted universe multiverse" >> /etc/apt/sources.list && \
    echo "deb-src http://mirrors.163.com/ubuntu/ trusty-security main restricted universe multiverse" >> /etc/apt/sources.list && \
    echo "deb-src http://mirrors.163.com/ubuntu/ trusty-updates main restricted universe multiverse" >> /etc/apt/sources.list && \
    echo "deb-src http://mirrors.163.com/ubuntu/ trusty-proposed main restricted universe multiverse" >> /etc/apt/sources.list && \
    echo "deb-src http://mirrors.163.com/ubuntu/ trusty-backports main restricted universe multiverse" >> /etc/apt/sources.list


# install openssh-server, openjdk and wget
RUN apt-get update && apt-get install -y openssh-server openjdk-7-jdk wget


RUN wget http://mirrors.tuna.tsinghua.edu.cn/apache/hadoop/common/hadoop-2.7.4/hadoop-2.7.4.tar.gz && \
    tar -xzvf hadoop-2.7.4.tar.gz && \
    mv hadoop-2.7.4 /usr/local/hadoop && \
    rm hadoop-2.7.4.tar.gz

# set environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64 
ENV HADOOP_HOME=/usr/local/hadoop 
ENV HADOOP_CONFIG_HOME=/usr/local/hadoop/etc/hadoop
ENV PATH=$PATH:/usr/local/hadoop/bin:/usr/local/hadoop/sbin 

# ssh without key
RUN ssh-keygen -t rsa -f ~/.ssh/id_rsa -P '' && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

RUN mkdir -p ~/hdfs/namenode && \ 
    mkdir -p ~/hdfs/datanode && \
    mkdir $HADOOP_HOME/logs


RUN echo "Host localhost \
StrictHostKeyChecking no \
Host 0.0.0.0 \
StrictHostKeyChecking no \
Host hadoop-* \
StrictHostKeyChecking no \
UserKnownHostsFile=/dev/null" > ~/.ssh/config


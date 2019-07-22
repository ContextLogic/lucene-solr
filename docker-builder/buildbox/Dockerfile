FROM openjdk:11
ENV ANT_VERSION 1.10.5
RUN mkdir -p /opt/ant && \
    curl -s http://www.us.apache.org/dist/ant/binaries/apache-ant-${ANT_VERSION}-bin.tar.gz | \
    tar -xzv --strip 1 -C /opt/ant
ENV ANT_HOME /opt/ant
ENV PATH ${PATH}:/opt/ant/bin

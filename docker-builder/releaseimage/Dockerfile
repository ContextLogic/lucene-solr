FROM openjdk:11-stretch

RUN apt-get update && \
  apt-get -y install acl dirmngr gpg lsof procps wget && \
  rm -rf /var/lib/apt/lists/*

ENV SOLR_USER="solr" \
    SOLR_UID="8983" \
    SOLR_GROUP="solr" \
    SOLR_GID="8983" \
    SOLR_VERSION="8.1.2-SNAPSHOT" \
    PATH="/opt/solr/bin:/opt/docker-solr/scripts:$PATH" \
    SOLR_INCLUDE=/etc/default/solr.in.sh \
    SOLR_HOME=/var/solr/data \
    SOLR_PID_DIR=/var/solr \
    SOLR_LOGS_DIR=/var/solr/logs \
    LOG4J_PROPS=/var/solr/log4j2.xml

ENV GOSU_VERSION 1.11
ENV GOSU_KEY B42F6819007F00F88E364FD4036A9C25BF357DD4

RUN groupadd -r --gid "$SOLR_GID" "$SOLR_GROUP" && \
  useradd -r --uid "$SOLR_UID" --gid "$SOLR_GID" "$SOLR_USER"

COPY solr-$SOLR_VERSION.tgz /opt/solr-$SOLR_VERSION.tgz

RUN set -e; \
  export GNUPGHOME="/tmp/gnupg_home" && \
  pkgArch="$(dpkg --print-architecture | awk -F- '{ print $NF }')" && \
  wget -O /usr/local/bin/gosu "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$pkgArch" && \
  chmod +x /usr/local/bin/gosu && \
  gosu nobody true && \
  tar -C /opt --extract --file "/opt/solr-$SOLR_VERSION.tgz" && \
  (cd /opt && ln -s "solr-$SOLR_VERSION" solr) && \
  rm "/opt/solr-$SOLR_VERSION.tgz"* && \
  rm -Rf /opt/solr/docs/ && \
  mkdir -p /opt/solr/server/solr/lib /docker-entrypoint-initdb.d /opt/docker-solr && \
  chown -R 0:0 "/opt/solr-$SOLR_VERSION" && \
  find "/opt/solr-$SOLR_VERSION" -type d -print0 | xargs -0 chmod 0755 && \
  find "/opt/solr-$SOLR_VERSION" -type f -print0 | xargs -0 chmod 0644 && \
  chmod -R 0755 "/opt/solr-$SOLR_VERSION/bin" "/opt/solr-$SOLR_VERSION/contrib/prometheus-exporter/bin/solr-exporter" && \
  cp /opt/solr/bin/solr.in.sh /etc/default/solr.in.sh && \
  mv /opt/solr/bin/solr.in.sh /opt/solr/bin/solr.in.sh.orig && \
  mv /opt/solr/bin/solr.in.cmd /opt/solr/bin/solr.in.cmd.orig && \
  chown root:0 /etc/default/solr.in.sh && \
  chmod 0664 /etc/default/solr.in.sh && \
  mkdir -p /var/solr/data /var/solr/logs && \
  (cd /opt/solr/server/solr && cp solr.xml zoo.cfg /var/solr/data/) && \
  cp /opt/solr/server/resources/log4j2.xml /var/solr/log4j2.xml && \
  find /var/solr -type d -print0 | xargs -0 chmod 0770 && \
  find /var/solr -type f -print0 | xargs -0 chmod 0660 && \
  sed -i -e "s/\"\$(whoami)\" == \"root\"/\$(id -u) == 0/" /opt/solr/bin/solr && \
  sed -i -e 's/lsof -PniTCP:/lsof -t -PniTCP:/' /opt/solr/bin/solr && \
  chown -R "0:0" /opt/solr-$SOLR_VERSION /docker-entrypoint-initdb.d /opt/docker-solr && \
  chown -R "$SOLR_USER:0" /var/solr

COPY --chown=0:0 scripts /opt/docker-solr/scripts

VOLUME /var/solr
EXPOSE 8983
WORKDIR /opt/solr
USER $SOLR_USER

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["solr-foreground"]

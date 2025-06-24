FROM docker.artifactoryprod.888holdings.corp/bi/maven:20250624.2

COPY pom.xml /usr/share/flink-process/
COPY src /usr/share/flink-process/src/
COPY .mvn /usr/share/flink-process/.mvn/

WORKDIR /usr/share/flink-process/

RUN mvn package -s .mvn/settings.xml

FROM docker.artifactoryprod.888holdings.corp/bi/flink:20250624.2

# Copy application jar
COPY --from=0 /usr/share/flink-process/target /usr/share/flink-process/target


RUN \
  ln -s /usr/share/flink-process/target $FLINK_HOME/usrlib \
  && cp /opt/flink/plugins/metrics-prometheus/flink-metrics-prometheus-2.0.0.jar /opt/flink/lib

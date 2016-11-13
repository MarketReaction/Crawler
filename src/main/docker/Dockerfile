FROM hypriot/rpi-java:1.8.0
#ENTRYPOINT ["java", "-javaagent:/opt/jassoft/Crawler.jar", "-Dnewrelic.config.file=/opt/jassoft/newrelic.yml", "-jar", "/opt/jassoft/Crawler.jar"]
ENTRYPOINT exec java $JAVA_OPTS -jar /opt/jassoft/Crawler.jar
ADD newrelic.yml /opt/jassoft/newrelic.yml
ADD Crawler.jar /opt/jassoft/Crawler.jar

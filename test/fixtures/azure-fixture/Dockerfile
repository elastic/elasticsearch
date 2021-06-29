FROM ubuntu:18.04
RUN apt-get update -qqy
RUN apt-get install -qqy openjdk-11-jre-headless
ENTRYPOINT exec java -classpath "/fixture/shared/*" fixture.azure.AzureHttpFixture 0.0.0.0 8091 azure_integration_test_account container
EXPOSE 8091

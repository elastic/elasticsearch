FROM ubuntu:20.04
RUN apt-get update -qqy
RUN apt-get install -qqy openjdk-17-jre-headless
ENTRYPOINT exec java -classpath "/fixture/shared/*" fixture.azure.AzureHttpFixture 0.0.0.0 8091 azure_integration_test_account container
EXPOSE 8091

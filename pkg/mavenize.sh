V=0.19.0-SNAPSHOT
cd ../modules/

# 1. add to all build.gradle files: elasticsearch, jarjar, test/testng, test/integration
# apply plugin: 'maven'

# 2. generate pom-default.xml
# cd jarjar/; ../../gradlew clean install; cd ..
# cd test/testng/; ../../../gradlew clean install; cd ../..
# now implicitely call elasticsearch
# cd test/integration/; ../../../gradlew clean install; cd ../..

# use gradle created pom
cp ./elasticsearch/build/poms/pom-default.xml ./elasticsearch/pom.xml

# add hyperic dep (which is not present in the gradle deps?)
sed -i 's/<dependencies>/<dependencies>\n<dependency>\n<groupId>org.hyperic<\/groupId>\n<artifactId>sigar<\/artifactId>\n<version>1.6.4.129<\/version>\n<\/dependency>/g' ./elasticsearch/pom.xml

# add jarjar dep (which is generated from subproject)
sed -i 's/<dependencies>/<dependencies>\n<dependency>\n<groupId>org.elasticsearch<\/groupId>\n<artifactId>jarjar<\/artifactId>\n<\/dependency>/g' ./elasticsearch/pom.xml

# add jboss repo for hyperic
sed -i 's/<\/dependencies>/<\/dependencies>\n<repositories>\n<repository>\n<name>JBOSS<\/name>\n<id>JBOSS<\/id>\n<url>https:\/\/repository.jboss.org\/nexus\/content\/groups\/public<\/url>\n<\/repository>\n<\/repositories>/g' ./elasticsearch/pom.xml

# TODO include test files ending on *Tests too
#<build><plugins><plugin>
#<groupId>org.apache.maven.plugins</groupId>
#<artifactId>maven-surefire-plugin</artifactId>
#<version>2.10</version>
#<executions>
#<execution>
#    <phase>test</phase>
#    <goals>
#        <goal>test</goal>
#    </goals>
#    <configuration>
#        <skip>false</skip>
#        <includes>
#            <include>**/*Tests.java</include>
#        </includes>
#    </configuration>
#</execution>
#</executions>
#</plugin></plugins></build>

# use gradle created poms also for subprojects
cp ./test/integration/build/poms/pom-default.xml ./test/integration/pom.xml
cp ./test/testng/build/poms/pom-default.xml ./test/testng/pom.xml

# apply some renaming
sed -i 's/elasticsearch-test-integration/test-integration/g' ./test/integration/pom.xml
sed -i 's/elasticsearch-test-testng/test-testng/g' ./test/testng/pom.xml

# force 1.6 compiler
for pomi in ./test/testng/pom.xml ./test/integration/pom.xml ./elasticsearch/pom.xml
do
 sed -i 's/<\/dependencies>/<\/dependencies>\n<build>\n<plugins>\n<plugin>\n<groupId>org.apache.maven.plugins<\/groupId>\n<artifactId>maven-compiler-plugin<\/artifactId>\n<version>2.3.2<\/version>\n<configuration>\n<source>1.6<\/source>\n<target>1.6<\/target>\n<\/configuration>\n<\/plugin>\n<\/plugins>\n<\/build>/g' $pomi 
done

# install jarjar
echo "mvn install:install-file -DgroupId=org.elasticsearch -DartifactId=jarjar -Dpackaging=jar -Dversion="$V" -Dfile=build/libs/jarjar-"$V".jar -DgeneratePom=true" > ./jarjar/install.sh
chmod +x ./jarjar/install.sh
cd ./jarjar/
./install.sh
rm install.sh
cd ..

# now install some jars (via normal maven procedure) for elasticsearch project
cd test/testng; mvn clean install; cd ../..
cd test/integration; mvn clean install; cd ../..

# move some configs around to match maven structure
mkdir -p ./elasticsearch/src/main/resources/org/elasticsearch/index/mapper/
mkdir -p ./elasticsearch/src/main/resources/config
mv ./elasticsearch/src/main/java/org/elasticsearch/index/mapper/default-mapping.json ./elasticsearch/src/main/resources/org/elasticsearch/index/mapper/default-mapping.json
mv ./elasticsearch/src/main/java/config/names.txt ./elasticsearch/src/main/resources/config/names.txt
rm -rf ./elasticsearch/src/main/java/config/


/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome
import org.xmlunit.builder.DiffBuilder
import org.xmlunit.builder.Input

class PublishPluginFuncTest extends AbstractGradleFuncTest {

    def setup() {
        // required for JarHell to work
        subProject(":libs:core") << "apply plugin:'java'"

        configurationCacheCompatible = false
    }

    def "artifacts and tweaked pom is published"() {
        given:
        buildFile << """
            plugins {
                id 'elasticsearch.java'
                id 'elasticsearch.publish'
            }

            version = "1.0"
            group = 'org.acme'
            description = "custom project description"
        """

        when:
        def result = gradleRunner('assemble').build()

        then:
        result.task(":generatePom").outcome == TaskOutcome.SUCCESS
        file("build/distributions/hello-world-1.0.jar").exists()
        file("build/distributions/hello-world-1.0-javadoc.jar").exists()
        file("build/distributions/hello-world-1.0-sources.jar").exists()
        file("build/distributions/hello-world-1.0.pom").exists()
        assertXmlEquals(file("build/distributions/hello-world-1.0.pom").text, """
            <project xmlns="http://maven.apache.org/POM/4.0.0" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <!-- This module was also published with a richer model, Gradle metadata,  -->
  <!-- which should be used instead. Do not delete the following line which  -->
  <!-- is to indicate to Gradle or any Gradle module metadata file consumer  -->
  <!-- that they should prefer consuming it instead. -->
  <!-- do_not_remove: published-with-gradle-metadata -->
  <modelVersion>4.0.0</modelVersion>
  <groupId>org.acme</groupId>
  <artifactId>hello-world</artifactId>
  <version>1.0</version>
  <name>hello-world</name>
  <description>custom project description</description>
  <url>unknown</url>
  <scm>
    <url>unknown</url>
  </scm>
  <inceptionYear>2009</inceptionYear>
  <licenses>
    <license>
      <name>Elastic License 2.0</name>
      <url>https://raw.githubusercontent.com/elastic/elasticsearch/v1.0/licenses/ELASTIC-LICENSE-2.0.txt</url>
      <distribution>repo</distribution>
    </license>
    <license>
      <name>GNU Affero General Public License Version 3</name>
      <url>https://raw.githubusercontent.com/elastic/elasticsearch/v1.0/licenses/AGPL-3.0+SSPL-1.0+ELASTIC-LICENSE-2.0.txt</url>
      <distribution>repo</distribution>
    </license>
    <license>
      <name>Server Side Public License, v 1</name>
      <url>https://www.mongodb.com/licensing/server-side-public-license</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <developers>
    <developer>
      <name>Elastic</name>
      <url>https://www.elastic.co</url>
    </developer>
  </developers>
</project>"""
        )
    }

    def "hides runtime dependencies and handles shadow dependencies"() {
        given:
        buildFile << """
            plugins {
                id 'elasticsearch.java'
                id 'elasticsearch.publish'
                id 'com.gradleup.shadow'
            }

            repositories {
                mavenCentral()
            }

            dependencies {
                implementation 'org.slf4j:log4j-over-slf4j:1.7.30'
                shadow 'org.slf4j:slf4j-api:1.7.30'
            }

            publishing {
                 repositories {
                    maven {
                        url = "\$buildDir/repo"
                    }
                 }
            }
            version = "1.0"
            group = 'org.acme'
            description = 'shadowed project'
        """

        when:
        def result = gradleRunner('assemble', '--stacktrace').build()

        then:
        result.task(":generatePom").outcome == TaskOutcome.SUCCESS
        file("build/distributions/hello-world-1.0-original.jar").exists()
        file("build/distributions/hello-world-1.0.jar").exists()
        file("build/distributions/hello-world-1.0-javadoc.jar").exists()
        file("build/distributions/hello-world-1.0-sources.jar").exists()
        file("build/distributions/hello-world-1.0.pom").exists()
        assertXmlEquals(file("build/distributions/hello-world-1.0.pom").text, """
            <project xmlns="http://maven.apache.org/POM/4.0.0" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
              <modelVersion>4.0.0</modelVersion>
              <groupId>org.acme</groupId>
              <artifactId>hello-world</artifactId>
              <version>1.0</version>
              <name>hello-world</name>
              <description>shadowed project</description>
              <url>unknown</url>
              <scm>
                <url>unknown</url>
              </scm>
              <inceptionYear>2009</inceptionYear>
              <licenses>
                <license>
                  <name>Elastic License 2.0</name>
                  <url>https://raw.githubusercontent.com/elastic/elasticsearch/v1.0/licenses/ELASTIC-LICENSE-2.0.txt</url>
                  <distribution>repo</distribution>
                </license>
                <license>
                  <name>GNU Affero General Public License Version 3</name>
                  <url>https://raw.githubusercontent.com/elastic/elasticsearch/v1.0/licenses/AGPL-3.0+SSPL-1.0+ELASTIC-LICENSE-2.0.txt</url>
                  <distribution>repo</distribution>
                </license>
                <license>
                  <name>Server Side Public License, v 1</name>
                  <url>https://www.mongodb.com/licensing/server-side-public-license</url>
                  <distribution>repo</distribution>
                </license>
              </licenses>
                <developers>
                  <developer>
                    <name>Elastic</name>
                    <url>https://www.elastic.co</url>
                  </developer>
                </developers>
              <dependencies>
                <dependency>
                  <groupId>org.slf4j</groupId>
                  <artifactId>slf4j-api</artifactId>
                  <version>1.7.30</version>
                  <scope>runtime</scope>
                </dependency>
              </dependencies>
            </project>"""
        )
    }

    def "handles project shadow dependencies"() {
        given:
        settingsFile << "include ':someLib'"
        file('someLib').mkdirs()
        buildFile << """
            plugins {
                id 'elasticsearch.java'
                id 'elasticsearch.publish'
                id 'com.gradleup.shadow'
            }

            dependencies {
                shadow project(":someLib")
            }
            publishing {
                 repositories {
                    maven {
                        url = "\$buildDir/repo"
                    }
                 }
            }

            allprojects {
                apply plugin: 'elasticsearch.java'
                version = "1.0"
                group = 'org.acme'
            }

            description = 'with shadowed dependencies'
        """

        when:
        def result = gradleRunner(':assemble', '--stacktrace').build()

        then:
        result.task(":generatePom").outcome == TaskOutcome.SUCCESS
        file("build/distributions/hello-world-1.0-original.jar").exists()
        file("build/distributions/hello-world-1.0.jar").exists()
        file("build/distributions/hello-world-1.0-javadoc.jar").exists()
        file("build/distributions/hello-world-1.0-sources.jar").exists()
        file("build/distributions/hello-world-1.0.pom").exists()
        assertXmlEquals(file("build/distributions/hello-world-1.0.pom").text, """
            <project xmlns="http://maven.apache.org/POM/4.0.0" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
              <modelVersion>4.0.0</modelVersion>
              <groupId>org.acme</groupId>
              <artifactId>hello-world</artifactId>
              <version>1.0</version>
              <name>hello-world</name>
              <description>with shadowed dependencies</description>
              <url>unknown</url>
              <scm>
                <url>unknown</url>
              </scm>
              <inceptionYear>2009</inceptionYear>
              <licenses>
                <license>
                  <name>Elastic License 2.0</name>
                  <url>https://raw.githubusercontent.com/elastic/elasticsearch/v1.0/licenses/ELASTIC-LICENSE-2.0.txt</url>
                  <distribution>repo</distribution>
                </license>
                <license>
                  <name>GNU Affero General Public License Version 3</name>
                  <url>https://raw.githubusercontent.com/elastic/elasticsearch/v1.0/licenses/AGPL-3.0+SSPL-1.0+ELASTIC-LICENSE-2.0.txt</url>
                  <distribution>repo</distribution>
                </license>
                <license>
                  <name>Server Side Public License, v 1</name>
                  <url>https://www.mongodb.com/licensing/server-side-public-license</url>
                  <distribution>repo</distribution>
                </license>
              </licenses>
              <developers>
                <developer>
                  <name>Elastic</name>
                  <url>https://www.elastic.co</url>
                </developer>
              </developers>
              <dependencies>
                <dependency>
                  <groupId>org.acme</groupId>
                  <artifactId>someLib</artifactId>
                  <version>1.0</version>
                  <scope>runtime</scope>
                </dependency>
              </dependencies>
            </project>"""
        )
    }

    def "generates artifacts for shadowed elasticsearch plugin"() {
        given:
        // we use the esplugin plugin in this test that is not configuration cache compatible yet
        configurationCacheCompatible = false
        file('license.txt') << "License file"
        file('notice.txt') << "Notice file"
        buildFile << """
            plugins {
                id 'elasticsearch.internal-es-plugin'
                id 'elasticsearch.publish'
                id 'com.gradleup.shadow'
            }

            esplugin {
                name = 'hello-world-plugin'
                classname 'org.acme.HelloWorldPlugin'
                description = "shadowed es plugin"
            }

            publishing {
                 repositories {
                    maven {
                        url = "\$buildDir/repo"
                    }
                 }
            }

            // requires elasticsearch artifact available
            tasks.named('bundlePlugin').configure { enabled = false }
            licenseFile.set(file('license.txt'))
            noticeFile.set(file('notice.txt'))
            version = "1.0"
            group = 'org.acme'
        """

        when:
        def result = gradleRunner('assemble', '--stacktrace', '-x', 'generateClusterFeaturesMetadata').build()

        then:
        result.task(":generatePom").outcome == TaskOutcome.SUCCESS
        file("build/distributions/hello-world-plugin-1.0-original.jar").exists()
        file("build/distributions/hello-world-plugin-1.0.jar").exists()
        file("build/distributions/hello-world-plugin-1.0-javadoc.jar").exists()
        file("build/distributions/hello-world-plugin-1.0-sources.jar").exists()
        file("build/distributions/hello-world-plugin-1.0.pom").exists()
        assertXmlEquals(file("build/distributions/hello-world-plugin-1.0.pom").text, """
            <project xmlns="http://maven.apache.org/POM/4.0.0" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
              <!-- This module was also published with a richer model, Gradle metadata,  -->
              <!-- which should be used instead. Do not delete the following line which  -->
              <!-- is to indicate to Gradle or any Gradle module metadata file consumer  -->
              <!-- that they should prefer consuming it instead. -->
              <!-- do_not_remove: published-with-gradle-metadata -->
              <modelVersion>4.0.0</modelVersion>
              <groupId>org.acme</groupId>
              <artifactId>hello-world-plugin</artifactId>
              <version>1.0</version>
              <name>hello-world</name>
              <description>shadowed es plugin</description>
              <url>unknown</url>
              <scm>
                <url>unknown</url>
              </scm>
              <inceptionYear>2009</inceptionYear>
              <licenses>
                <license>
                  <name>Elastic License 2.0</name>
                  <url>https://raw.githubusercontent.com/elastic/elasticsearch/v1.0/licenses/ELASTIC-LICENSE-2.0.txt</url>
                  <distribution>repo</distribution>
                </license>
                <license>
                  <name>GNU Affero General Public License Version 3</name>
                  <url>https://raw.githubusercontent.com/elastic/elasticsearch/v1.0/licenses/AGPL-3.0+SSPL-1.0+ELASTIC-LICENSE-2.0.txt</url>
                  <distribution>repo</distribution>
                </license>
                <license>
                  <name>Server Side Public License, v 1</name>
                  <url>https://www.mongodb.com/licensing/server-side-public-license</url>
                  <distribution>repo</distribution>
                </license>
              </licenses>
              <developers>
                <developer>
                  <name>Elastic</name>
                  <url>https://www.elastic.co</url>
                </developer>
              </developers>
            </project>"""
        )
    }

    def "generates pom for elasticsearch plugin"() {
        given:
        // we use the esplugin plugin in this test that is not configuration cache compatible yet
        configurationCacheCompatible = false
        file('license.txt') << "License file"
        file('notice.txt') << "Notice file"
        buildFile << """
            plugins {
                id 'elasticsearch.internal-es-plugin'
                id 'elasticsearch.publish'
            }

            esplugin {
                name = 'hello-world-plugin'
                classname 'org.acme.HelloWorldPlugin'
                description = "custom project description"
            }

            // requires elasticsearch artifact available
            tasks.named('bundlePlugin').configure { enabled = false }
            licenseFile.set(file('license.txt'))
            noticeFile.set(file('notice.txt'))
            version = "2.0"
            group = 'org.acme'
        """

        when:
        def result = gradleRunner('generatePom').build()

        then:
        result.task(":generatePom").outcome == TaskOutcome.SUCCESS
        file("build/distributions/hello-world-plugin-2.0.pom").exists()
        assertXmlEquals(file("build/distributions/hello-world-plugin-2.0.pom").text, """
            <project xmlns="http://maven.apache.org/POM/4.0.0" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
              <!-- This module was also published with a richer model, Gradle metadata,  -->
              <!-- which should be used instead. Do not delete the following line which  -->
              <!-- is to indicate to Gradle or any Gradle module metadata file consumer  -->
              <!-- that they should prefer consuming it instead. -->
              <!-- do_not_remove: published-with-gradle-metadata -->
              <modelVersion>4.0.0</modelVersion>
              <groupId>org.acme</groupId>
              <artifactId>hello-world-plugin</artifactId>
              <version>2.0</version>
              <name>hello-world</name>
              <description>custom project description</description>
              <url>unknown</url>
              <scm>
                <url>unknown</url>
              </scm>
              <inceptionYear>2009</inceptionYear>
              <licenses>
                <license>
                  <name>Elastic License 2.0</name>
                  <url>https://raw.githubusercontent.com/elastic/elasticsearch/v2.0/licenses/ELASTIC-LICENSE-2.0.txt</url>
                  <distribution>repo</distribution>
                </license>
                <license>
                  <name>GNU Affero General Public License Version 3</name>
                  <url>https://raw.githubusercontent.com/elastic/elasticsearch/v2.0/licenses/AGPL-3.0+SSPL-1.0+ELASTIC-LICENSE-2.0.txt</url>
                  <distribution>repo</distribution>
                </license>
                <license>
                  <name>Server Side Public License, v 1</name>
                  <url>https://www.mongodb.com/licensing/server-side-public-license</url>
                  <distribution>repo</distribution>
                </license>
              </licenses>
              <developers>
                <developer>
                  <name>Elastic</name>
                  <url>https://www.elastic.co</url>
                </developer>
              </developers>
            </project>"""
        )
    }

    def "generated pom can be validated"() {
        given:
        // scm info only added for internal builds
        internalBuild()
        buildFile << """
            buildParams.getGitOriginProperty().set("https://some-repo.com/repo.git")
            apply plugin:'elasticsearch.java'
            apply plugin:'elasticsearch.publish'

            version = "1.0"
            group = 'org.acme'
            description = "just a test project"

            ext.projectLicenses.set(['The Apache Software License, Version 2.0': 'http://www.apache.org/licenses/LICENSE-2.0'])
        """

        when:
        def result = gradleRunner('generatePom', 'validateElasticPom').build()

        then:
        result.task(":generatePom").outcome == TaskOutcome.SUCCESS
        file("build/distributions/hello-world-1.0.pom").exists()
        assertXmlEquals(file("build/distributions/hello-world-1.0.pom").text, """
            <project xmlns="http://maven.apache.org/POM/4.0.0" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
          <!-- This module was also published with a richer model, Gradle metadata,  -->
          <!-- which should be used instead. Do not delete the following line which  -->
          <!-- is to indicate to Gradle or any Gradle module metadata file consumer  -->
          <!-- that they should prefer consuming it instead. -->
          <!-- do_not_remove: published-with-gradle-metadata -->
          <modelVersion>4.0.0</modelVersion>
          <groupId>org.acme</groupId>
          <artifactId>hello-world</artifactId>
          <version>1.0</version>
          <name>hello-world</name>
          <description>just a test project</description>
          <url>unknown</url>
          <scm>
            <url>unknown</url>
          </scm>
          <inceptionYear>2009</inceptionYear>
          <licenses>
            <license>
              <name>The Apache Software License, Version 2.0</name>
              <url>http://www.apache.org/licenses/LICENSE-2.0</url>
              <distribution>repo</distribution>
            </license>
          </licenses>
          <developers>
            <developer>
              <name>Elastic</name>
              <url>https://www.elastic.co</url>
            </developer>
          </developers>
        </project>"""
        )
    }

    private boolean assertXmlEquals(String toTest, String expected) {
        def diff = DiffBuilder.compare(Input.fromString(expected))
                .ignoreWhitespace()
                .ignoreComments()
                .normalizeWhitespace()
                .withTest(Input.fromString(toTest))
                .build()
        diff.differences.each { difference ->
            println difference
        }
        if(diff.differences.size() > 0) {
            println """ given:
$toTest
"""
            println """ expected:
$expected
"""


        }
        assert diff.hasDifferences() == false
        true
    }
}

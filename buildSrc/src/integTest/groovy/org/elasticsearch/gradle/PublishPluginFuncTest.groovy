/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome
import org.xmlunit.builder.DiffBuilder
import org.xmlunit.builder.Input

class PublishPluginFuncTest extends AbstractGradleFuncTest {

    def "published pom takes es project description into account"() {
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

        def generatedPomFile = file("build/distributions/hello-world-1.0.pom")
        generatedPomFile.exists()
        assertXmlEquals(generatedPomFile.text, """
            <project xmlns="http://maven.apache.org/POM/4.0.0" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
              <modelVersion>4.0.0</modelVersion>
              <groupId>org.acme</groupId>
              <artifactId>hello-world</artifactId>
              <version>1.0</version>
              <name>hello-world</name>
              <description>custom project description</description>
            </project>"""
        )
    }

    def "generates pom for shadowed elasticsearch plugin"() {
        given:
        file('license.txt') << "License file"
        file('notice.txt') << "Notice file"
        buildFile << """
            plugins {
                id 'elasticsearch.esplugin'
                id 'elasticsearch.publish'
                id 'com.github.johnrengelman.shadow'
            }
            
            esplugin {
                name = 'hello-world-plugin'
                classname 'org.acme.HelloWorldPlugin'
                description = "custom project description"
            }
                    
            // requires elasticsearch artifact available
            tasks.named('bundlePlugin').configure { enabled = false }
            licenseFile = file('license.txt')
            noticeFile = file('notice.txt')
            version = "1.0"
            group = 'org.acme'        
        """

        when:
        def result = gradleRunner('generatePom').build()

        then:
        result.task(":generatePom").outcome == TaskOutcome.SUCCESS

        def generatedPomFile = file("build/distributions/hello-world-plugin-1.0.pom")
        generatedPomFile.exists()
        assertXmlEquals(generatedPomFile.text, """
            <project xmlns="http://maven.apache.org/POM/4.0.0" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
              <modelVersion>4.0.0</modelVersion>
              <groupId>org.acme</groupId>
              <artifactId>hello-world-plugin</artifactId>
              <version>1.0</version>
              <name>hello-world</name>
              <description>custom project description</description>
              <dependencies/>
            </project>"""
        )
    }

    def "generates pom for elasticsearch plugin"() {
        given:
        file('license.txt') << "License file"
        file('notice.txt') << "Notice file"
        buildFile << """
            plugins {
                id 'elasticsearch.esplugin'
                id 'elasticsearch.publish'
            }
            
            esplugin {
                name = 'hello-world-plugin'
                classname 'org.acme.HelloWorldPlugin'
                description = "custom project description"
            }
//            
//              // this is currently required to have validation passed
//              // In our elasticsearch build this is currently setup in the 
//              // root build.gradle file.
//              plugins.withType(MavenPublishPlugin) {
//                publishing {
//                  publications {
//                    // add license information to generated poms
//                    all {
//                      pom.withXml { XmlProvider xml ->
//                        Node node = xml.asNode()
//                        node.appendNode('inceptionYear', '2009')
//            
//                        Node license = node.appendNode('licenses').appendNode('license')
//                        license.appendNode('name', "The Apache Software License, Version 2.0")
//                        license.appendNode('url', "http://www.apache.org/licenses/LICENSE-2.0.txt")
//                        license.appendNode('distribution', 'repo')
//            
//                        Node developer = node.appendNode('developers').appendNode('developer')
//                        developer.appendNode('name', 'Elastic')
//                        developer.appendNode('url', 'https://www.elastic.co')
//                      }
//                    }
//                  }
//                }
//              }
            
            // requires elasticsearch artifact available
            tasks.named('bundlePlugin').configure { enabled = false }
            licenseFile = file('license.txt')
            noticeFile = file('notice.txt')
            version = "1.0"
            group = 'org.acme'        
        """

        when:
        def result = gradleRunner('generatePom').build()

        then:
        result.task(":generatePom").outcome == TaskOutcome.SUCCESS
        def generatedPomFile = file("build/distributions/hello-world-plugin-1.0.pom")
        generatedPomFile.exists()
        assertXmlEquals(generatedPomFile.text, """
            <project xmlns="http://maven.apache.org/POM/4.0.0" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
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
              <description>custom project description</description>
            </project>"""
        )
    }

    def "generated pom can be tweaked and validated"() {
        given:
        // scm info only added for internal builds
        internalBuild()
        buildFile << """
            BuildParams.init { it.setGitOrigin("https://some-repo.com/repo.git") }

            apply plugin:'elasticsearch.java'
            apply plugin:'elasticsearch.publish'

            version = "1.0"
            group = 'org.acme'        
            description = "just a test project"
            // this is currently required to have validation passed
            // In our elasticsearch build this is currently setup in the 
            // root build.gradle file.
            plugins.withType(MavenPublishPlugin) {
                publishing {
                    publications {
                        // add license information to generated poms
                        all {
                            pom.withXml { XmlProvider xml ->
                                Node node = xml.asNode()
                                node.appendNode('inceptionYear', '2009')
                
                                Node license = node.appendNode('licenses').appendNode('license')
                                license.appendNode('name', "The Apache Software License, Version 2.0")
                                license.appendNode('url', "http://www.apache.org/licenses/LICENSE-2.0.txt")
                                license.appendNode('distribution', 'repo')
              
                                Node developer = node.appendNode('developers').appendNode('developer')
                                developer.appendNode('name', 'Elastic')
                                developer.appendNode('url', 'https://www.elastic.co')
                            }
                        }
                    }
                }
            }
        """

        when:
        def result = gradleRunner('generatePom', 'validateNebulaPom').build()

        then:
        result.task(":generatePom").outcome == TaskOutcome.SUCCESS
        def generatedPomFile = file("build/distributions/hello-world-1.0.pom")
        println generatedPomFile.text
        generatedPomFile.exists()
        assertXmlEquals(generatedPomFile.text, """
            <project xmlns="http://maven.apache.org/POM/4.0.0" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
              <modelVersion>4.0.0</modelVersion>
              <groupId>org.acme</groupId>
              <artifactId>hello-world</artifactId>
              <version>1.0</version>
              <name>hello-world</name>
              <description>just a test project</description>
              <url>https://some-repo.com/repo.git</url>
              <scm>
                <url>https://some-repo.com/repo.git</url>
              </scm>
              <inceptionYear>2009</inceptionYear>
              <licenses>
                <license>
                  <name>The Apache Software License, Version 2.0</name>
                  <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
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
        assert diff.hasDifferences() == false
        true
    }
}

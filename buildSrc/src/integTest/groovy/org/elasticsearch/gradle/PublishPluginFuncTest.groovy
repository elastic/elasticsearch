/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome
import org.xmlunit.builder.DiffBuilder
import org.xmlunit.builder.Input

class PublishPluginFuncTest extends AbstractGradleFuncTest {

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
            <project xmlns="http://maven.apache.org/POM/4.0.0" 
                     xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd" 
                     xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
              <modelVersion>4.0.0</modelVersion>
              <groupId>org.acme</groupId>
              <artifactId>hello-world</artifactId>
              <version>1.0</version>
              <name>hello-world</name>
              <description>custom project description</description>
            </project>"""
        )
    }

    def "generates artifacts for shadowed elasticsearch plugin"() {
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
            
            publishing {
                 repositories {
                    maven {
                        url = "\$buildDir/repo"
                    }
                 }
            }
                    
            // requires elasticsearch artifact available
            tasks.named('bundlePlugin').configure { enabled = false }
            licenseFile = file('license.txt')
            noticeFile = file('notice.txt')
            version = "1.0"
            group = 'org.acme'        
        """

        when:
        def result = gradleRunner('assemble', '--stacktrace').build()

        then:
        result.task(":generatePom").outcome == TaskOutcome.SUCCESS
        file("build/distributions/hello-world-plugin-1.0-original.jar").exists()
        file("build/distributions/hello-world-plugin-1.0.jar").exists()
        file("build/distributions/hello-world-plugin-1.0-javadoc.jar").exists()
        file("build/distributions/hello-world-plugin-1.0-sources.jar").exists()
        file("build/distributions/hello-world-plugin-1.0.pom").exists()
        assertXmlEquals(file("build/distributions/hello-world-plugin-1.0.pom").text, """
            <project xmlns="http://maven.apache.org/POM/4.0.0" 
                     xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd" 
                     xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
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
        file("build/distributions/hello-world-plugin-1.0.pom").exists()
        assertXmlEquals(file("build/distributions/hello-world-plugin-1.0.pom").text, """
            <project xmlns="http://maven.apache.org/POM/4.0.0" 
                     xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd" 
                     xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
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
        def result = gradleRunner('generatePom', 'validatElasticPom').build()

        then:
        result.task(":generatePom").outcome == TaskOutcome.SUCCESS
        file("build/distributions/hello-world-1.0.pom").exists()
        assertXmlEquals(file("build/distributions/hello-world-1.0.pom").text, """
            <project xmlns="http://maven.apache.org/POM/4.0.0" 
                     xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd" 
                     xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
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

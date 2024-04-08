/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.dra

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.elasticsearch.gradle.fixtures.LocalRepositoryFixture
import org.elasticsearch.gradle.fixtures.WiremockFixture
import org.gradle.testkit.runner.TaskOutcome
import org.junit.ClassRule
import spock.lang.Shared

class DraResolvePluginFuncTest extends AbstractGradleFuncTest {

    @Shared
    @ClassRule
    public LocalRepositoryFixture repository = new LocalRepositoryFixture()

    def setup() {
        configurationCacheCompatible = false

        buildFile << """
        plugins {
            id 'elasticsearch.dra-artifacts'
        }
        
        repositories.all {
            // for supporting http testing repos here
            allowInsecureProtocol = true
        }
        """
    }

    def "provides flag indicating dra usage"() {
        setup:
        repository.generateJar("org.acme", "ml-cpp", "8.6.0-SNAPSHOT")
        buildFile << """
        if(useDra == false) {
            repositories {
              maven {
                name = "local-test"
                url = "${repository.getRepoDir().toURI()}"
                metadataSources {
                  artifact()
                }
              }
            }  
        }
        """

        buildFile << """
        configurations {
            someConfig
        }
        
        dependencies {
            someConfig "org.acme:ml-cpp:8.6.0-SNAPSHOT"
        }
        
        tasks.register('resolveArtifacts') {
            doLast {
                configurations.someConfig.files.each { println it }
            }
        }
        """

        when:
        def result = gradleRunner("resolveArtifacts").build()

        then:
        result.task(":resolveArtifacts").outcome == TaskOutcome.SUCCESS

        when:
        result = gradleRunner("resolveArtifacts", "-Ddra.artifacts=true", "-Ddra.workflow=SNAPSHOT").buildAndFail()

        then:
        result.task(":resolveArtifacts").outcome == TaskOutcome.FAILED
        result.output.contains("Cannot resolve external dependency org.acme:ml-cpp:8.6.0-SNAPSHOT because no repositories are defined.")
    }

    def "configures repositories to resolve #draKey like dra #workflow artifacts"() {
        setup:
        repository.generateJar("some.group", "bar", "1.0.0")
        repository.generateJar("some.group", "baz", "1.0.0-SNAPSHOT")
        repository.configureBuild(buildFile)
        buildFile << """
        configurations {
            someConfig
        }
        
        dependencies {
            someConfig "some.group:bar:1.0.0"
            someConfig "some.group:baz:1.0.0-SNAPSHOT"
            someConfig "org.acme:$draArtifact:$draVersion:deps@zip"
        }
        
        tasks.register('resolveArtifacts') {
            doLast {
                configurations.someConfig.files.each { println it }
            }
        }
        """

        when:
        def result = WiremockFixture.withWireMock(expectedRequest, "content".getBytes('UTF-8')) { server ->
            gradleRunner("resolveArtifacts",
                    '-Ddra.artifacts=true',
                    "-Ddra.workflow=$workflow",
                    "-Ddra.artifacts.dependency.${draKey}=$buildId",
                    "-Ddra.artifacts.url.repo.prefix=${server.baseUrl()}").build()
        }

        then:
        result.task(":resolveArtifacts").outcome == TaskOutcome.SUCCESS

        where:
        workflow   | buildId          | draVersion       | draKey   | draArtifact  | expectedRequest
        "snapshot" | '8.6.0-f633b1d7' | "8.6.0-SNAPSHOT" | "ml-cpp" | "ml-cpp"     | "/$draKey/${buildId}/downloads/$draArtifact/${draArtifact}-${draVersion}-deps.zip"
        "staging"  | '8.6.0-f633b1d7' | "8.6.0"          | "ml-cpp" | "ml-cpp"     | "/$draKey/${buildId}/downloads/$draArtifact/${draArtifact}-${draVersion}-deps.zip"
        "release"  | '8.6.0-f633b1d7' | "8.6.0"          | "ml-cpp" | "ml-cpp"     | "/$draKey/${buildId}/downloads/$draArtifact/${draArtifact}-${draVersion}-deps.zip"
        "snapshot" | '8.6.0-f633b1d7' | "8.6.0-SNAPSHOT" | "beats"  | "metricbeat" | "/$draKey/${buildId}/downloads/$draKey/$draArtifact/${draArtifact}-${draVersion}-deps.zip"
        "staging"  | '8.6.0-f633b1d7' | "8.6.0"          | "beats"  | "metricbeat" | "/$draKey/${buildId}/downloads/$draKey/$draArtifact/${draArtifact}-${draVersion}-deps.zip"
        "release"  | '8.6.0-f633b1d7' | "8.6.0"          | "beats"  | "metricbeat" | "/$draKey/${buildId}/downloads/$draKey/$draArtifact/${draArtifact}-${draVersion}-deps.zip"
    }
}

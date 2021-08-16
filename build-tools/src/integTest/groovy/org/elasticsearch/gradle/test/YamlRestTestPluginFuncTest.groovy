/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.test

import org.elasticsearch.gradle.VersionProperties
import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome

class YamlRestTestPluginFuncTest extends AbstractGradleFuncTest {

    def "declares default dependencies"() {
        given:
        buildFile << """
        plugins {
          id 'elasticsearch.yaml-rest-test'
        }
        """

        when:
        def result = gradleRunner("dependencies").build()

        then:
        result.output.contains("""
restTestSpecs
\\--- org.elasticsearch:rest-api-spec:${VersionProperties.elasticsearch} FAILED
""")
        result.output.contains("""
yamlRestTestImplementation - Implementation only dependencies for source set 'yaml rest test'. (n)
\\--- org.elasticsearch.test:framework:8.0.0-SNAPSHOT (n)
""")

    }

    def "yamlRestTest does nothing when there are no tests"() {
        given:
        buildFile << """
        plugins {
          id 'elasticsearch.yaml-rest-test'
        }
        
        repositories {
            mavenCentral()
        }
   
        dependencies {
            yamlRestTestImplementation "org.elasticsearch.test:framework:7.14.0"
            restTestSpecs "org.elasticsearch:rest-api-spec:7.14.0"
        }
        """

        when:
        def result = gradleRunner("yamlRestTest").build()
        then:
        result.task(':yamlRestTest').outcome == TaskOutcome.NO_SOURCE
        result.task(':compileYamlRestTestJava').outcome == TaskOutcome.NO_SOURCE
        result.task(':processYamlRestTestResources').outcome == TaskOutcome.NO_SOURCE
    }

}
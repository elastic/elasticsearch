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

class JavaRestTestPluginFuncTest extends AbstractGradleFuncTest {

    def setup() {
        // underlaying TestClusterPlugin and StandaloneRestIntegTestTask are not cc compatible
        configurationCacheCompatible = false
    }

    def "declares default dependencies"() {
        given:
        buildFile << """
        plugins {
          id 'elasticsearch.java-rest-test'
        }
        """

        when:
        def result = gradleRunner("dependencies").build()
        def output = normalized(result.output)
        then:
        output.contains(normalized("""
javaRestTestImplementation - Implementation only dependencies for source set 'java rest test'. (n)
/--- org.elasticsearch.test:framework:${VersionProperties.elasticsearch} (n)"""))
    }

    def "javaRestTest does nothing when there are no tests"() {
        given:
        buildFile << """
        plugins {
          id 'elasticsearch.java-rest-test'
        }

        repositories {
            mavenCentral()
        }

        dependencies {
            javaRestTestImplementation "org.elasticsearch.test:framework:7.14.0"
        }
        """

        when:
        def result = gradleRunner("javaRestTest").build()
        then:
        result.task(':compileJavaRestTestJava').outcome == TaskOutcome.NO_SOURCE
        result.task(':javaRestTest').outcome == TaskOutcome.NO_SOURCE
    }

}

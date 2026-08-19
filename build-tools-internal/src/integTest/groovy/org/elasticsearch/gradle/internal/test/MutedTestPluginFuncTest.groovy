/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test

import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.gradle.api.Plugin
import org.gradle.testkit.runner.TaskOutcome

class MutedTestPluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    Class<? extends Plugin> pluginClassUnderTest = MutedTestPlugin

    def setup() {
        buildFile << """
            apply plugin: 'java'
            repositories { mavenCentral() }
            dependencies { testImplementation 'junit:junit:4.13.1' }
            tasks.named("test").configure {
                testLogging { events "started" }
            }
        """
    }

    def "muted tests are excluded by default"() {
        given:
        muteTest("org.acme.SomeTest", "someMutedTest")
        testClazz("org.acme.SomeTest") {
            """
            @org.junit.Test public void someMutedTest() {}
            @org.junit.Test public void someUnmutedTest() {}
            """
        }

        when:
        def result = gradleRunner("test").build()

        then:
        result.task(":test").outcome == TaskOutcome.SUCCESS
        result.output.contains("someMutedTest STARTED") == false
        result.output.contains("someUnmutedTest STARTED")
    }

    def "tests.mutes.enabled=true applies mutes explicitly"() {
        given:
        muteTest("org.acme.SomeTest", "someMutedTest")
        testClazz("org.acme.SomeTest") {
            """
            @org.junit.Test public void someMutedTest() {}
            @org.junit.Test public void someUnmutedTest() {}
            """
        }

        when:
        def result = gradleRunner("test", "-Dtests.mutes.enabled=true").build()

        then:
        result.task(":test").outcome == TaskOutcome.SUCCESS
        result.output.contains("someMutedTest STARTED") == false
        result.output.contains("someUnmutedTest STARTED")
    }

    def "tests.mutes.enabled=false runs all tests including muted ones"() {
        given:
        muteTest("org.acme.SomeTest", "someMutedTest")
        testClazz("org.acme.SomeTest") {
            """
            @org.junit.Test public void someMutedTest() {}
            @org.junit.Test public void someUnmutedTest() {}
            """
        }

        when:
        def result = gradleRunner("test", "-Dtests.mutes.enabled=false").build()

        then:
        result.task(":test").outcome == TaskOutcome.SUCCESS
        result.output.contains("someMutedTest STARTED")
        result.output.contains("someUnmutedTest STARTED")
    }

    private void muteTest(String className, String method) {
        file("muted-tests.yml").text = """
tests:
- class: ${className}
  method: ${method}
  issue: https://github.com/elastic/elasticsearch/issues/1
"""
    }
}

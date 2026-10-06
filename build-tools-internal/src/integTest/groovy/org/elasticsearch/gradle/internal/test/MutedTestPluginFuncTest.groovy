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
            dependencies { testImplementation 'junit:junit:4.13.2' }
            tasks.withType(Test).configureEach {
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

    def "task scoped mutes only apply to listed tasks"() {
        given:
        addOtherTestSourceSet()
        muteTest("org.acme.ScopedTest", "someMutedTest", [":otherTest"])
        testClazz("org.acme.ScopedTest") {
            """
            @org.junit.Test public void someMutedTest() {}
            """
        }
        clazz(file("src/otherTest/java"), "org.acme.ScopedTest", null) {
            """
            @org.junit.Test public void someMutedTest() {}
            """
        }

        when:
        def mainTaskResult = gradleRunner("test").build()
        def scopedTaskResult = gradleRunner("otherTest").buildAndFail()

        then:
        mainTaskResult.task(":test").outcome == TaskOutcome.SUCCESS
        mainTaskResult.output.contains("someMutedTest STARTED")
        scopedTaskResult.output.contains("No tests found for given includes")
    }

    def "adding a scoped mute only reexecutes the affected test task"() {
        given:
        addOtherTestSourceSet()
        testClazz("org.acme.UnaffectedTest") {
            """
            @org.junit.Test public void unaffectedTest() {}
            """
        }
        clazz(file("src/otherTest/java"), "org.acme.ScopedTest", null) {
            """
            @org.junit.Test public void someMutedTest() {}
            @org.junit.Test public void someUnmutedTest() {}
            """
        }

        when:
        def initialResult = gradleRunner("test", "otherTest").build()
        muteTest("org.acme.ScopedTest", "someMutedTest", [":otherTest"])
        def secondResult = gradleRunner("test", "otherTest").build()

        then:
        initialResult.task(":test").outcome == TaskOutcome.SUCCESS
        initialResult.task(":otherTest").outcome == TaskOutcome.SUCCESS

        secondResult.task(":test").outcome == TaskOutcome.UP_TO_DATE
        secondResult.task(":otherTest").outcome == TaskOutcome.SUCCESS
        secondResult.output.contains("someMutedTest STARTED") == false
        secondResult.output.contains("someUnmutedTest STARTED")
        secondResult.output.contains("unaffectedTest STARTED") == false
    }

    def "invalid task scoped mute schema fails the build"() {
        given:
        file("muted-tests.yml").text = """
            tests:
            - class: org.acme.SomeTest
              method: someMutedTest
              tasks: []
              issue: https://github.com/elastic/elasticsearch/issues/1
        """.stripIndent()
        testClazz("org.acme.SomeTest") {
            """
            @org.junit.Test public void someMutedTest() {}
            """
        }

        when:
        def result = gradleRunner("test").buildAndFail()

        then:
        result.output.contains("muted test tasks must not be empty")
    }

    private void addOtherTestSourceSet() {
        buildFile << """
            sourceSets {
                otherTest {
                    java.srcDir 'src/otherTest/java'
                    compileClasspath += sourceSets.main.output + configurations.testRuntimeClasspath
                    runtimeClasspath += output + compileClasspath
                }
            }
            configurations {
                otherTestImplementation.extendsFrom testImplementation
                otherTestRuntimeOnly.extendsFrom testRuntimeOnly
            }
            tasks.register('otherTest', Test) {
                testClassesDirs = sourceSets.otherTest.output.classesDirs
                classpath = sourceSets.otherTest.runtimeClasspath
            }
        """
    }

    private void muteTest(String className, String method, List<String> tasks = null) {
        String tasksBlock = tasks == null ? "" : "\n  tasks:\n" + tasks.collect { "  - ${it}" }.join("\n")
        file("muted-tests.yml").text = """tests:
- class: ${className}
  method: ${method}
  issue: https://github.com/elastic/elasticsearch/issues/1${tasksBlock}
"""
    }
}

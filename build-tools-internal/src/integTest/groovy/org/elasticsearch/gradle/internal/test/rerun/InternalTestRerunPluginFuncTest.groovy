/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test.rerun

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome

class InternalTestRerunPluginFuncTest extends AbstractGradleFuncTest {

    def setup() {
        buildFile.text = """
        plugins {
            id 'elasticsearch.internal-test-rerun' apply false
        }
        """
    }

    def "runs all tests by default"() {
        given:
        simpleTestSetup()
        when:
        def result = gradleRunner("test", "--warning-mode", "all").build()
        then:
        result.task(":subproject1:test").outcome == TaskOutcome.SUCCESS
        testExecuted(result.output, "SubProject1TestClazz1 > someTest1")
        testExecuted(result.output, "SubProject1TestClazz1 > someTest2")
        testExecuted(result.output, "SubProject1TestClazz2 > someTest1")
        testExecuted(result.output, "SubProject1TestClazz2 > someTest2")

        result.task(":subproject2:test").outcome == TaskOutcome.SUCCESS
        testExecuted(result.output, "SubProject2TestClazz1 > someTest1")
        testExecuted(result.output, "SubProject2TestClazz1 > someTest2")
        testExecuted(result.output, "SubProject2TestClazz2 > someTest1")
        testExecuted(result.output, "SubProject2TestClazz2 > someTest2")
    }

    def "skips successful tasks and runs everything else"() {
        given:
        simpleTestSetup()
        writeSuccessfulTasks([":subproject1:test"])
        when:
        def result = gradleRunner("test", "--warning-mode", "all").build()
        then:
        result.task(":subproject1:test").outcome == TaskOutcome.SKIPPED
        result.output.contains("succeeded in previous run")

        result.task(":subproject2:test").outcome == TaskOutcome.SUCCESS
        testExecuted(result.output, "SubProject2TestClazz1 > someTest1")
        testExecuted(result.output, "SubProject2TestClazz1 > someTest2")
        testExecuted(result.output, "SubProject2TestClazz2 > someTest1")
        testExecuted(result.output, "SubProject2TestClazz2 > someTest2")
    }

    def "skips all tasks when all are successful"() {
        given:
        simpleTestSetup()
        writeSuccessfulTasks([":subproject1:test", ":subproject2:test"])
        when:
        def result = gradleRunner("test", "--warning-mode", "all").build()
        then:
        result.task(":subproject1:test").outcome == TaskOutcome.SKIPPED
        result.task(":subproject2:test").outcome == TaskOutcome.SKIPPED
        result.output.contains("succeeded in previous run")
    }

    def "runs all tests when no tasks are successful"() {
        given:
        simpleTestSetup()
        writeSuccessfulTasks([])
        when:
        def result = gradleRunner("test", "--warning-mode", "all").build()
        then:
        result.task(":subproject1:test").outcome == TaskOutcome.SUCCESS
        result.task(":subproject2:test").outcome == TaskOutcome.SUCCESS
        result.output.contains("not confirmed successful in previous run")
        testExecuted(result.output, "SubProject1TestClazz1 > someTest1")
        testExecuted(result.output, "SubProject2TestClazz1 > someTest1")
    }

    def "handles malformed failed-test-history gracefully"() {
        given:
        simpleTestSetup()
        file(".failed-test-history.json") << "{ invalid json"

        when:
        def result = gradleRunner("test", "--warning-mode", "all").buildAndFail()

        then:
        result.output.contains("Failed to parse .failed-test-history.json")
    }

    def "rejects oversized failed-test-history file"() {
        given:
        simpleTestSetup()
        def largeContent = '{"successfulTasks":["' + ('x'.multiply(10 * 1024 * 1024)) + '"]}'
        file(".failed-test-history.json") << largeContent

        when:
        def result = gradleRunner("test", "--warning-mode", "all").buildAndFail()

        then:
        result.output.contains("Failed test history file too large")
    }

    def "ignores unknown fields in history file"() {
        given:
        simpleTestSetup()
        file(".failed-test-history.json") << '''
{
  "successfulTasks": [":subproject1:test"],
  "workUnits": [],
  "executedTestTasks": [":subproject1:test", ":subproject2:test"],
  "unknownField": "ignored"
}'''

        when:
        def result = gradleRunner("test", "--warning-mode", "all").build()

        then:
        result.task(":subproject1:test").outcome == TaskOutcome.SKIPPED
        result.task(":subproject2:test").outcome == TaskOutcome.SUCCESS
    }

    def "non-test task paths in successfulTasks are harmless"() {
        given:
        simpleTestSetup()
        file(".failed-test-history.json") << '''
{
  "successfulTasks": [":subproject1:compileJava", ":subproject2:assemble"]
}'''

        when:
        def result = gradleRunner("test", "--warning-mode", "all").build()

        then:
        result.task(":subproject1:test").outcome == TaskOutcome.SUCCESS
        result.task(":subproject2:test").outcome == TaskOutcome.SUCCESS
    }

    boolean testExecuted(String output, String testReference) {
        output.contains(testReference + " STARTED")
    }

    boolean testNotExecuted(String output, String testReference) {
        output.contains(testReference) == false
    }

    private File writeSuccessfulTasks(List<String> tasks) {
        def tasksJson = tasks.collect { "\"$it\"" }.join(", ")
        file(".failed-test-history.json") << """
{
  "successfulTasks": [$tasksJson]
}
"""
    }

    void simpleTestSetup() {
        buildFile << """
        allprojects {
                apply plugin: 'java'
                apply plugin: 'elasticsearch.internal-test-rerun'

                repositories {
                    mavenCentral()
                }

                dependencies {
                    testImplementation 'junit:junit:4.13.1'
                }

                tasks.named("test").configure {
                    testLogging {
                        events("started", "skipped")
                    }
                }
            }
            """
        subProject(":subproject1") {
            createTest("SubProject1TestClazz1")
            createTest("SubProject1TestClazz2")
        }
        subProject(":subproject2") {
            createTest("SubProject2TestClazz1")
            createTest("SubProject2TestClazz2")
        }
    }
}

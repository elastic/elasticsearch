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

    def "filters test task execution by detected testsfailed-history file"() {
        given:
        simpleTestSetup()
        writeFailedHistoryWithExecutedTasks(
            ":subproject2:test", "org.acme.SubProject2TestClazz2", "someTest1",
            [":subproject1:test", ":subproject2:test"]
        )
        when:
        def result = gradleRunner("test", "--warning-mode", "all").build()
        then:
        result.task(":subproject1:test").outcome == TaskOutcome.SKIPPED
        result.task(":subproject2:test").outcome == TaskOutcome.SUCCESS
        testNotExecuted(result.output, "SubProject2TestClazz1 > someTest1")
        testNotExecuted(result.output, "SubProject2TestClazz1 > someTest2")
        testExecuted(result.output, "SubProject2TestClazz2 > someTest1")
        testNotExecuted(result.output, "SubProject2TestClazz2 > someTest2")
    }

    def "filters failed parameterized tests in tests-failed-history file"() {
        given:
        buildFile << """
        allprojects {
            apply plugin: 'java'
            apply plugin: 'elasticsearch.internal-test-rerun'

            repositories {
                mavenCentral()
            }

            dependencies {
                testImplementation 'junit:junit:4.13.1'
                testImplementation "com.carrotsearch.randomizedtesting:randomizedtesting-runner:2.8.2"
            }

            tasks.named("test").configure {
                testLogging {
                    events("started", "skipped")
                }
            }
        }
        """
        subProject(":subproject1") {
            createTest("SubProject1RandomizedTestClazz1")
        }
        subProject(":subproject2") {
            createTest("SubProject2RandomizedTestClazz1")
            file("src/test/java/org/acme/SubProject2RandomizedTestClazz2.java") <<'''
            package org.acme;

            import java.util.Arrays;
import java.util.Formatter;

import org.junit.Test;
import org.junit.runners.Parameterized;

import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.Repeat;
import com.carrotsearch.randomizedtesting.annotations.Seed;
import com.carrotsearch.randomizedtesting.annotations.Seeds;

public class SubProject2RandomizedTestClazz2 extends RandomizedTest {
  private String yaml;

  public SubProject2RandomizedTestClazz2(
      @Name("yaml") String yaml) {
    this.yaml = yaml;
  }

  @Test
  public void test() {
    System.out.println(yaml + " "
        + getContext().getRandomness());
  }

  @ParametersFactory
  public static Iterable<Object[]> parameters() {
    return Arrays.asList($$(
        $("analysis-common/30_tokenizers/letter"),
        $("analysis-common/40_tokenizers/letter")));
  }
}
        '''
        }
        writeFailedHistoryWithExecutedTasks(
            ":subproject2:test", "org.acme.SubProject2RandomizedTestClazz2", "test {yaml=analysis-common/30_tokenizers/letter}",
            [":subproject1:test", ":subproject2:test"]
        )

        when:
        def result = gradleRunner("test", "--warning-mode", "all").build()
        then:
        result.task(":subproject1:test").outcome == TaskOutcome.SKIPPED
        result.task(":subproject2:test").outcome == TaskOutcome.SUCCESS
        testNotExecuted(result.output, "SubProject2TestClazz1 > someTest1")
        testExecuted(result.output, "SubProject2RandomizedTestClazz2 > test > test {yaml=analysis-common/30_tokenizers/letter}")
        testNotExecuted(result.output, "SubProject2RandomizedTestClazz2 > test > test {yaml=analysis-common/40_tokenizers/letter}")
    }



    private File writeFailedHistory(String taskPath, String testClazz, String testName) {
        file(".failed-test-history.json") << """
{
  "workUnits": [
    {
      "name": "$taskPath",
      "outcome": "failed",
      "tests": [
        {
          "name": "$testClazz",
          "outcome": {
            "overall": "failed",
            "own": "passed",
            "children": "failed"
          },
          "executions": [
            {
              "outcome": {
                "overall": "failed",
                "own": "passed",
                "children": "failed"
              }
            }
          ],
          "children": [
            {
              "name": "$testName",
              "outcome": {
                "overall": "failed"
              },
              "executions": [
                {
                  "outcome": {
                    "overall": "failed"
                  }
                }
              ],
              "children": []
            }
          ]
        }
      ]
    }
  ]
}
"""
    }

    private File writeFailedHistoryWithExecutedTasks(String taskPath, String testClazz, String testName, List<String> executedTasks) {
        def executedTasksJson = executedTasks.collect { "\"$it\"" }.join(", ")
        file(".failed-test-history.json") << """
{
  "workUnits": [
    {
      "name": "$taskPath",
      "outcome": "failed",
      "tests": [
        {
          "name": "$testClazz",
          "outcome": {
            "overall": "failed",
            "own": "passed",
            "children": "failed"
          },
          "executions": [
            {
              "outcome": {
                "overall": "failed",
                "own": "passed",
                "children": "failed"
              }
            }
          ],
          "children": [
            {
              "name": "$testName",
              "outcome": {
                "overall": "failed"
              },
              "executions": [
                {
                  "outcome": {
                    "overall": "failed"
                  }
                }
              ],
              "children": []
            }
          ]
        }
      ]
    }
  ],
  "executedTestTasks": [$executedTasksJson]
}
"""
    }

    private File writeFailedHistoryWithExecutedAndFailedTasks(List<String> executedTasks, List<String> failedTasks) {
        def executedJson = executedTasks.collect { "\"$it\"" }.join(", ")
        def failedJson = failedTasks.collect { "\"$it\"" }.join(", ")
        file(".failed-test-history.json") << """
{
  "workUnits": [],
  "executedTestTasks": [$executedJson],
  "failedTestTasks": [$failedJson]
}
"""
    }

    boolean testExecuted(String output, String testReference) {
        output.contains(testReference + " STARTED")
    }

    boolean testSkipped(String output, String testReference) {
        output.contains(testReference + " SKIPPED")
    }

    boolean testNotExecuted(String output, String testReference) {
        output.contains(testReference) == false
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
        // Create file > 10MB
        def largeContent = '{"workUnits":[' + ('{"name":"test","tests":[]},'.multiply(500000)) + '{}]}'
        file(".failed-test-history.json") << largeContent

        when:
        def result = gradleRunner("test", "--warning-mode", "all").buildAndFail()

        then:
        result.output.contains("Failed test history file too large")
    }

    def "skips tests with null names in history"() {
        given:
        simpleTestSetup()
        file(".failed-test-history.json") << '''
{
  "workUnits": [{
    "name": ":subproject1:test",
    "outcome": "failed",
    "tests": [{
      "name": null,
      "children": []
    }]
  }]
}'''

        when:
        def result = gradleRunner("test", "--warning-mode", "all").build()

        then:
        // Task runs but skips null-named test
        result.task(":subproject1:test").outcome == TaskOutcome.SUCCESS
        result.output.contains("Skipping test class with null name")
    }

    def "handles empty workUnits array"() {
        given:
        simpleTestSetup()
        file(".failed-test-history.json") << '{"workUnits":[]}'

        when:
        def result = gradleRunner("test", "--warning-mode", "all").build()

        then:
        // Should skip all tests since no failures recorded and no executedTestTasks data (null fallback runs all)
        // With null executedTestTasks, wasTaskExecuted returns false, so tasks run all tests
        result.task(":subproject1:test").outcome == TaskOutcome.SUCCESS
        result.task(":subproject2:test").outcome == TaskOutcome.SUCCESS
    }

    def "skips confirmed-passed tasks when executedTestTasks is present"() {
        given:
        simpleTestSetup()
        writeFailedHistoryWithExecutedTasks(
            ":subproject2:test", "org.acme.SubProject2TestClazz2", "someTest1",
            [":subproject1:test", ":subproject2:test"]
        )

        when:
        def result = gradleRunner("test", "--warning-mode", "all").build()

        then:
        // subproject1:test was executed but had no failures — confirmed passed, skip it
        result.task(":subproject1:test").outcome == TaskOutcome.SKIPPED
        result.output.contains("confirmed passed in previous run")

        // subproject2:test had failures — rerun only failed tests
        result.task(":subproject2:test").outcome == TaskOutcome.SUCCESS
        testExecuted(result.output, "SubProject2TestClazz2 > someTest1")
        testNotExecuted(result.output, "SubProject2TestClazz2 > someTest2")
        testNotExecuted(result.output, "SubProject2TestClazz1 > someTest1")
    }

    def "runs all tests for tasks not in executedTestTasks"() {
        given:
        simpleTestSetup()
        // Only subproject2:test was executed in previous run, subproject1:test was never executed
        writeFailedHistoryWithExecutedTasks(
            ":subproject2:test", "org.acme.SubProject2TestClazz2", "someTest1",
            [":subproject2:test"]
        )

        when:
        def result = gradleRunner("test", "--warning-mode", "all").build()

        then:
        // subproject1:test was NOT in executedTestTasks — never executed, run all tests
        result.task(":subproject1:test").outcome == TaskOutcome.SUCCESS
        result.output.contains("not executed in previous run")
        testExecuted(result.output, "SubProject1TestClazz1 > someTest1")
        testExecuted(result.output, "SubProject1TestClazz1 > someTest2")
        testExecuted(result.output, "SubProject1TestClazz2 > someTest1")
        testExecuted(result.output, "SubProject1TestClazz2 > someTest2")

        // subproject2:test had failures — rerun only failed tests
        result.task(":subproject2:test").outcome == TaskOutcome.SUCCESS
        testExecuted(result.output, "SubProject2TestClazz2 > someTest1")
        testNotExecuted(result.output, "SubProject2TestClazz2 > someTest2")
    }

    def "falls back to run all tests when executedTestTasks is null"() {
        given:
        simpleTestSetup()
        // Write history without executedTestTasks (backward compatible JSON)
        writeFailedHistory(":subproject2:test", "org.acme.SubProject2TestClazz2", "someTest1")

        when:
        def result = gradleRunner("test", "--warning-mode", "all").build()

        then:
        // subproject1:test has no failures and executedTestTasks is null — safe fallback, run all tests
        result.task(":subproject1:test").outcome == TaskOutcome.SUCCESS
        result.output.contains("not executed in previous run")
        testExecuted(result.output, "SubProject1TestClazz1 > someTest1")
        testExecuted(result.output, "SubProject1TestClazz1 > someTest2")

        // subproject2:test had failures — rerun only failed tests
        result.task(":subproject2:test").outcome == TaskOutcome.SUCCESS
        testExecuted(result.output, "SubProject2TestClazz2 > someTest1")
        testNotExecuted(result.output, "SubProject2TestClazz2 > someTest2")
    }

    def "runs all tests for tasks in failedTestTasks (State 2)"() {
        given:
        simpleTestSetup()
        // subproject1:test failed at the Gradle level (e.g. resource leak) with no individual
        // test failures; subproject2:test was a confirmed pass.
        writeFailedHistoryWithExecutedAndFailedTasks(
            [":subproject1:test", ":subproject2:test"],
            [":subproject1:test"]
        )

        when:
        def result = gradleRunner("test", "--warning-mode", "all").build()

        then:
        // subproject1:test is in failedTestTasks — run all tests to reproduce the leak
        result.task(":subproject1:test").outcome == TaskOutcome.SUCCESS
        result.output.contains("task failed without test failures in previous run")
        testExecuted(result.output, "SubProject1TestClazz1 > someTest1")
        testExecuted(result.output, "SubProject1TestClazz1 > someTest2")
        testExecuted(result.output, "SubProject1TestClazz2 > someTest1")
        testExecuted(result.output, "SubProject1TestClazz2 > someTest2")

        // subproject2:test was executed and passed — confirmed passed, skip it
        result.task(":subproject2:test").outcome == TaskOutcome.SKIPPED
        result.output.contains("confirmed passed in previous run")
    }

    def "State 1 wins when task is in both workUnits and failedTestTasks"() {
        given:
        simpleTestSetup()
        // subproject2:test has both a recorded test failure AND a Gradle-level failure.
        // State 1 (rerun failed tests only) must take precedence over State 2.
        file(".failed-test-history.json") << """
{
  "workUnits": [
    {
      "name": ":subproject2:test",
      "outcome": "failed",
      "tests": [
        {
          "name": "org.acme.SubProject2TestClazz2",
          "outcome": { "overall": "failed", "own": "passed", "children": "failed" },
          "executions": [ { "outcome": { "overall": "failed", "own": "passed", "children": "failed" } } ],
          "children": [
            {
              "name": "someTest1",
              "outcome": { "overall": "failed" },
              "executions": [ { "outcome": { "overall": "failed" } } ],
              "children": []
            }
          ]
        }
      ]
    }
  ],
  "executedTestTasks": [":subproject1:test", ":subproject2:test"],
  "failedTestTasks": [":subproject2:test"]
}
"""

        when:
        def result = gradleRunner("test", "--warning-mode", "all").build()

        then:
        // subproject2:test wins via State 1 — filter to failed tests only
        result.task(":subproject2:test").outcome == TaskOutcome.SUCCESS
        result.output.contains("filtering to 1 failed test classes")
        testExecuted(result.output, "SubProject2TestClazz2 > someTest1")
        testNotExecuted(result.output, "SubProject2TestClazz2 > someTest2")
        testNotExecuted(result.output, "SubProject2TestClazz1 > someTest1")

        // Must NOT log the State 2 message for this task
        result.output.contains("task failed without test failures") == false
    }

    def "handles empty failedTestTasks array without regressing existing states"() {
        given:
        simpleTestSetup()
        // failedTestTasks is present but empty — must not affect State 3 (skip passed).
        file(".failed-test-history.json") << """
{
  "workUnits": [],
  "executedTestTasks": [":subproject1:test", ":subproject2:test"],
  "failedTestTasks": []
}
"""

        when:
        def result = gradleRunner("test", "--warning-mode", "all").build()

        then:
        // Both tasks were executed and passed — confirmed passed, skip them
        result.task(":subproject1:test").outcome == TaskOutcome.SKIPPED
        result.task(":subproject2:test").outcome == TaskOutcome.SKIPPED
        result.output.contains("confirmed passed in previous run")
    }

    def "ignores non-test task paths in failedTestTasks"() {
        given:
        simpleTestSetup()
        // failedTestTasks contains a non-test path (e.g. compileJava from the fallback branch
        // in smart-retry.sh when executedTestTasks is unknown). The plugin only consults
        // failedTestTasks per Test task, so non-test entries must be inert.
        file(".failed-test-history.json") << """
{
  "workUnits": [],
  "executedTestTasks": [":subproject1:test", ":subproject2:test"],
  "failedTestTasks": [":subproject1:compileJava", ":subproject2:assemble"]
}
"""

        when:
        def result = gradleRunner("test", "--warning-mode", "all").build()

        then:
        // No test task is in failedTestTasks; both are in executedTestTasks with no failures
        // — State 3 (confirmed passed).
        result.task(":subproject1:test").outcome == TaskOutcome.SKIPPED
        result.task(":subproject2:test").outcome == TaskOutcome.SKIPPED
        result.output.contains("task failed without test failures") == false
    }

    def "handles empty executedTestTasks array"() {
        given:
        simpleTestSetup()
        // executedTestTasks is present but empty — all tasks are "never executed"
        writeFailedHistoryWithExecutedTasks(
            ":subproject2:test", "org.acme.SubProject2TestClazz2", "someTest1",
            []
        )

        when:
        def result = gradleRunner("test", "--warning-mode", "all").build()

        then:
        // Both tasks should run all tests since none are in executedTestTasks
        result.task(":subproject1:test").outcome == TaskOutcome.SUCCESS
        testExecuted(result.output, "SubProject1TestClazz1 > someTest1")

        // subproject2:test is in workUnits (has failures) so it still filters
        result.task(":subproject2:test").outcome == TaskOutcome.SUCCESS
        testExecuted(result.output, "SubProject2TestClazz2 > someTest1")
        testNotExecuted(result.output, "SubProject2TestClazz2 > someTest2")
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

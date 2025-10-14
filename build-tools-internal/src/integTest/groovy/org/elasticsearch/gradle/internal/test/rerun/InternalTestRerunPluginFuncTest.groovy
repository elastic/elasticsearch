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
        writeFailedHistory(":subproject2:test", "org.acme.SubProject2TestClazz2", "someTest1")
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
        writeFailedHistory(":subproject2:test", "org.acme.SubProject2RandomizedTestClazz2", "test {yaml=analysis-common/30_tokenizers/letter}")

        when:
        def result = gradleRunner("test", "--warning-mode", "all").build()
        then:
        result.task(":subproject1:test").outcome == TaskOutcome.SKIPPED
        result.task(":subproject2:test").outcome == TaskOutcome.SUCCESS
        testNotExecuted(result.output, "SubProject2TestClazz1 > someTest1")
        testExecuted(result.output, "SubProject2RandomizedTestClazz2 > test {yaml=analysis-common/30_tokenizers/letter}")
        testNotExecuted(result.output, "SubProject2RandomizedTestClazz2 > test {yaml=analysis-common/40_tokenizers/letter}")
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

    boolean testExecuted(String output, String testReference) {
        output.contains(testReference + " STARTED")
    }

    boolean testSkipped(String output, String testReference) {
        output.contains(testReference + " SKIPPED")
    }

    boolean testNotExecuted(String output, String testReference) {
        output.contains(testReference) == false
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

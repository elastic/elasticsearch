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
        writeHistory([":subproject1:test"], [:])
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
        writeHistory([":subproject1:test", ":subproject2:test"], [:])
        when:
        def result = gradleRunner("test", "--warning-mode", "all").build()
        then:
        result.task(":subproject1:test").outcome == TaskOutcome.SKIPPED
        result.task(":subproject2:test").outcome == TaskOutcome.SKIPPED
        result.output.contains("succeeded in previous run")
    }

    def "runs all tests when no tasks are successful and no tests to exclude"() {
        given:
        simpleTestSetup()
        writeHistory([], [:])
        when:
        def result = gradleRunner("test", "--warning-mode", "all").build()
        then:
        result.task(":subproject1:test").outcome == TaskOutcome.SUCCESS
        result.task(":subproject2:test").outcome == TaskOutcome.SUCCESS
        result.output.contains("not confirmed successful in previous run")
        testExecuted(result.output, "SubProject1TestClazz1 > someTest1")
        testExecuted(result.output, "SubProject2TestClazz1 > someTest1")
    }

    def "excludes individual successful tests from partially-failed tasks"() {
        given:
        simpleTestSetup()
        writeHistory(
            [":subproject1:test"],
            [":subproject2:test": ["org.acme.SubProject2TestClazz1#someTest1", "org.acme.SubProject2TestClazz1#someTest2"]]
        )
        when:
        def result = gradleRunner("test", "--warning-mode", "all").build()
        then:
        // subproject1 fully successful — skip entirely
        result.task(":subproject1:test").outcome == TaskOutcome.SKIPPED

        // subproject2 partially failed — exclude the 2 successful tests
        result.task(":subproject2:test").outcome == TaskOutcome.SUCCESS
        result.output.contains("excluding 0 successful suites and 2 successful tests")
        testNotExecuted(result.output, "SubProject2TestClazz1 > someTest1")
        testNotExecuted(result.output, "SubProject2TestClazz1 > someTest2")
        testExecuted(result.output, "SubProject2TestClazz2 > someTest1")
        testExecuted(result.output, "SubProject2TestClazz2 > someTest2")
    }

    def "excludes specific methods while running others in the same class"() {
        given:
        simpleTestSetup()
        writeHistory(
            [],
            [":subproject2:test": ["org.acme.SubProject2TestClazz2#someTest1"]]
        )
        when:
        def result = gradleRunner("test", "--warning-mode", "all").build()
        then:
        result.task(":subproject2:test").outcome == TaskOutcome.SUCCESS
        result.output.contains("excluding 0 successful suites and 1 successful tests")
        testNotExecuted(result.output, "SubProject2TestClazz2 > someTest1")
        testExecuted(result.output, "SubProject2TestClazz2 > someTest2")
        // Other classes in the same task still run
        testExecuted(result.output, "SubProject2TestClazz1 > someTest1")
        testExecuted(result.output, "SubProject2TestClazz1 > someTest2")
    }

    def "excludes a single parameter of a parameterized test while its siblings still run"() {
        given:
        parameterizedTestSetup()
        writeHistory(
            [],
            [":subproject1:test": ["org.acme.ParameterizedTestClazz#someTest1 {phase=0}"]]
        )
        when:
        def result = gradleRunner("test", "--warning-mode", "all").build()
        then:
        result.task(":subproject1:test").outcome == TaskOutcome.SUCCESS
        testNotExecuted(result.output, "someTest1 > someTest1 {phase=0}")
        testExecuted(result.output, "someTest1 > someTest1 {phase=1}")
        testExecuted(result.output, "someTest1 > someTest1 {phase=2}")
        // The bare method pattern the randomized runner needs must not take the other method down with it
        testExecuted(result.output, "someTest2 > someTest2 {phase=0}")
    }

    def "excludes a plain method of a randomized runner suite that has no parameters"() {
        given:
        parameterizedTestSetup()
        writeHistory(
            [],
            [":subproject1:test": ["org.acme.RandomizedTestClazz#someTest1"]]
        )
        when:
        def result = gradleRunner("test", "--warning-mode", "all").build()
        then:
        result.task(":subproject1:test").outcome == TaskOutcome.SUCCESS
        // Without parameters both descriptions the runner checks are identical, so the single bare pattern excludes it
        testNotExecuted(result.output, "RandomizedTestClazz > someTest1")
        testExecuted(result.output, "RandomizedTestClazz > someTest2")
    }

    def "excludes an entire parameterized suite via suite level pruning"() {
        given:
        parameterizedTestSetup()
        writeHistory(
            [],
            [:],
            [":subproject1:test": ["org.acme.ParameterizedTestClazz"]]
        )
        when:
        def result = gradleRunner("test", "--warning-mode", "all").build()
        then:
        result.task(":subproject1:test").outcome == TaskOutcome.SUCCESS
        result.output.contains("excluding 1 successful suites and 0 successful tests")
        testNotExecuted(result.output, "someTest1 > someTest1 {phase=0}")
        testNotExecuted(result.output, "someTest1 > someTest1 {phase=1}")
        testNotExecuted(result.output, "someTest1 > someTest1 {phase=2}")
        testNotExecuted(result.output, "someTest2 > someTest2 {phase=0}")
        // The plain suite in the same task is untouched
        testExecuted(result.output, "SubProject1TestClazz1 > someTest1")
    }

    def "runs all parameters when the project opted out of individual test pruning"() {
        given:
        parameterizedTestSetup(true)
        writeHistory(
            [],
            [":subproject1:test": ["org.acme.ParameterizedTestClazz#someTest1 {phase=0}"]]
        )
        when:
        def result = gradleRunner("test", "--warning-mode", "all").build()
        then:
        result.task(":subproject1:test").outcome == TaskOutcome.SUCCESS
        result.output.contains("project opted out of individual test pruning")
        testExecuted(result.output, "someTest1 > someTest1 {phase=0}")
        testExecuted(result.output, "someTest1 > someTest1 {phase=1}")
    }

    def "suite level pruning still applies when the project opted out of individual test pruning"() {
        given:
        parameterizedTestSetup(true)
        writeHistory(
            [],
            [":subproject1:test": ["org.acme.ParameterizedTestClazz#someTest1 {phase=0}"]],
            [":subproject1:test": ["org.acme.SubProject1TestClazz1"]]
        )
        when:
        def result = gradleRunner("test", "--warning-mode", "all").build()
        then:
        result.task(":subproject1:test").outcome == TaskOutcome.SUCCESS
        // A single message covers both halves of the outcome, instead of one line per concern
        result.output.contains(
            "excluding 1 successful suites from :subproject1:test and rerunning 1 successful tests " +
                "(project opted out of individual test pruning)"
        )
        result.output.contains("rerunning 1 successful tests in :subproject1:test") == false
        testNotExecuted(result.output, "SubProject1TestClazz1 > someTest1")
        testExecuted(result.output, "someTest1 > someTest1 {phase=0}")
    }

    def "task level pruning still applies when the project opted out of individual test pruning"() {
        given:
        parameterizedTestSetup(true)
        writeHistory([":subproject1:test"], [:])
        when:
        def result = gradleRunner("test", "--warning-mode", "all").build()
        then:
        result.task(":subproject1:test").outcome == TaskOutcome.SKIPPED
        result.output.contains("succeeded in previous run")
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
        // The size check runs before JSON parsing, so content validity is irrelevant here. Write a file
        // just over the 100MB cap in 1MB chunks to avoid allocating the whole payload in memory.
        file(".failed-test-history.json").withOutputStream { out ->
            byte[] chunk = new byte[1024 * 1024]
            for (int i = 0; i < 101; i++) {
                out.write(chunk)
            }
        }

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
  "successfulTests": {},
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

    def "task in successfulTasks takes precedence over successfulTests"() {
        given:
        simpleTestSetup()
        // Task is in both successfulTasks (skip entirely) and successfulTests (exclude tests).
        // successfulTasks should win — skip the whole task.
        writeHistory(
            [":subproject1:test"],
            [":subproject1:test": ["org.acme.SubProject1TestClazz1#someTest1"]]
        )

        when:
        def result = gradleRunner("test", "--warning-mode", "all").build()

        then:
        result.task(":subproject1:test").outcome == TaskOutcome.SKIPPED
    }

    boolean testExecuted(String output, String testReference) {
        output.contains(testReference + " STARTED")
    }

    boolean testNotExecuted(String output, String testReference) {
        output.contains(testReference) == false
    }

    private File writeHistory(
        List<String> successfulTasks,
        Map<String, List<String>> successfulTests,
        Map<String, List<String>> successfulSuites = [:]
    ) {
        def tasksJson = successfulTasks.collect { "\"$it\"" }.join(", ")
        def testsEntries = successfulTests.collect { taskPath, tests ->
            def testsJson = tests.collect { "\"$it\"" }.join(", ")
            "\"$taskPath\": [$testsJson]"
        }.join(", ")
        def suitesEntries = successfulSuites.collect { taskPath, suites ->
            def suitesJson = suites.collect { "\"$it\"" }.join(", ")
            "\"$taskPath\": [$suitesJson]"
        }.join(", ")
        file(".failed-test-history.json") << """
{
  "successfulTasks": [$tasksJson],
  "successfulSuites": {$suitesEntries},
  "successfulTests": {$testsEntries}
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

    /**
     * Sets up a single subproject holding a suite parameterized by the randomized runner alongside a plain JUnit4 suite.
     * The randomized runner reports such tests as {@code someTest1 {phase=0}} and only excludes one when both that name
     * and the bare method name are filtered out, which is what the parameterized pruning has to get right.
     */
    void parameterizedTestSetup(boolean optOutOfIndividualTestPruning = false) {
        buildFile << """
        allprojects {
                apply plugin: 'java'
                apply plugin: 'elasticsearch.internal-test-rerun'

                repositories {
                    mavenCentral()
                }

                dependencies {
                    testImplementation 'junit:junit:4.13.1'
                    testImplementation 'com.carrotsearch.randomizedtesting:randomizedtesting-runner:2.8.2'
                }

                tasks.named("test").configure {
                    testLogging {
                        events("started", "skipped")
                    }
                }
            }
            """
        subProject(":subproject1") {
            if (optOutOfIndividualTestPruning) {
                buildFile << """
                smartRetry.pruneIndividualTests.set(false)
                """
            }
            createTest("SubProject1TestClazz1")
            file("src/test/java/org/acme/RandomizedTestClazz.java") << """
            package org.acme;

            import com.carrotsearch.randomizedtesting.RandomizedRunner;
            import org.junit.Test;
            import org.junit.runner.RunWith;

            @RunWith(RandomizedRunner.class)
            public class RandomizedTestClazz {

                @Test
                public void someTest1() {
                }

                @Test
                public void someTest2() {
                }
            }
            """
            file("src/test/java/org/acme/ParameterizedTestClazz.java") << """
            package org.acme;

            import com.carrotsearch.randomizedtesting.RandomizedRunner;
            import com.carrotsearch.randomizedtesting.annotations.Name;
            import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
            import java.util.List;
            import org.junit.Test;
            import org.junit.runner.RunWith;

            @RunWith(RandomizedRunner.class)
            public class ParameterizedTestClazz {

                private final int phase;

                public ParameterizedTestClazz(@Name("phase") int phase) {
                    this.phase = phase;
                }

                @ParametersFactory(shuffle = false)
                public static Iterable<Object[]> parameters() {
                    return List.of(new Object[] { 0 }, new Object[] { 1 }, new Object[] { 2 });
                }

                @Test
                public void someTest1() {
                }

                @Test
                public void someTest2() {
                }
            }
            """
        }
    }
}

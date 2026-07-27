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

    def "does not skip a successful task that a running task depends on"() {
        given:
        // Force-run resolution inspects the resolved task graph, so it requires the configuration phase to
        // run (a config-cache miss). That always holds for smart retry, whose history file is not a tracked
        // config-cache input; see InternalTestRerunPlugin#resolveForceRunTasks.
        configurationCacheCompatible = false
        // :downstream:test dependsOn :upstream:test, and they share mutable external state (like the
        // rolling-upgrade cluster chain), so skipping the dependency would break the dependent task.
        chainedTestSetup([":upstream", ":downstream"])
        // :upstream succeeded previously but :downstream did not, so :downstream must rerun.
        writeHistory([":upstream:test"], [:])

        when:
        // Request only the downstream task so :upstream:test is pulled in purely as a dependency.
        def result = gradleRunner(":downstream:test", "--warning-mode", "all").build()

        then:
        // :upstream must still run even though it succeeded, because a running task depends on it.
        result.task(":upstream:test").outcome == TaskOutcome.SUCCESS
        result.task(":downstream:test").outcome == TaskOutcome.SUCCESS
        testExecuted(result.output, "UpstreamTestClazz > someTest1")
        testExecuted(result.output, "DownstreamTestClazz > someTest1")
    }

    def "still skips a successful task when the task depending on it is also skipped"() {
        given:
        chainedTestSetup([":upstream", ":downstream"])
        // Both succeeded previously and nothing else needs them, so both should be skipped.
        writeHistory([":upstream:test", ":downstream:test"], [:])

        when:
        def result = gradleRunner(":downstream:test", "--warning-mode", "all").build()

        then:
        result.task(":upstream:test").outcome == TaskOutcome.SKIPPED
        result.task(":downstream:test").outcome == TaskOutcome.SKIPPED
        result.output.contains("succeeded in previous run")
    }

    def "does not skip transitive successful dependencies of a running task"() {
        given:
        // See the note above: force-run resolution requires the configuration phase to run.
        configurationCacheCompatible = false
        // :libc:test -> :libb:test -> :liba:test
        chainedTestSetup([":liba", ":libb", ":libc"])
        // Only the final task failed; both upstream links succeeded but must still rerun so the shared
        // state is rebuilt through the whole chain, not just the direct dependency.
        writeHistory([":liba:test", ":libb:test"], [:])

        when:
        def result = gradleRunner(":libc:test", "--warning-mode", "all").build()

        then:
        result.task(":liba:test").outcome == TaskOutcome.SUCCESS
        result.task(":libb:test").outcome == TaskOutcome.SUCCESS
        result.task(":libc:test").outcome == TaskOutcome.SUCCESS
    }

    def "handles a dependency that exists but is not scheduled to run"() {
        given:
        // :downstream:test dependsOn :upstream:test. Excluding the dependency with -x removes it from the
        // execution plan while leaving it in the dependent task's declared dependencies, so force-run
        // resolution encounters a dependency it must not try to traverse.
        chainedTestSetup([":upstream", ":downstream"])
        writeHistory([":upstream:test"], [:])

        when:
        def result = gradleRunner(":downstream:test", "-x", ":upstream:test", "--warning-mode", "all").build()

        then:
        // The build must not fail resolving the task graph; the dependent runs and the excluded task does not.
        result.task(":downstream:test").outcome == TaskOutcome.SUCCESS
        result.task(":upstream:test") == null
    }

    boolean testExecuted(String output, String testReference) {
        output.contains(testReference + " STARTED")
    }

    boolean testNotExecuted(String output, String testReference) {
        output.contains(testReference) == false
    }

    private File writeHistory(List<String> successfulTasks, Map<String, List<String>> successfulTests) {
        def tasksJson = successfulTasks.collect { "\"$it\"" }.join(", ")
        def testsEntries = successfulTests.collect { taskPath, tests ->
            def testsJson = tests.collect { "\"$it\"" }.join(", ")
            "\"$taskPath\": [$testsJson]"
        }.join(", ")
        file(".failed-test-history.json") << """
{
  "successfulTasks": [$tasksJson],
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
     * Sets up a linear chain of subprojects where each project's {@code test} task depends on the previous
     * project's {@code test} task (project[i]:test dependsOn project[i-1]:test). Each project gets a single
     * test class named after the project (e.g. {@code :upstream} -> {@code UpstreamTestClazz}).
     */
    void chainedTestSetup(List<String> projectPaths) {
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
        String previousPath = null
        for (String projectPath : projectPaths) {
            String className = projectPath.replace(":", "").capitalize() + "TestClazz"
            String dependency = previousPath
            subProject(projectPath) {
                createTest(className)
                if (dependency != null) {
                    buildFile << """
                    tasks.named("test").configure { dependsOn "${dependency}:test" }
                    """
                }
            }
            previousPath = projectPath
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.runner

import groovy.json.JsonSlurper
import spock.lang.Shared
import spock.lang.Specification

/**
 * Functional test that exercises the GradleRunner fat JAR against the real
 * Elasticsearch checkout. The JAR is built by the {@code :build-tools:gradle-runner:jar}
 * task before this test runs (wired via the {@code test} task dependency).
 *
 * <p>Two scenarios are tested:
 * <ol>
 *   <li><b>Normal build</b> &ndash; runs the {@code help} task without preemption
 *       and validates that {@code task-status.json} is written correctly.</li>
 *   <li><b>Preemption build</b> &ndash; runs with simulated GCP preemption and
 *       validates the exit code, marker file, exit file, and task-status.json.</li>
 * </ol>
 */
class GradleRunnerFuncSpec extends Specification {

    @Shared
    static String runnerJar = System.getProperty('runner.jar')

    @Shared
    static String projectDir = System.getProperty('root.project.dir')

    static String preemptionExitFile = '/tmp/gradle-preemption-exit-local'

    def setupSpec() {
        assert runnerJar != null: 'System property runner.jar must be set'
        assert new File(runnerJar).exists(): "Runner JAR not found: ${runnerJar}"
        assert projectDir != null: 'System property root.project.dir must be set'
    }

    def setup() {
        // Clean output files before each test
        new File(projectDir, 'build/task-status.json').delete()
        new File(projectDir, 'build/.preemption-marker.json').delete()
        new File(preemptionExitFile).delete()
    }

    def "normal build writes task-status.json and exits with 0"() {
        when:
        def result = runGradleRunner([:], 'help')

        then: 'exit code is 0'
        result.exitCode == 0

        and: 'task-status.json exists with correct structure'
        def status = parseJson('build/task-status.json')
        status != null
        status.tasks instanceof List
        status.tasks.size() > 0
        status.tests instanceof List
        status.cancelled == false
        status.preemptedAt == null

        and: 'tasks have recorded outcomes'
        status.tasks.every { it.path != null && it.outcome != null }

        and: 'preemption artifacts are absent'
        !new File(projectDir, 'build/.preemption-marker.json').exists()
        !new File(preemptionExitFile).exists()
    }

    def "preempted build exits with 47 and writes all preemption artifacts"() {
        when:
        def result = runGradleRunner([
            GCP_PREEMPTION_WATCHDOG: 'true',
            GCP_PREEMPTION_SIMULATE_AFTER_SECONDS: '1'
        ], 'projects')

        then: 'exit code is 47'
        result.exitCode == 47

        and: 'task-status.json reflects preemption'
        def status = parseJson('build/task-status.json')
        status != null
        status.cancelled == true
        status.preemptedAt != null
        status.tasks instanceof List
        status.tests instanceof List

        and: 'no tasks are marked FAILED (preemption failures become INTERRUPTED)'
        status.tasks.every { it.outcome != 'FAILED' }

        and: 'preemption marker file is correct'
        def marker = parseJson('build/.preemption-marker.json')
        marker != null
        marker.preempted == true
        marker.preemptedAt != null

        and: 'preemption exit file contains 47'
        def exitFile = new File(preemptionExitFile)
        exitFile.exists()
        exitFile.text.trim() == '47'

        and: 'stdout mentions preemption'
        result.output.contains('[gcp-preemption-watchdog] preemption detected')
        result.output.contains('[gcp-preemption-watchdog] cancelling Gradle build via CancellationToken')
        result.output.contains('exiting with code 47')
    }

    def "preempted build respects custom exit code"() {
        when:
        def result = runGradleRunner([
            GCP_PREEMPTION_WATCHDOG: 'true',
            GCP_PREEMPTION_SIMULATE_AFTER_SECONDS: '1',
            GCP_PREEMPTION_EXIT_CODE: '99'
        ], 'projects')

        then:
        result.exitCode == 99

        and:
        def exitFile = new File(preemptionExitFile)
        exitFile.exists()
        exitFile.text.trim() == '99'
    }

    // --- Helpers ---

    private RunResult runGradleRunner(Map<String, String> env, String... gradleArgs) {
        def command = ['java', '-jar', runnerJar, '--project-dir', projectDir, '--'] + gradleArgs.toList()
        def pb = new ProcessBuilder(command)
        pb.redirectErrorStream(true)
        pb.environment().putAll(env)

        def process = pb.start()
        def output = new StringBuilder()
        def reader = new Thread({
            process.inputStream.eachLine { line ->
                output.append(line).append('\n')
            }
        })
        reader.start()
        def exited = process.waitFor(120, java.util.concurrent.TimeUnit.SECONDS)
        reader.join(5000)

        if (!exited) {
            process.destroyForcibly()
            throw new RuntimeException("GradleRunner timed out after 120s. Output:\n${output}")
        }

        return new RunResult(exitCode: process.exitValue(), output: output.toString())
    }

    private Object parseJson(String relativePath) {
        def file = new File(projectDir, relativePath)
        if (!file.exists()) return null
        new JsonSlurper().parseText(file.text)
    }

    static class RunResult {
        int exitCode
        String output
    }
}

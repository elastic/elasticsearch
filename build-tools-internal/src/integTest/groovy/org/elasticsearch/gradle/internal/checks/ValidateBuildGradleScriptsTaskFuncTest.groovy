/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.checks

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome

class ValidateBuildGradleScriptsTaskFuncTest extends AbstractGradleFuncTest {

    def setup() {
        buildFile << """
plugins {
  id 'elasticsearch.global-build-info'
}
import org.elasticsearch.gradle.internal.checks.ValidateBuildGradleScriptsTask

tasks.register('validateBuildGradleScripts', ValidateBuildGradleScriptsTask) {
  scriptFiles.from(fileTree(project.layout.settingsDirectory) {
    include '**/build.gradle'
    exclude '**/build/**'
    exclude '**/src/**/resources/**'
    exclude '.gradle/**'
  })
  ignore.set([:])
}
"""
    }

    def "fails with structured problems for cross-project dereferences"() {
        given:
        file('subproject/build.gradle').text = 'classpath += project(":server").sourceSets.main.runtimeClasspath\n'

        when:
        def result = gradleRunner('validateBuildGradleScripts').buildAndFail()

        then:
        result.task(':validateBuildGradleScripts').outcome == TaskOutcome.FAILED
        assertOutputContains(result.output, 'cross-project-dereference')
        assertOutputContains(result.output, 'subproject/build.gradle:1')
        assertOutputContains(result.output, 'Problems report is available at')
    }

    def "fails for stale ignore entries"() {
        given:
        buildFile << """
tasks.named('validateBuildGradleScripts').configure {
  ignore.set(['cross-project-dereference': ['subproject/build.gradle']])
}
"""
        file('subproject/build.gradle').text = 'dependencies { implementation project(":server") }\n'

        when:
        def result = gradleRunner('validateBuildGradleScripts').buildAndFail()

        then:
        result.task(':validateBuildGradleScripts').outcome == TaskOutcome.FAILED
        assertOutputContains(result.output, 'stale-ignore-entry')
        assertOutputContains(result.output, 'cross-project-dereference -> subproject/build.gradle')
    }

    def "ignores build.gradle files under test resources"() {
        given:
        file('src/test/resources/fixture/build.gradle').text = 'classpath += project(":server").sourceSets.main.runtimeClasspath\n'
        file('subproject/build.gradle').text = 'dependencies { implementation project(":server") }\n'

        when:
        def result = gradleRunner('validateBuildGradleScripts').build()

        then:
        result.task(':validateBuildGradleScripts').outcome == TaskOutcome.SUCCESS
        result.output.contains('cross-project-dereference') == false
    }

    def "execution closure findings have structured problems and exact-count ratcheting"() {
        given:
        file('subproject/build.gradle').text = "tasks.register('probe') { onlyIf { project.name } }\n"

        when:
        def result = gradleRunner('validateBuildGradleScripts').buildAndFail()

        then:
        result.output.contains('execution-closure-project-reference')
        result.output.contains('subproject/build.gradle:1')
        result.output.contains('Problems report is available at')

        when:
        buildFile << """
tasks.named('validateBuildGradleScripts') {
  ignore.set(['execution-closure-project-reference': ['subproject/build.gradle::1']])
}
"""
        result = gradleRunner('validateBuildGradleScripts').build()

        then:
        result.task(':validateBuildGradleScripts').outcome == TaskOutcome.SUCCESS

        when:
        file('subproject/build.gradle').text = "tasks.register('probe') { onlyIf { true } }\n"
        result = gradleRunner('validateBuildGradleScripts').buildAndFail()

        then:
        result.output.contains('stale-ignore-entry')
    }

    def "parse failures are actionable problems rather than a silently skipped script"() {
        given:
        file('subproject/build.gradle').text = "tasks.register('broken') {\n"

        when:
        def result = gradleRunner('validateBuildGradleScripts').buildAndFail()

        then:
        result.output.contains('build-script-parse-failure')
        result.output.contains('subproject/build.gradle')
        result.output.contains('Unexpected input')
        result.output.contains('Problems report is available at')
    }

    def "validator executes again from a reused configuration cache and ignores precompiled scripts"() {
        given:
        file('subproject/build.gradle').text = "def name = project.name; tasks.register('probe') { doLast { println name } }\n"
        file('src/main/groovy/fixture.gradle').text = "tasks.register('unsafe') { doLast { project.name } }\n"

        when:
        def first = gradleRunner('validateBuildGradleScripts', '--rerun-tasks').build()
        def reused = gradleRunner('validateBuildGradleScripts', '--rerun-tasks').build()

        then:
        first.task(':validateBuildGradleScripts').outcome == TaskOutcome.SUCCESS
        reused.task(':validateBuildGradleScripts').outcome == TaskOutcome.SUCCESS
        reused.output.contains('Reusing configuration cache.')
    }

    def "is up to date when inputs are unchanged"() {
        given:
        file('subproject/build.gradle').text = 'dependencies { implementation project(":server") }\n'

        when:
        def result = gradleRunner('validateBuildGradleScripts').build()

        then:
        result.task(':validateBuildGradleScripts').outcome == TaskOutcome.SUCCESS

        when:
        result = gradleRunner('validateBuildGradleScripts').build()

        then:
        result.task(':validateBuildGradleScripts').outcome == TaskOutcome.UP_TO_DATE
    }
}

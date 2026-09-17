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
    exclude '.gradle/**'
  })
  baseline.set([:])
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

    def "fails for stale baseline entries"() {
        given:
        buildFile << """
tasks.named('validateBuildGradleScripts').configure {
  baseline.set(['cross-project-dereference': ['subproject/build.gradle']])
}
"""
        file('subproject/build.gradle').text = 'dependencies { implementation project(":server") }\n'

        when:
        def result = gradleRunner('validateBuildGradleScripts').buildAndFail()

        then:
        result.task(':validateBuildGradleScripts').outcome == TaskOutcome.FAILED
        assertOutputContains(result.output, 'stale-baseline-entry')
        assertOutputContains(result.output, 'cross-project-dereference -> subproject/build.gradle')
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

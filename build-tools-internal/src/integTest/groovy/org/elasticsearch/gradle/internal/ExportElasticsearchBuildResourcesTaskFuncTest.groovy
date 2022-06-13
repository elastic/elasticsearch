/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome

class ExportElasticsearchBuildResourcesTaskFuncTest extends AbstractGradleFuncTest {

    def setup() {
        buildFile << """
plugins {
  id 'elasticsearch.global-build-info'
}
import org.elasticsearch.gradle.internal.ExportElasticsearchBuildResourcesTask

File buildResourcesDir = new File(project.getBuildDir(), 'build-tools-exported')
TaskProvider buildResourcesTask = tasks.register('buildResources', ExportElasticsearchBuildResourcesTask) {
  outputDir = buildResourcesDir
  copy 'checkstyle_suppressions.xml'
  copy 'checkstyle.xml'
}
"""
    }

    def "test upToDate with sources configured"() {
        when:
        def result = gradleRunner("buildResources").build()

        then:
        result.task(":buildResources").outcome == TaskOutcome.SUCCESS
        file("build/build-tools-exported/checkstyle.xml").exists()
        file("build/build-tools-exported/checkstyle_suppressions.xml").exists()

        when:
        result = gradleRunner("buildResources").build()

        then:
        result.task(":buildResources").outcome == TaskOutcome.UP_TO_DATE
    }

    def "test output as input"() {
        given:
        buildFile << """
tasks.register("sampleCopy", Sync) {
  /** Note: no explicit dependency. This works with tasks that use the Provider API a.k.a "Lazy Configuration" **/
  from buildResourcesTask
  into "build/sampleCopy"
}"""
        when:
        def result = gradleRunner("sampleCopy").build()

        then:
        result.task(":sampleCopy").outcome == TaskOutcome.SUCCESS
        file("build/sampleCopy/checkstyle.xml").exists()
        file("build/sampleCopy/checkstyle_suppressions.xml").exists()
    }

    def "test incorrect usage"() {
        given:
        buildFile << """
tasks.register("noConfigAfterExecution") {
  dependsOn buildResourcesTask
  doLast {
    buildResourcesTask.get().copy('foo')
  }
}"""
        when:
        def result = gradleRunner("noConfigAfterExecution").buildAndFail()

        then:
        result.task(":noConfigAfterExecution").outcome == TaskOutcome.FAILED
        result.output.contains("buildResources can't be configured after the task ran")
    }
}

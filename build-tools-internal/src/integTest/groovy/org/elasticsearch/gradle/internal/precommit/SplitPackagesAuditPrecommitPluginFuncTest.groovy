/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.precommit

import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.gradle.testkit.runner.TaskOutcome

class SplitPackagesAuditPrecommitPluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    Class<SplitPackagesAuditPrecommitPlugin> pluginClassUnderTest = SplitPackagesAuditPrecommitPlugin.class

    /**
     * The audit reads every source directory of the main source set, including directories that
     * are produced by another task (e.g. the protobuf plugin registers processProtoResources'
     * output as a main resource source directory). This mirrors that setup with a generic
     * generator task and asserts the generator runs before the audit, so the task-dependency edge
     * is declared and Gradle 9.9's dependency validation does not fail the build.
     */
    def "runs a source-generating task before the audit consumes its output"() {
        given:
        buildFile << """
        apply plugin: 'java'

        def generateResources = tasks.register("generateResources") {
            def outputDir = layout.buildDirectory.dir("generated/resources/main")
            outputs.dir(outputDir)
            doLast {
                def generated = outputDir.get().file("org/example/generated.properties").asFile
                generated.parentFile.mkdirs()
                generated.text = "generated=true"
            }
        }
        // Registering the generator's output as a resource source directory establishes the
        // builtBy dependency the audit must honour.
        sourceSets.main.resources.srcDir(generateResources)
        """
        file("src/main/java/org/example/Foo.java").text = "package org.example; public class Foo {}"

        when:
        // Also run `classes`, so `processResources` pulls the generator into the task graph. Without
        // the audit declaring a dependency on the generated source directory, Gradle 9.9 fails the
        // build with an implicit-dependency validation error, exactly as observed in CI.
        def result = gradleRunner("classes", "splitPackagesAudit").build()

        then:
        result.task(":generateResources").outcome == TaskOutcome.SUCCESS
        result.task(":splitPackagesAudit").outcome == TaskOutcome.SUCCESS

        and: "the generator runs before the audit"
        def executed = result.tasks.collect { it.path }
        executed.indexOf(":generateResources") < executed.indexOf(":splitPackagesAudit")

        and: "no implicit-dependency validation error is reported"
        result.output.contains("without declaring an explicit or implicit dependency") == false
    }
}

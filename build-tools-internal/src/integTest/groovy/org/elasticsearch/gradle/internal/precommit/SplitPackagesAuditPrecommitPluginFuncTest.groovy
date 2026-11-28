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

    def "reports the owning project of a project dependency with isolated projects enabled (#buildDirLayout build dir, #libraryElements)"() {
        given:
        applyProjectBuildDirsSettingsPlugin()
        splitPackageProject("a", buildDirConfig)
        splitPackageProject("b", """
        configurations.compileClasspath.attributes {
            attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, objects.named(LibraryElements, '${libraryElements}'))
        }
        dependencies { implementation project(':a') }
        """)

        when:
        def result = gradleRunner(":b:splitPackagesAudit", "--isolated-projects").buildAndFail()

        then:
        result.task(":b:splitPackagesAudit").outcome == TaskOutcome.FAILED
        assertSplitPackageReported(result.output, "project :a")
        assertOutputMissing(result.output, "Build directory unknown to gradle")

        where:
        // relocated build directories are unknown to the settings-level service, the owner is taken from the
        // resolved project artifact instead
        buildDirLayout | buildDirConfig                              | libraryElements
        "conventional" | ""                                          | "classes"
        "relocated"    | "layout.buildDirectory = file('out/build')" | "classes"
        "conventional" | ""                                          | "jar"
        "relocated"    | "layout.buildDirectory = file('out/build')" | "jar"
    }

    def "reports the owning project of a file dependency into a project build directory"() {
        given:
        applyProjectBuildDirsSettingsPlugin()
        splitPackageProject("a")
        // a plain file dependency carries no component identity, so the owner can only be derived from the build
        // directory the classes live in
        prebuiltClasses("a/build/prebuilt")
        splitPackageProject("b", "dependencies { implementation files('../a/build/prebuilt') }")

        when:
        def result = gradleRunner(":b:splitPackagesAudit", "--isolated-projects").buildAndFail()

        then:
        result.task(":b:splitPackagesAudit").outcome == TaskOutcome.FAILED
        assertSplitPackageReported(result.output, "project :a")
    }

    def "reports the classpath directory when no project owns it"() {
        given:
        applyProjectBuildDirsSettingsPlugin()
        // the "build" ancestor is not a project build directory, so it must not be used as the label either
        prebuiltClasses("vendor/build/checkout/classes")
        splitPackageProject("b", "dependencies { implementation files('../vendor/build/checkout/classes') }")

        when:
        def result = gradleRunner(":b:splitPackagesAudit", "--isolated-projects").buildAndFail()

        then:
        result.task(":b:splitPackagesAudit").outcome == TaskOutcome.FAILED
        assertSplitPackageReported(result.output, "./vendor/build/checkout/classes")
        assertOutputMissing(result.output, "Build directory unknown to gradle")
    }

    def "reports the classpath directory when project build dirs service is unavailable"() {
        given:
        splitPackageProject("a")
        prebuiltClasses("a/build/prebuilt")
        splitPackageProject("b", "dependencies { implementation files('../a/build/prebuilt') }")

        when:
        def result = gradleRunner(":b:splitPackagesAudit", "--isolated-projects").buildAndFail()

        then:
        result.task(":b:splitPackagesAudit").outcome == TaskOutcome.FAILED
        assertSplitPackageReported(result.output, "./a/build/prebuilt")
        assertOutputMissing(result.output, "Build directory unknown to gradle")
    }

    private void applyProjectBuildDirsSettingsPlugin() {
        settingsFile.text = settingsFile.text.replace(
            "id 'elasticsearch.java-toolchain'",
            "id 'elasticsearch.java-toolchain'\n            id 'elasticsearch.project-build-dirs'"
        )
    }

    /**
     * Creates a java project with its own build script (no cross-project configuration, so the build stays compatible
     * with isolated projects) that defines a class in the package split across projects.
     */
    private void splitPackageProject(String name, String extraConfig = "") {
        subProject(":" + name) << """
        plugins {
            id 'java'
        }
        plugins.apply(org.elasticsearch.gradle.internal.precommit.SplitPackagesAuditPrecommitPlugin)
        ${extraConfig}
        """
        file("${name}/src/main/java/org/acme/splitpkg/From${name.capitalize()}.java").text = """
        package org.acme.splitpkg;
        public class From${name.capitalize()} {}
        """
    }

    /**
     * Places a class in the split package into {@code dir} without any task producing it, so file dependencies on
     * it do not need task dependencies. The audit only inspects class file names, the content is irrelevant.
     */
    private void prebuiltClasses(String dir) {
        file("${dir}/org/acme/splitpkg/Prebuilt.class").bytes = new byte[0]
    }

    private void assertSplitPackageReported(String output, String dependencyLabel) {
        assertOutputContains(
            output,
            """Project :b defines classes in package org.acme.splitpkg exposed by dependencies
  Dependencies:
    ${dependencyLabel}
  Classes:"""
        )
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.nativelibs

import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.gradle.api.Plugin
import org.gradle.testkit.runner.TaskOutcome

class NativeLibraryBuildPluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    /** The platform directory a build has to populate on this machine. */
    static final String PLATFORM = BuildNativeLibraryTask.hostPlatform()

    @Override
    <T extends Plugin> Class<T> getPluginClassUnderTest() {
        NativeLibraryBuildPlugin.class
    }

    def setup() {
        file("native/src/lib.c") << "int answer() { return 42; }"
        file("native/Makefile") << "all:\n\ttrue\n"
        buildFile << """
        nativeLibraryBuild {
          modeEnvironmentVariable = 'TEST_NATIVE_BUILD'
          sourceDir = layout.projectDirectory.dir('native')
          sources = ['src/**', 'Makefile']
          toolchainImage = 'example/toolchain:1'
          dockerCommand = ['make', 'all']
          hostCommand { outputDir -> ['sh', '-c', "mkdir -p \$outputDir.asFile/${PLATFORM} && echo built > \$outputDir.asFile/${PLATFORM}/libtest.so"] }
        }
        """
    }

    def "builds from source on the host and substitutes the output directory"() {
        when:
        def result = gradleRunner("buildNativeLibrary").withEnvironment(["TEST_NATIVE_BUILD": "host"]).build()

        then:
        result.task(":buildNativeLibrary").outcome == TaskOutcome.SUCCESS
        file("build/native-libs/${PLATFORM}/libtest.so").exists()
    }

    def "is up to date when the sources have not changed"() {
        given:
        gradleRunner("buildNativeLibrary").withEnvironment(["TEST_NATIVE_BUILD": "host"]).build()

        when:
        def result = gradleRunner("buildNativeLibrary").withEnvironment(["TEST_NATIVE_BUILD": "host"]).build()

        then:
        result.task(":buildNativeLibrary").outcome == TaskOutcome.UP_TO_DATE
    }

    def "reruns when a source file changes"() {
        given:
        gradleRunner("buildNativeLibrary").withEnvironment(["TEST_NATIVE_BUILD": "host"]).build()

        when:
        file("native/src/lib.c") << "int other() { return 1; }"
        def result = gradleRunner("buildNativeLibrary").withEnvironment(["TEST_NATIVE_BUILD": "host"]).build()

        then:
        result.task(":buildNativeLibrary").outcome == TaskOutcome.SUCCESS
    }

    def "explains itself when no build mode is selected"() {
        when:
        def result = gradleRunner("buildNativeLibrary").buildAndFail()

        then:
        result.output.contains("configured to come from its published artifact")
    }

    def "fails when the build produces nothing for this platform"() {
        given:
        buildFile << """
        nativeLibraryBuild.hostCommand { outputDir -> ['sh', '-c', 'true'] }
        """

        when:
        def result = gradleRunner("buildNativeLibrary").withEnvironment(["TEST_NATIVE_BUILD": "host"]).buildAndFail()

        then:
        result.output.contains("Build produced nothing under")
        result.output.contains(PLATFORM)
    }

    def "exposes the built tree as a consumable variant produced by the task"() {
        given:
        buildFile << """
        tasks.register('variantArtifacts') {
          def artifacts = configurations.${NativeLibraryBuildPlugin.ELEMENTS_CONFIGURATION}.artifacts
          def producers = artifacts.collectMany { it.buildDependencies.getDependencies(null) }.collect { it.name }
          doLast { println "producedBy=" + producers }
        }
        """

        when:
        def result = gradleRunner("variantArtifacts").withEnvironment(["TEST_NATIVE_BUILD": "host"]).build()

        then:
        result.output.contains("producedBy=[buildNativeLibrary]")
    }
}

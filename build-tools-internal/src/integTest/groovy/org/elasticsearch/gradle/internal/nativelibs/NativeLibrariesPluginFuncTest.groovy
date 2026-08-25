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

class NativeLibrariesPluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    static final String PLATFORM = BuildNativeLibraryTask.hostPlatform()

    @Override
    <T extends Plugin> Class<T> getPluginClassUnderTest() {
        NativeLibrariesPlugin.class
    }

    def setup() {
        settingsFile << """
        include 'producer'
        """
        file("producer/native/Makefile") << "all:\n\ttrue\n"
        file("producer/build.gradle") << """
        plugins.apply(${NativeLibraryBuildPlugin.class.name})
        nativeLibraryBuild {
          modeEnvironmentVariable = 'TEST_NATIVE_BUILD'
          sourceDir = layout.projectDirectory.dir('native')
          sources = ['Makefile']
          toolchainImage = 'example/toolchain:1'
          dockerCommand = ['make', 'all']
          hostCommand { outputDir -> ['sh', '-c', "mkdir -p \$outputDir.asFile/${PLATFORM} && echo built > \$outputDir.asFile/${PLATFORM}/libtest.so"] }
        }
        """
        buildFile << """
        nativeLibraries {
          test {
            modeEnvironmentVariable = 'TEST_NATIVE_BUILD'
            publishedModule = 'org.example:test:1.0.0@zip'
            builtBy = ':producer'
          }
        }

        tasks.register('showSelection') {
          def selected = configurations.${NativeLibrariesPlugin.SOURCES_CONFIGURATION}
              .incoming.dependencies.collect { it.toString() }
          doLast { println "selected=" + selected }
        }

        tasks.register('collectLibraries', Copy) {
          from configurations.${NativeLibrariesPlugin.LIBRARIES_CONFIGURATION}
          into layout.buildDirectory.dir('collected')
        }
        """
    }

    def "selects the published module when no build mode is set"() {
        when:
        def result = gradleRunner("showSelection").build()

        then:
        result.output.contains("org.example:test:1.0.0")
        result.output.contains("project ':producer'") == false
    }

    def "selects the building project when a build mode is set"() {
        when:
        def result = gradleRunner("showSelection").withEnvironment(["TEST_NATIVE_BUILD": "host"]).build()

        then:
        result.output.contains("project ':producer'")
        result.output.contains("org.example:test:1.0.0") == false
    }

    def "resolving the libraries builds them from source and yields the platform tree"() {
        when:
        def result = gradleRunner("collectLibraries").withEnvironment(["TEST_NATIVE_BUILD": "host"]).build()

        then:
        result.task(":producer:buildNativeLibrary") != null
        file("build/collected/${PLATFORM}/libtest.so").exists()
    }
}

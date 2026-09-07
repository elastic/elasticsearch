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

import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

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
          supportedPlatforms = ['${PLATFORM}']
          sourceDir = layout.projectDirectory.dir('native')
          sources = ['Makefile']
          toolchainImage = 'example/toolchain:1'
          dockerCommand = ['make', 'all']
          hostCommand { outputDir -> ['sh', '-c', "mkdir -p \$outputDir.asFile/${PLATFORM} && echo built > \$outputDir.asFile/${PLATFORM}/libtest.so"] }
        }
        """
        publishArtifact()
        buildFile << """
        repositories {
          maven {
            url = layout.projectDirectory.dir('maven-repo')
            metadataSources { artifact() }
          }
        }

        nativeLibraries {
          test {
            modeEnvironmentVariable = 'TEST_NATIVE_BUILD'
            publishedModule = 'org.example:test:1.0.0@zip'
            builtBy = ':producer'
          }
        }

        tasks.register('collectLibraries', Copy) {
          from configurations.${NativeLibrariesPlugin.LIBRARIES_CONFIGURATION}
          into layout.buildDirectory.dir('collected')
        }
        """
    }

    def "takes the published artifact and does not build when no build mode is set"() {
        when:
        def result = gradleRunner("collectLibraries").build()

        then:
        result.task(":collectLibraries").outcome == TaskOutcome.SUCCESS
        result.task(":producer:buildNativeLibrary") == null
        file("build/collected/${PLATFORM}/libtest.so").text.trim() == "from-repository"
    }

    def "builds from source when a build mode is set"() {
        when:
        def result = gradleRunner("collectLibraries").withEnvironment(["TEST_NATIVE_BUILD": "host"]).build()

        then:
        result.task(":producer:buildNativeLibrary").outcome == TaskOutcome.SUCCESS
        file("build/collected/${PLATFORM}/libtest.so").text.trim() == "built"
    }

    /** A published artifact in a local Maven repository, so the published path is resolvable. */
    private void publishArtifact() {
        def zip = new ByteArrayOutputStream()
        new ZipOutputStream(zip).withStream { out ->
            out.putNextEntry(new ZipEntry("${PLATFORM}/libtest.so"))
            out.write("from-repository\n".getBytes("UTF-8"))
            out.closeEntry()
        }
        def artifact = file("maven-repo/org/example/test/1.0.0/test-1.0.0.zip")
        artifact.parentFile.mkdirs()
        artifact.bytes = zip.toByteArray()
    }
}

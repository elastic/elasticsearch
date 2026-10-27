/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal

import org.elasticsearch.gradle.Architecture
import org.elasticsearch.gradle.ElasticsearchDistribution
import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.gradle.api.Plugin
import org.gradle.testkit.runner.TaskOutcome

import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.Path

/**
 * Covers how {@link ElasticsearchJavaBasePlugin} wires the native libraries into test tasks.
 * <p>
 * {@code es.nativelibs.path} is absolute and differs per checkout, so it is passed as a non-input property while the
 * library contents are tracked as a relative-path-sensitive input. A relocated checkout with the same libraries must
 * reuse the cached test result, and a changed library must re-run the tests.
 */
class ElasticsearchJavaBasePluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    Class<? extends Plugin> pluginClassUnderTest = ElasticsearchJavaBasePlugin

    @TempDir
    File buildCacheDir

    @TempDir
    File relocatedParentDir

    def setup() {
        // a shared local build cache, so a second checkout at another path can reuse cached results
        settingsFile << """
            buildCache {
                local {
                    directory = file('${buildCacheDir.absolutePath}')
                }
            }
        """
        // the fixture stubs :libs:native:native-libraries with an empty 'nativeLibs' variant; give it a library directory
        subProject(':libs:native:native-libraries') << """
            artifacts {
                nativeLibs(layout.projectDirectory.dir('platform'))
            }
        """
        nativeLibrary(testProjectDir.root).text = "native library v1"

        buildFile << """
            apply plugin: 'java'
            repositories { mavenCentral() }
            dependencies { testImplementation 'junit:junit:4.13.2' }
        """
        testClazz("org.acme.SomeTests") {
            """
            @org.junit.Test public void someTest() {}
            """
        }
    }

    def "test results are reused across checkouts and invalidated by native library changes"() {
        when:
        def result = gradleRunner("test", "--build-cache").build()

        then:
        result.task(":test").outcome == TaskOutcome.SUCCESS

        when: "the same sources are built from another directory"
        File relocatedDir = relocatedCheckout()
        result = gradleRunner(relocatedDir, "test", "--build-cache").build()

        then:
        result.task(":test").outcome == TaskOutcome.FROM_CACHE

        when: "a native library changes"
        nativeLibrary(relocatedDir).text = "native library v2"
        result = gradleRunner(relocatedDir, "test", "--build-cache", "-i").build()

        then:
        result.task(":test").outcome == TaskOutcome.SUCCESS
        result.output.contains("Input property 'nativeLibraries' file")
    }

    private static File nativeLibrary(File projectDir) {
        String platform = "${ElasticsearchDistribution.CURRENT_PLATFORM}-${Architecture.current().toString().toLowerCase(Locale.ROOT)}"
        File library = new File(projectDir, "libs/native/native-libraries/platform/${platform}/libsome.so")
        library.parentFile.mkdirs()
        return library
    }

    private File relocatedCheckout() {
        Path source = testProjectDir.root.toPath()
        Path target = new File(relocatedParentDir, "relocated-checkout").toPath()
        Files.walk(source).withCloseable { paths ->
            paths.filter { Path p -> source.relativize(p).any { it.toString() in ["build", ".gradle"] } == false }
                .forEach { Path p ->
                    Path copy = target.resolve(source.relativize(p).toString())
                    if (Files.isDirectory(p)) {
                        Files.createDirectories(copy)
                    } else {
                        Files.copy(p, copy)
                    }
                }
        }
        return target.toFile()
    }
}

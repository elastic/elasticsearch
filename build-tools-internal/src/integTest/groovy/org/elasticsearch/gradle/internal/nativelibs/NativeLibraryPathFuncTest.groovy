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
import org.elasticsearch.gradle.internal.ElasticsearchJavaBasePlugin
import org.gradle.api.Plugin
import org.gradle.testkit.runner.TaskOutcome

/**
 * Tests how {@link ElasticsearchJavaBasePlugin} sets {@code es.nativelibs.path} on {@code Test}
 * tasks. Instead of running a test JVM, a {@code report<Task>} task writes out the
 * {@code es.nativelibs.path} argument that the {@code Test} task would pass to its JVM, and
 * the tests check that file.
 */
class NativeLibraryPathFuncTest extends AbstractGradleInternalPluginFuncTest {

    /** The {@code <os>-<arch>} name of the platform the test runs on. */
    static final String PLATFORM = BuildNativeLibraryTask.hostPlatform()

    Class<? extends Plugin> pluginClassUnderTest = ElasticsearchJavaBasePlugin.class

    def setup() {
        buildFile << """
        import ${NativeLibraryPathArgumentProvider.name}

        apply plugin: 'java'

        abstract class ReportJvmArguments extends DefaultTask {
          @Nested
          abstract ListProperty<CommandLineArgumentProvider> getProviders()

          @OutputFile
          abstract RegularFileProperty getReport()

          @TaskAction
          void write() {
            report.get().asFile.text = providers.get().collectMany { it.asArguments() as List }.join('\\n')
          }
        }

        def reportOn = { String testTask ->
          tasks.register("report\${testTask.capitalize()}", ReportJvmArguments) {
            providers.addAll(tasks.named(testTask).map { t -> t.jvmArgumentProviders.findAll { it instanceof NativeLibraryPathArgumentProvider } })
            report = layout.buildDirectory.file("\${testTask}-jvm-arguments.txt")
          }
        }
        reportOn('test')
        """
    }

    def "a test task passes the directories containing the native libraries of the projects it depends on"() {
        given:
        nativeLibraryProject('library')
        buildFile << """
        dependencies {
          implementation project(':library')
        }
        """

        when:
        def result = gradleRunner("reportTest").withEnvironment(["TEST_NATIVE_BUILD": "host"]).build()

        then:
        result.task(":library:buildNativeLibrary").outcome == TaskOutcome.SUCCESS
        nativeLibraryPath('test') == [file("library/build/native-libs/${PLATFORM}").canonicalPath]
    }

    def "a test task depending on no project that builds native libraries passes an empty es.nativelibs.path"() {
        given:
        nativeLibraryProject('library')
        plainProject('plain')
        buildFile << """
        dependencies {
          implementation project(':plain')
        }
        """

        when:
        def result = gradleRunner("reportTest").withEnvironment(["TEST_NATIVE_BUILD": "host"]).build()

        then:
        result.task(":library:buildNativeLibrary") == null
        file("build/test-jvm-arguments.txt").text == "-Des.nativelibs.path="
    }

    def "a test task named after a source set uses that source set's runtime classpath, any other test task uses all source sets' runtime classpaths"() {
        given:
        nativeLibraryProject('testLibrary')
        nativeLibraryProject('extraLibrary')
        buildFile << """
        sourceSets {
          extra
        }
        dependencies {
          testImplementation project(':testLibrary')
          extraImplementation project(':extraLibrary')
        }
        tasks.register('otherTest', Test)
        reportOn('otherTest')
        """

        when:
        gradleRunner("reportTest", "reportOtherTest").withEnvironment(["TEST_NATIVE_BUILD": "host"]).build()

        then:
        nativeLibraryPath('test') == [file("testLibrary/build/native-libs/${PLATFORM}").canonicalPath]
        nativeLibraryPath('otherTest') as Set == [
            file("testLibrary/build/native-libs/${PLATFORM}").canonicalPath,
            file("extraLibrary/build/native-libs/${PLATFORM}").canonicalPath
        ] as Set
    }

    def "a test task passes the directory containing the native libraries built by the project of the task"() {
        given:
        file("native/Makefile") << "all:\n\ttrue\n"
        buildFile << """
        plugins.apply(${NativeLibraryBuildPlugin.name})
        ${nativeLibraryBuild('hello-world')}
        """

        when:
        def result = gradleRunner("reportTest").withEnvironment(["TEST_NATIVE_BUILD": "host"]).build()

        then:
        result.task(":buildNativeLibrary").outcome == TaskOutcome.SUCCESS
        nativeLibraryPath('test') == [file("build/native-libs/${PLATFORM}").canonicalPath]
    }

    /** The directories listed in the {@code es.nativelibs.path} argument that {@code testTask} would pass to its JVM. */
    private List<String> nativeLibraryPath(String testTask) {
        String arguments = file("build/${testTask}-jvm-arguments.txt").text
        assert arguments.startsWith("-Des.nativelibs.path=")
        String path = arguments.substring("-Des.nativelibs.path=".length())
        return path.isEmpty() ? [] : path.split(File.pathSeparator).collect { new File(it).canonicalPath }
    }

    /** Adds a Java library project, {@code name}, that applies {@link NativeLibraryBuildPlugin} and builds {@code lib<name>.so}. */
    private void nativeLibraryProject(String name) {
        settingsFile << """
        include '${name}'
        """
        file("${name}/native/Makefile") << "all:\n\ttrue\n"
        file("${name}/build.gradle") << """
        apply plugin: 'java-library'
        plugins.apply(${NativeLibraryBuildPlugin.name})
        ${nativeLibraryBuild(name)}
        """
    }

    /** Adds a Java library project, {@code name}, that builds no native library. */
    private void plainProject(String name) {
        settingsFile << """
        include '${name}'
        """
        file("${name}/build.gradle") << """
        apply plugin: 'java-library'
        """
    }

    /** A {@code nativeLibraryBuild} block whose host build writes {@code lib<name>.so} for the platform the test runs on. */
    private static String nativeLibraryBuild(String name) {
        return """
        nativeLibraryBuild {
          modeEnvironmentVariable = 'TEST_NATIVE_BUILD'
          supportedPlatforms = ['${PLATFORM}']
          workingDir = layout.projectDirectory.dir('native')
          sources = ['native/Makefile']
          toolchainImage = 'example/toolchain:1'
          dockerCommand = ['make', 'all']
          hostCommand { outputDir -> ['sh', '-c', "mkdir -p \$outputDir.asFile/${PLATFORM} && echo built > \$outputDir.asFile/${PLATFORM}/lib${name}.so"] }
        }
        """
    }
}

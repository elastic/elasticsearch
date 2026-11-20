/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.nativelibs

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import com.sun.net.httpserver.HttpServer
import groovy.io.FileType
import groovy.json.JsonSlurper
import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.gradle.api.Plugin
import org.gradle.testkit.runner.TaskOutcome

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

class NativeLibraryBuildPluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    /** The platform directory a build has to populate on this machine. */
    static final String PLATFORM = BuildNativeLibraryTask.hostPlatform()

    HttpServer server

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
          supportedPlatforms = ['${PLATFORM}']
          workingDir = layout.projectDirectory.dir('native')
          sources = ['native/src/**', 'native/Makefile', 'build.gradle']
          toolchainImage = 'example/toolchain:1'
          dockerCommand = ['make', 'all']
          hostCommand { outputDir -> ['sh', '-c', "mkdir -p \$outputDir.asFile/${PLATFORM} && echo built > \$outputDir.asFile/${PLATFORM}/libtest.so"] }
        }
        """
    }

    def cleanup() {
        server?.stop(0)
    }

    def "builds from source on the host and substitutes the output directory"() {
        when:
        def result = gradleRunner("buildNativeLibrary").withEnvironment(["TEST_NATIVE_BUILD": "host"]).build()

        then:
        result.task(":buildNativeLibrary").outcome == TaskOutcome.SUCCESS
        file("build/native-libs/${PLATFORM}/libtest.so").exists()
    }

    def "reuses the published artifact without building when one exists for these sources"() {
        given:
        useRepositoryServing { exchange ->
            respond(exchange, 200, publishedArchive())
        }
        markerOnBuild()

        when:
        def result = gradleRunner("buildNativeLibrary").withEnvironment(["TEST_NATIVE_BUILD": "host"]).build()

        then:
        result.task(":buildNativeLibrary").outcome == TaskOutcome.SUCCESS
        file("build/native-libs/${PLATFORM}/libtest.so").text.trim() == "from-repository"
        file("build/native-libs/built-marker").exists() == false
    }

    def "builds from source when these sources have no published artifact"() {
        given:
        useRepositoryServing { exchange ->
            respond(exchange, 404, new byte[0])
        }
        markerOnBuild()

        when:
        def result = gradleRunner("buildNativeLibrary").withEnvironment(["TEST_NATIVE_BUILD": "host"]).build()

        then:
        result.task(":buildNativeLibrary").outcome == TaskOutcome.SUCCESS
        file("build/native-libs/built-marker").exists()
    }

    def "builds from source offline, without asking the repository"() {
        given:
        def requests = new AtomicInteger()
        useRepositoryServing { exchange ->
            requests.incrementAndGet()
            respond(exchange, 404, new byte[0])
        }
        markerOnBuild()

        when:
        def result = gradleRunner("buildNativeLibrary", "--offline")
            .withEnvironment(["TEST_NATIVE_BUILD": "host"])
            .build()

        then:
        result.task(":buildNativeLibrary").outcome == TaskOutcome.SUCCESS
        file("build/native-libs/built-marker").exists()
        requests.get() == 0
    }

    def "explains itself when offline and no build mode is selected"() {
        given:
        useRepositoryServing { exchange ->
            respond(exchange, 404, new byte[0])
        }

        when:
        def result = gradleRunner("buildNativeLibrary", "--offline").buildAndFail()

        then:
        result.output.contains("while offline")
    }

    def "does not publish a host build even with a credential"() {
        given:
        def puts = new AtomicInteger()
        useRepositoryServing { exchange ->
            if (exchange.requestMethod == "PUT") {
                puts.incrementAndGet()
                exchange.requestBody.bytes
                respond(exchange, 201, new byte[0])
            } else {
                respond(exchange, 404, new byte[0])
            }
        }
        buildFile << """
        nativeLibraryBuild.publishCredentialEnvironmentVariable = 'TEST_ARTIFACTORY_KEY'
        """

        when:
        def result = gradleRunner("buildNativeLibrary")
            .withEnvironment(["TEST_NATIVE_BUILD": "host", "TEST_ARTIFACTORY_KEY": "secret"])
            .build()

        then:
        result.task(":buildNativeLibrary").outcome == TaskOutcome.SUCCESS
        puts.get() == 0
    }

    def "does not publish without a credential"() {
        given:
        def puts = new AtomicInteger()
        useRepositoryServing { exchange ->
            if (exchange.requestMethod == "PUT") {
                puts.incrementAndGet()
                exchange.requestBody.bytes
                respond(exchange, 201, new byte[0])
            } else {
                respond(exchange, 404, new byte[0])
            }
        }

        when:
        def result = gradleRunner("buildNativeLibrary").withEnvironment(["TEST_NATIVE_BUILD": "host"]).build()

        then:
        result.task(":buildNativeLibrary").outcome == TaskOutcome.SUCCESS
        puts.get() == 0
    }

    def "always builds when no repository is declared"() {
        given:
        markerOnBuild()

        when:
        def result = gradleRunner("buildNativeLibrary").withEnvironment(["TEST_NATIVE_BUILD": "host"]).build()

        then:
        result.task(":buildNativeLibrary").outcome == TaskOutcome.SUCCESS
        file("build/native-libs/built-marker").exists()
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

    def "a change to the declared build gives the library a different identity"() {
        given:
        def requestedPath = new AtomicReference<String>()
        useRepositoryServing { exchange ->
            requestedPath.set(exchange.requestURI.path)
            respond(exchange, 404, new byte[0])
        }
        gradleRunner("buildNativeLibrary").withEnvironment(["TEST_NATIVE_BUILD": "host"]).build()
        def before = requestedPath.get()

        when:
        buildFile << """
        nativeLibraryBuild.supportedPlatforms = ['${PLATFORM}', 'linux-x64']
        """
        gradleRunner("buildNativeLibrary").withEnvironment(["TEST_NATIVE_BUILD": "host"]).build()

        then:
        requestedPath.get() != before
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
        result.output.contains("Build produced nothing for")
        result.output.contains(PLATFORM)
    }

    def "uses the declared toolchain image unless overridden from the environment"() {
        given:
        buildFile << """
        tasks.register('printToolchainImage') {
          def image = tasks.named('buildNativeLibrary').flatMap { it.toolchainImage }
          doLast { println "toolchainImage=" + image.get() }
        }
        """

        when:
        def declared = gradleRunner("printToolchainImage").withEnvironment(["TEST_NATIVE_BUILD": "docker"]).build()
        def overridden = gradleRunner("printToolchainImage").withEnvironment(
            ["TEST_NATIVE_BUILD": "docker", (NativeLibraryBuildPlugin.TOOLCHAIN_IMAGE_OVERRIDE): "example/toolchain:local"]
        ).build()

        then:
        declared.output.contains("toolchainImage=example/toolchain:1")
        overridden.output.contains("toolchainImage=example/toolchain:local")
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

    def "rejects host mode on a platform the library is not built for, pointing at docker mode"() {
        given:
        buildFile << """
        nativeLibraryBuild.supportedPlatforms = ['some-other-os-x64']
        """

        when:
        def result = gradleRunner("buildNativeLibrary").withEnvironment(["TEST_NATIVE_BUILD": "host"]).buildAndFail()

        then:
        result.output.contains("'host' mode is not available on ${PLATFORM}")
        result.output.contains("some-other-os-x64")
        result.output.contains("'docker' mode")
    }

    def "is restored from the build cache after a clean, without contacting the repository"() {
        given:
        useRepositoryServing { exchange ->
            respond(exchange, 200, publishedArchive())
        }
        gradleRunner("buildNativeLibrary", "--build-cache").build()
        file("build/native-libs").deleteDir()

        when:
        // --offline would fail the task if it downloaded again, so passing proves it did not.
        def result = gradleRunner("buildNativeLibrary", "--build-cache", "--offline").build()

        then:
        result.task(":buildNativeLibrary").outcome == TaskOutcome.FROM_CACHE
        file("build/native-libs/${PLATFORM}/libtest.so").text.trim() == "from-repository"
    }

    def "a host build is not cached, since it depends on the local compiler"() {
        given:
        useRepositoryServing { exchange ->
            respond(exchange, 404, new byte[0])
        }
        gradleRunner("buildNativeLibrary", "--build-cache").withEnvironment(["TEST_NATIVE_BUILD": "host"]).build()
        file("build/native-libs").deleteDir()

        when:
        def result = gradleRunner("buildNativeLibrary", "--build-cache")
            .withEnvironment(["TEST_NATIVE_BUILD": "host"])
            .build()

        then:
        result.task(":buildNativeLibrary").outcome == TaskOutcome.SUCCESS
    }

    def "another project consuming the library receives it, without declaring a task dependency"() {
        given:
        addConsumingProject()

        when:
        def result = gradleRunner(":consumer:consume").withEnvironment(["TEST_NATIVE_BUILD": "host"]).build()

        then:
        result.task(":buildNativeLibrary").outcome == TaskOutcome.SUCCESS
        result.task(":consumer:consume").outcome == TaskOutcome.SUCCESS
        consumedFiles() == ["${PLATFORM}/libtest.so".toString()]
    }

    def "the runtime classpath of a project depending on this one contains the jar, not the native library directory"() {
        given:
        addConsumingProject()

        when:
        gradleRunner(":consumer:copyClasspath").withEnvironment(["TEST_NATIVE_BUILD": "host"]).build()

        then:
        def copied = file("consumer/build/classpath").list() as Set
        copied == ["hello-world.jar", "plain.jar", "plain-external-1.0.jar"] as Set
    }

    def "the published java component does not include the native library directory"() {
        given:
        buildFile << """
        apply plugin: 'java-library'
        apply plugin: 'maven-publish'
        group = 'org.example'
        version = '1.0'
        publishing {
          publications {
            library(MavenPublication) {
              from components.java
            }
          }
        }
        """

        when:
        gradleRunner("generateMetadataFileForLibraryPublication").withEnvironment(["TEST_NATIVE_BUILD": "host"]).build()

        then:
        def variants = new JsonSlurper().parse(file("build/publications/library/module.json")).variants
        variants*.name.contains("runtimeElements")
        variants.every { it.attributes["org.gradle.libraryelements"] != NativeLibraries.LIBRARY_ELEMENTS }
    }

    /**
     * Adds a project, {@code consumer}, that depends on this one as a Java library and selects the native
     * library directory from its runtime classpath. {@code consumer} also depends on a project and on an
     * external module that build no native library, so the tests check that the selection skips them.
     */
    private void addConsumingProject() {
        buildFile << """
        apply plugin: 'java-library'
        """
        settingsFile << """
        include 'consumer'
        include 'plain'
        """
        file("plain/build.gradle") << """
        apply plugin: 'java-library'
        """
        file("repo/org/example/plain-external/1.0/plain-external-1.0.jar") << "not really a jar"
        file("repo/org/example/plain-external/1.0/plain-external-1.0.pom") << """<?xml version="1.0" encoding="UTF-8"?>
        <project xmlns="http://maven.apache.org/POM/4.0.0">
          <modelVersion>4.0.0</modelVersion>
          <groupId>org.example</groupId>
          <artifactId>plain-external</artifactId>
          <version>1.0</version>
        </project>
        """
        file("consumer/build.gradle") << """
        import ${NativeLibraryPathExtension.name}

        apply plugin: 'java'
        repositories {
          maven { url = file('../repo') }
        }
        dependencies {
          implementation project(':')
          implementation project(':plain')
          implementation 'org.example:plain-external:1.0'
        }
        tasks.register('consume', Copy) {
          from NativeLibraryPathExtension.of(project).libraryDirectoriesOf(configurations.runtimeClasspath)
          into layout.buildDirectory.dir('consumed')
        }
        tasks.register('copyClasspath', Copy) {
          from configurations.runtimeClasspath
          into layout.buildDirectory.dir('classpath')
        }
        """
    }

    /**
     * The paths of all the files the {@code consume} task of {@link #addConsumingProject} copied, relative to its
     * destination directory and separated by {@code /}.
     */
    private List<String> consumedFiles() {
        def destination = file("consumer/build/consumed").toPath()
        def files = []
        destination.toFile().eachFileRecurse(FileType.FILES) { files << destination.relativize(it.toPath()).toString().replace(File.separator, "/") }
        return files
    }

    /**
     * Points the build at a loopback repository. Real HTTP rather than a stub, so the task's handling
     * of actual statuses and bodies is what gets exercised.
     */
    private void useRepositoryServing(HttpHandler handler) {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0)
        server.createContext("/", handler)
        server.start()
        String host = server.address.address.hostAddress
        buildFile << """
        nativeLibraryBuild.artifactRepositoryUrl = 'http://${host}:${server.address.port}'
        nativeLibraryBuild.artifactName = 'testlib'
        """
    }

    /** Makes the build command leave a trace, so a test can tell whether it ran at all. */
    private void markerOnBuild() {
        buildFile << """
        nativeLibraryBuild.hostCommand { outputDir -> ['sh', '-c', "mkdir -p \$outputDir.asFile/${PLATFORM} && echo built > \$outputDir.asFile/${PLATFORM}/libtest.so && touch \$outputDir.asFile/built-marker"] }
        """
    }

    /** A published artifact: the platform layout a real one contains, zipped. */
    private static byte[] publishedArchive() {
        def bytes = new ByteArrayOutputStream()
        new ZipOutputStream(bytes).withStream { zip ->
            zip.putNextEntry(new ZipEntry("${PLATFORM}/libtest.so"))
            zip.write("from-repository\n".getBytes("UTF-8"))
            zip.closeEntry()
        }
        return bytes.toByteArray()
    }

    private static void respond(HttpExchange exchange, int status, byte[] body) {
        exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length)
        if (body.length > 0) {
            exchange.responseBody.withStream { it.write(body) }
        }
        exchange.close()
    }
}

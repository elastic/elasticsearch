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
import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.gradle.api.Plugin
import org.gradle.testkit.runner.TaskOutcome

import java.util.concurrent.atomic.AtomicInteger
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
          sourceDir = layout.projectDirectory.dir('native')
          sources = ['src/**', 'Makefile']
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

    def "fails with an actionable message when offline and nothing is published locally"() {
        given:
        useRepositoryServing { exchange ->
            respond(exchange, 404, new byte[0])
        }

        when:
        def result = gradleRunner("buildNativeLibrary", "--offline")
            .withEnvironment(["TEST_NATIVE_BUILD": "host"])
            .buildAndFail()

        then:
        result.output.contains("while offline")
        result.output.contains("build it from source")
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
        consumedLibrary().exists()
    }

    /**
     * Adds a second project that consumes this library. How a consumer reaches the library is the part
     * expected to change — today a named configuration, later carried along by depending on the owning
     * project — so it is wired in one place, leaving the tests above about behaviour only.
     */
    private void addConsumingProject() {
        settingsFile << """
        include 'consumer'
        """
        file("consumer/build.gradle") << """
        import org.gradle.api.artifacts.type.ArtifactTypeDefinition

        // Gradle 9 rejects a configuration that both declares dependencies and is resolved.
        configurations.dependencyScope('nativeLibsDeclared')
        configurations.resolvable('nativeLibs') {
          extendsFrom(configurations.nativeLibsDeclared)
          attributes.attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE)
        }
        dependencies {
          nativeLibsDeclared project(path: ':', configuration: '${NativeLibraryBuildPlugin.ELEMENTS_CONFIGURATION}')
        }
        tasks.register('consume', Copy) {
          from configurations.nativeLibs
          into layout.buildDirectory.dir('consumed')
        }
        """
    }

    /** Where {@link #addConsumingProject} leaves the library it consumed. */
    private File consumedLibrary() {
        file("consumer/build/consumed/${PLATFORM}/libtest.so")
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

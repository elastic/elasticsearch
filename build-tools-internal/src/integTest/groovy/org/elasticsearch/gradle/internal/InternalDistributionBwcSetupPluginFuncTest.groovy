/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal

import spock.lang.Unroll

import com.github.tomakehurst.wiremock.WireMockServer
import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream
import org.elasticsearch.gradle.fixtures.AbstractGitAwareGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse
import static com.github.tomakehurst.wiremock.client.WireMock.get
import static com.github.tomakehurst.wiremock.client.WireMock.head
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo

class InternalDistributionBwcSetupPluginFuncTest extends AbstractGitAwareGradleFuncTest {

    def setup() {
        // Cannot serialize BwcSetupExtension containing project object
        configurationCacheCompatible = false
        internalBuild()
        buildFile << """
            apply plugin: 'elasticsearch.internal-distribution-bwc-setup'
        """
        execute("git branch origin/8.x", file("cloned"))
        execute("git branch origin/8.3", file("cloned"))
        execute("git branch origin/8.2", file("cloned"))
        execute("git branch origin/8.1", file("cloned"))
        execute("git branch origin/7.16", file("cloned"))
    }

    def "builds distribution from branches via archives extractedAssemble"() {
        given:
        buildFile.text = ""
        internalBuild()
        buildFile << """
            apply plugin: 'elasticsearch.internal-distribution-bwc-setup'
        """
        when:
        def result = gradleRunner(":distribution:bwc:${bwcProject}:buildBwcDarwinTar",
                ":distribution:bwc:${bwcProject}:buildBwcDarwinTar",
                "-DtestRemoteRepo=" + remoteGitRepo,
                "-Dbwc.remote=origin",
                "-Dbwc.dist.version=${bwcDistVersion}-SNAPSHOT")
                .build()
        then:
        result.task(":distribution:bwc:${bwcProject}:buildBwcDarwinTar").outcome == TaskOutcome.SUCCESS

        and: "assemble task triggered"
        assertOutputContains(result.output, "[$bwcDistVersion] > Task :distribution:archives:darwin-tar:${expectedAssembleTaskName}")

        where:
        bwcDistVersion | bwcProject | expectedAssembleTaskName
        "8.4.0"        | "major1"   | "extractedAssemble"
        "8.3.0"        | "major2"   | "extractedAssemble"
        "8.2.1"        | "major3"   | "extractedAssemble"
        "8.1.3"        | "major4"   | "extractedAssemble"
    }

    @Unroll
    def "supports #platform aarch distributions"() {
        when:
        def result = gradleRunner(":distribution:bwc:major1:buildBwc${platform.capitalize()}Aarch64Tar",
                "-DtestRemoteRepo=" + remoteGitRepo,
                "-Dbwc.remote=origin",
                "-Dbwc.dist.version=${bwcDistVersion}-SNAPSHOT")
                .build()
        then:
        result.task(":distribution:bwc:major1:buildBwc${platform.capitalize()}Aarch64Tar").outcome == TaskOutcome.SUCCESS

        and: "assemble tasks triggered"
        assertOutputContains(result.output, "[$bwcDistVersion] > Task :distribution:archives:${platform}-aarch64-tar:extractedAssemble")

        where:
        bwcDistVersion | platform
        "8.4.0"       | "darwin"
        "8.4.0"       | "linux"
    }

    def "downloads distribution from DRA snapshot when hash matches"() {
        given:
        def bwcVersion = "8.4.0"
        def bwcBranch = "8.x"
        def shortHash = "abc12345"
        def buildId = "${bwcVersion}-${shortHash}"
        def manifestPath = "/elasticsearch/${buildId}/manifest-${bwcVersion}-SNAPSHOT.json"
        def artifactPath = "/elasticsearch/${buildId}/downloads/elasticsearch/" +
            "elasticsearch-${bwcVersion}-SNAPSHOT-darwin-x86_64.tar.gz"

        def wireMock = new WireMockServer(0)
        wireMock.start()
        try {
            wireMock.stubFor(head(urlEqualTo(manifestPath)).willReturn(aResponse().withStatus(200)))
            wireMock.stubFor(get(urlEqualTo(manifestPath))
                .willReturn(aResponse().withStatus(200).withBody("{}")))
            wireMock.stubFor(head(urlEqualTo(artifactPath)).willReturn(aResponse().withStatus(200)))
            wireMock.stubFor(get(urlEqualTo(artifactPath))
                .willReturn(aResponse().withStatus(200)
                    .withBody(minimalElasticsearchTarGz("${bwcVersion}-SNAPSHOT"))))

            buildFile << """
                allprojects { p ->
                    p.repositories.all { repo ->
                        if (repo.name.startsWith("dra-bwc-elasticsearch-")) {
                            repo.setUrl('${wireMock.baseUrl()}')
                            allowInsecureProtocol = true
                        }
                    }
                }
            """

            when:
            def result = gradleRunner(
                ":distribution:bwc:major1:buildBwcDarwinTar",
                "-DtestRemoteRepo=" + remoteGitRepo,
                "-Dbwc.remote=origin",
                "-Dtests.bwc.dra.enabled=true",
                "-Dtests.bwc.dra.hash.${bwcBranch}=${shortHash}",
                "-Dtests.bwc.dra.base.url=${wireMock.baseUrl()}"
            ).build()

            then: "DRA Copy task succeeds"
            result.task(":distribution:bwc:major1:buildBwcDarwinTar").outcome == TaskOutcome.SUCCESS

            and: "no nested Gradle build was triggered"
            result.output.contains("extractedAssemble") == false

            and: "the DRA log message names the snapshot build ID"
            result.output.contains("DRA snapshot ${buildId}")

            and: "the distribution directory was created at the expected path"
            file("cloned/distribution/bwc/major1/build/bwc/checkout-${bwcBranch}/" +
                "distribution/archives/darwin-tar/build/install/" +
                "elasticsearch-${bwcVersion}-SNAPSHOT").exists()
        } finally {
            wireMock.stop()
        }
    }

    def "resolveByLatest falls back to origin remote when configured remote ref is absent (CI scenario)"() {
        given:
        def bwcVersion = "8.4.0"
        def bwcBranch = "8.x"
        def fullHash = execute("git rev-parse HEAD", file("cloned")).trim()
        def shortHash = fullHash.substring(0, 8)
        def buildId = "${bwcVersion}-${shortHash}"

        // Write the ref under 'origin' only — simulates a CI checkout where the
        // configured BWC remote ('elastic') is absent but 'origin' IS elastic/elasticsearch.
        def originRefFile = file("cloned/.git/refs/remotes/origin/${bwcBranch}")
        originRefFile.text = fullHash + "\n"
        // Ensure there is no 'elastic' remote ref so the fallback path is exercised.
        def elasticRefFile = new File(file("cloned/.git/refs/remotes").parentFile, "remotes/elastic/${bwcBranch}")
        elasticRefFile.delete()

        def latestPath = "/elasticsearch/latest/${bwcBranch}.json"
        def manifestPath = "/elasticsearch/${buildId}/manifest-${bwcVersion}-SNAPSHOT.json"
        def artifactPath = "/elasticsearch/${buildId}/downloads/elasticsearch/" +
            "elasticsearch-${bwcVersion}-SNAPSHOT-darwin-x86_64.tar.gz"

        def wireMock = new WireMockServer(0)
        wireMock.start()
        try {
            wireMock.stubFor(get(urlEqualTo(latestPath))
                .willReturn(aResponse().withStatus(200)
                    .withBody("""{"build_id": "${buildId}"}""")))
            wireMock.stubFor(head(urlEqualTo(manifestPath)).willReturn(aResponse().withStatus(200)))
            wireMock.stubFor(get(urlEqualTo(manifestPath))
                .willReturn(aResponse().withStatus(200)
                    .withBody("""{"projects": {"elasticsearch": {"commit_hash": "${fullHash}"}}}""")))
            wireMock.stubFor(head(urlEqualTo(artifactPath)).willReturn(aResponse().withStatus(200)))
            wireMock.stubFor(get(urlEqualTo(artifactPath))
                .willReturn(aResponse().withStatus(200)
                    .withBody(minimalElasticsearchTarGz("${bwcVersion}-SNAPSHOT"))))

            buildFile << """
                allprojects { p ->
                    p.repositories.all { repo ->
                        if (repo.name.startsWith("dra-bwc-elasticsearch-")) {
                            repo.setUrl('${wireMock.baseUrl()}')
                            allowInsecureProtocol = true
                        }
                    }
                }
            """

            when:
            // bwc.remote=elastic (the default) — no elastic remote ref exists, only origin
            def result = gradleRunner(
                ":distribution:bwc:major1:buildBwcDarwinTar",
                "-DtestRemoteRepo=" + remoteGitRepo,
                "-Dbwc.remote=elastic",
                "-Dtests.bwc.dra.enabled=true",
                "-Dtests.bwc.dra.base.url=${wireMock.baseUrl()}"
            ).build()

            then: "DRA Copy task succeeds via origin fallback"
            result.task(":distribution:bwc:major1:buildBwcDarwinTar").outcome == TaskOutcome.SUCCESS

            and: "no nested Gradle build was triggered"
            result.output.contains("extractedAssemble") == false

            and: "the DRA log message names the snapshot build ID"
            result.output.contains("DRA snapshot ${buildId}")
        } finally {
            wireMock.stop()
        }
    }

    def "downloads JDBC jar from DRA snapshot when hash matches"() {
        given:
        def bwcVersion = "8.4.0"
        def bwcBranch = "8.x"
        def shortHash = "abc12345"
        def buildId = "${bwcVersion}-${shortHash}"
        def manifestPath = "/elasticsearch/${buildId}/manifest-${bwcVersion}-SNAPSHOT.json"
        def jdbcArtifactPath = "/elasticsearch/${buildId}/downloads/elasticsearch/" +
            "x-pack-sql-jdbc-${bwcVersion}-SNAPSHOT.jar"

        def wireMock = new WireMockServer(0)
        wireMock.start()
        try {
            wireMock.stubFor(head(urlEqualTo(manifestPath)).willReturn(aResponse().withStatus(200)))
            wireMock.stubFor(get(urlEqualTo(manifestPath))
                .willReturn(aResponse().withStatus(200).withBody("{}")))
            wireMock.stubFor(head(urlEqualTo(jdbcArtifactPath)).willReturn(aResponse().withStatus(200)))
            wireMock.stubFor(get(urlEqualTo(jdbcArtifactPath))
                .willReturn(aResponse().withStatus(200).withBody("fake-jdbc-content".bytes)))

            buildFile << """
                allprojects { p ->
                    p.repositories.all { repo ->
                        if (repo.name.startsWith("dra-bwc-elasticsearch-")) {
                            repo.setUrl('${wireMock.baseUrl()}')
                            allowInsecureProtocol = true
                        }
                    }
                }
            """

            when:
            def result = gradleRunner(
                ":distribution:bwc:major1:buildBwcJdbc",
                "-DtestRemoteRepo=" + remoteGitRepo,
                "-Dbwc.remote=origin",
                "-Dtests.bwc.dra.enabled=true",
                "-Dtests.bwc.dra.hash.${bwcBranch}=${shortHash}",
                "-Dtests.bwc.dra.base.url=${wireMock.baseUrl()}"
            ).build()

            then: "DRA Copy task succeeds"
            result.task(":distribution:bwc:major1:buildBwcJdbc").outcome == TaskOutcome.SUCCESS

            and: "no nested Gradle build was triggered"
            result.output.contains("> Task :x-pack:plugin:sql:jdbc:assemble") == false

            and: "the DRA log message names the snapshot build ID"
            result.output.contains("DRA snapshot ${buildId}")

            and: "the JDBC jar was created at the expected path"
            file("cloned/distribution/bwc/major1/build/bwc/checkout-${bwcBranch}/" +
                "x-pack/plugin/sql/jdbc/build/distributions/" +
                "x-pack-sql-jdbc-${bwcVersion}-SNAPSHOT.jar").exists()
        } finally {
            wireMock.stop()
        }
    }

    def "falls back to local gradle build when DRA is disabled"() {
        when:
        def result = gradleRunner(":distribution:bwc:major1:buildBwcDarwinTar",
            "-DtestRemoteRepo=" + remoteGitRepo,
            "-Dbwc.remote=origin",
            "-Dbwc.dist.version=8.4.0-SNAPSHOT")
            .build()
        then:
        result.task(":distribution:bwc:major1:buildBwcDarwinTar").outcome == TaskOutcome.SUCCESS

        and: "local nested build was triggered"
        assertOutputContains(result.output, "[8.4.0] > Task :distribution:archives:darwin-tar:extractedAssemble")
    }

    def "falls back to local gradle build when DRA manifest returns 404"() {
        given:
        def bwcVersion = "8.4.0"
        def shortHash = "notexist"
        def buildId = "${bwcVersion}-${shortHash}"
        def manifestPath = "/elasticsearch/${buildId}/manifest-${bwcVersion}-SNAPSHOT.json"

        def wireMock = new WireMockServer(0)
        wireMock.start()
        try {
            wireMock.stubFor(head(urlEqualTo(manifestPath)).willReturn(aResponse().withStatus(404)))
            wireMock.stubFor(get(urlEqualTo(manifestPath)).willReturn(aResponse().withStatus(404)))

            when:
            def result = gradleRunner(
                ":distribution:bwc:major1:buildBwcDarwinTar",
                "-DtestRemoteRepo=" + remoteGitRepo,
                "-Dbwc.remote=origin",
                "-Dbwc.dist.version=8.4.0-SNAPSHOT",
                "-Dtests.bwc.dra.enabled=true",
                "-Dtests.bwc.dra.hash.8.x=${shortHash}",
                "-Dtests.bwc.dra.base.url=${wireMock.baseUrl()}"
            ).build()

            then: "task succeeds via local build fallback"
            result.task(":distribution:bwc:major1:buildBwcDarwinTar").outcome == TaskOutcome.SUCCESS

            and: "local nested build was used, not DRA"
            assertOutputContains(result.output, "[8.4.0] > Task :distribution:archives:darwin-tar:extractedAssemble")
        } finally {
            wireMock.stop()
        }
    }

    def "downloads distribution via resolveByLatest when remote tracking ref matches DRA manifest commit"() {
        given:
        def bwcVersion = "8.4.0"
        def bwcBranch = "8.x"
        def fullHash = execute("git rev-parse HEAD", file("cloned")).trim()
        def shortHash = fullHash.substring(0, 8)
        def buildId = "${bwcVersion}-${shortHash}"

        // Write a remote tracking ref so remoteRefRevision() can resolve it without network access.
        def remoteRefFile = file("cloned/.git/refs/remotes/origin/${bwcBranch}")
        remoteRefFile.text = fullHash + "\n"

        def latestPath = "/elasticsearch/latest/${bwcBranch}.json"
        def manifestPath = "/elasticsearch/${buildId}/manifest-${bwcVersion}-SNAPSHOT.json"
        def artifactPath = "/elasticsearch/${buildId}/downloads/elasticsearch/" +
            "elasticsearch-${bwcVersion}-SNAPSHOT-darwin-x86_64.tar.gz"

        def wireMock = new WireMockServer(0)
        wireMock.start()
        try {
            wireMock.stubFor(get(urlEqualTo(latestPath))
                .willReturn(aResponse().withStatus(200)
                    .withBody("""{"build_id": "${buildId}"}""")))
            wireMock.stubFor(head(urlEqualTo(manifestPath)).willReturn(aResponse().withStatus(200)))
            wireMock.stubFor(get(urlEqualTo(manifestPath))
                .willReturn(aResponse().withStatus(200)
                    .withBody("""{"projects": {"elasticsearch": {"commit_hash": "${fullHash}"}}}""")))
            wireMock.stubFor(head(urlEqualTo(artifactPath)).willReturn(aResponse().withStatus(200)))
            wireMock.stubFor(get(urlEqualTo(artifactPath))
                .willReturn(aResponse().withStatus(200)
                    .withBody(minimalElasticsearchTarGz("${bwcVersion}-SNAPSHOT"))))

            buildFile << """
                allprojects { p ->
                    p.repositories.all { repo ->
                        if (repo.name.startsWith("dra-bwc-elasticsearch-")) {
                            repo.setUrl('${wireMock.baseUrl()}')
                            allowInsecureProtocol = true
                        }
                    }
                }
            """

            when:
            def result = gradleRunner(
                ":distribution:bwc:major1:buildBwcDarwinTar",
                "-DtestRemoteRepo=" + remoteGitRepo,
                "-Dbwc.remote=origin",
                "-Dtests.bwc.dra.enabled=true",
                "-Dtests.bwc.dra.base.url=${wireMock.baseUrl()}"
            ).build()

            then: "DRA Copy task succeeds"
            result.task(":distribution:bwc:major1:buildBwcDarwinTar").outcome == TaskOutcome.SUCCESS

            and: "no nested Gradle build was triggered"
            result.output.contains("extractedAssemble") == false

            and: "the DRA log message names the snapshot build ID"
            result.output.contains("DRA snapshot ${buildId}")
        } finally {
            wireMock.stop()
        }
    }

    def "falls back to local build when DRA manifest commit does not match remote tracking ref"() {
        given:
        def bwcBranch = "8.x"
        def fullHash = execute("git rev-parse HEAD", file("cloned")).trim()

        // Write a remote tracking ref pointing to the real local commit.
        def remoteRefFile = file("cloned/.git/refs/remotes/origin/${bwcBranch}")
        remoteRefFile.text = fullHash + "\n"

        def bwcVersion = "8.4.0"
        def shortHash = "deadbeef"
        def buildId = "${bwcVersion}-${shortHash}"
        // DRA manifest reports a different commit — mismatch triggers local build fallback.
        def differentCommit = "0" * 40

        def latestPath = "/elasticsearch/latest/${bwcBranch}.json"
        def manifestPath = "/elasticsearch/${buildId}/manifest-${bwcVersion}-SNAPSHOT.json"

        def wireMock = new WireMockServer(0)
        wireMock.start()
        try {
            wireMock.stubFor(get(urlEqualTo(latestPath))
                .willReturn(aResponse().withStatus(200)
                    .withBody("""{"build_id": "${buildId}"}""")))
            wireMock.stubFor(head(urlEqualTo(manifestPath)).willReturn(aResponse().withStatus(200)))
            wireMock.stubFor(get(urlEqualTo(manifestPath))
                .willReturn(aResponse().withStatus(200)
                    .withBody("""{"projects": {"elasticsearch": {"commit_hash": "${differentCommit}"}}}""")))

            when:
            def result = gradleRunner(
                ":distribution:bwc:major1:buildBwcDarwinTar",
                "-DtestRemoteRepo=" + remoteGitRepo,
                "-Dbwc.remote=origin",
                "-Dbwc.dist.version=8.4.0-SNAPSHOT",
                "-Dtests.bwc.dra.enabled=true",
                "-Dtests.bwc.dra.base.url=${wireMock.baseUrl()}"
            ).build()

            then: "task succeeds via local build fallback"
            result.task(":distribution:bwc:major1:buildBwcDarwinTar").outcome == TaskOutcome.SUCCESS

            and: "local nested build was used, not DRA"
            assertOutputContains(result.output, "[8.4.0] > Task :distribution:archives:darwin-tar:extractedAssemble")

            and: "log explains why DRA was not used"
            result.output.contains("DRA snapshot commit did not match")
        } finally {
            wireMock.stop()
        }
    }

    def "bwc expanded distribution folder can be resolved as bwc project artifact"() {
        setup:
        buildFile << """

        configurations {
            expandedDist
        }

        dependencies {
            expandedDist project(path: ":distribution:bwc:major1", configuration:"expanded-darwin-tar")
        }

        tasks.register("resolveExpandedDistribution") {
            inputs.files(configurations.expandedDist)
            doLast {
                configurations.expandedDist.files.each {
                    println "expandedRootPath " + (it.absolutePath - project.rootDir.absolutePath)
                    it.eachFile { nested ->
                        println "nested folder " + (nested.absolutePath - project.rootDir.absolutePath)
                    }
                }
            }
        }
        """
        when:
        def result = gradleRunner(":resolveExpandedDistribution",
                "-DtestRemoteRepo=" + remoteGitRepo,
                "-Dbwc.remote=origin")
                .build()
        then:
        result.task(":resolveExpandedDistribution").outcome == TaskOutcome.SUCCESS
        result.task(":distribution:bwc:major1:buildBwcDarwinTar").outcome == TaskOutcome.SUCCESS
        and: "assemble task triggered"
        result.output.contains("[8.4.0] > Task :distribution:archives:darwin-tar:extractedAssemble")
        result.output.contains("expandedRootPath /distribution/bwc/major1/build/bwc/checkout-8.x/" +
                        "distribution/archives/darwin-tar/build/install")
        result.output.contains("nested folder /distribution/bwc/major1/build/bwc/checkout-8.x/" +
                        "distribution/archives/darwin-tar/build/install/elasticsearch-8.4.0-SNAPSHOT")
    }

    /**
     * Creates a minimal tar.gz that {@link org.elasticsearch.gradle.transform.SymbolicLinkPreservingUntarTransform}
     * can extract.  The archive contains a single top-level directory {@code elasticsearch-{version}/} with one
     * executable file inside, matching the real distribution layout well enough to satisfy the
     * {@code expectedOutputFile.exists()} check in the DRA Copy task.
     */
    private static byte[] minimalElasticsearchTarGz(String version) {
        def baos = new ByteArrayOutputStream()
        def gzos = new GzipCompressorOutputStream(baos)
        def taos = new TarArchiveOutputStream(gzos)
        taos.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX)

        def addDir = { String name ->
            def entry = new TarArchiveEntry(name)
            entry.setMode(0755)
            taos.putArchiveEntry(entry)
            taos.closeArchiveEntry()
        }
        def addFile = { String name, byte[] content ->
            def entry = new TarArchiveEntry(name)
            entry.setSize(content.length)
            entry.setMode(0755)
            taos.putArchiveEntry(entry)
            taos.write(content)
            taos.closeArchiveEntry()
        }

        addDir("elasticsearch-${version}/")
        addDir("elasticsearch-${version}/bin/")
        addFile("elasticsearch-${version}/bin/elasticsearch", "#!/bin/sh\n".bytes)

        taos.finish()
        gzos.close()
        return baos.toByteArray()
    }
}

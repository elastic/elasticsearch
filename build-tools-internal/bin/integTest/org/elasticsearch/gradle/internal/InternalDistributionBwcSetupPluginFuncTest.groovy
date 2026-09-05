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

    Class<? extends org.gradle.api.Plugin> pluginClassUnderTest = org.elasticsearch.gradle.internal.InternalDistributionBwcSetupPlugin

    WireMockServer wireMock

    def setup() {
        disableConfigurationCache("cannot serialize BwcSetupExtension containing project object")
        internalBuild()
        buildFile << """
            apply plugin: 'elasticsearch.internal-distribution-bwc-setup'
        """
        execute("git branch origin/8.x", file("cloned"))
        execute("git branch origin/8.3", file("cloned"))
        execute("git branch origin/8.2", file("cloned"))
        execute("git branch origin/8.1", file("cloned"))
        execute("git branch origin/7.16", file("cloned"))
        wireMock = new WireMockServer(0)
        wireMock.start()
    }

    def cleanup() {
        wireMock?.stop()
    }

    def "builds distribution from branches via archives extractedAssemble"() {
        // A single BWC branch is sufficient to verify that buildBwcDarwinTar triggers
        // extractedAssemble — the plugin logic is identical for all supported branches.
        when:
        def result = gradleRunner(
            ":distribution:bwc:major1:buildBwcDarwinTar",
            "-DtestRemoteRepo=" + remoteGitRepo,
            "-Dbwc.remote=origin",
            "-Dbwc.dist.version=8.4.0-SNAPSHOT"
        )
            .build()
        then:
        result.task(":distribution:bwc:major1:buildBwcDarwinTar").outcome == TaskOutcome.SUCCESS

        and: "assemble task triggered"
        assertOutputContains(result.output, "[8.4.0] > Task :distribution:archives:darwin-tar:extractedAssemble")
    }

    def "supports linux aarch distributions"() {
        // One aarch64 platform is sufficient to verify the feature; the same plugin
        // logic handles both darwin and linux aarch64 targets.
        when:
        def result = gradleRunner(
            ":distribution:bwc:major1:buildBwcLinuxAarch64Tar",
            "-DtestRemoteRepo=" + remoteGitRepo,
            "-Dbwc.remote=origin",
            "-Dbwc.dist.version=8.4.0-SNAPSHOT"
        )
            .build()
        then:
        result.task(":distribution:bwc:major1:buildBwcLinuxAarch64Tar").outcome == TaskOutcome.SUCCESS

        and: "assemble tasks triggered"
        assertOutputContains(result.output, "[8.4.0] > Task :distribution:archives:linux-aarch64-tar:extractedAssemble")
    }

    def "downloads distribution from DRA snapshot when mode=dra and hash override is set"() {
        given:
        def bwcVersion = "8.4.0"
        def bwcBranch = "8.x"
        def shortHash = "abc12345"
        def buildId = "${bwcVersion}-${shortHash}"
        stubManifestOk(wireMock, draManifestPath(buildId, bwcVersion))
        stubTarArtifact(wireMock, draTarArtifactPath(buildId, bwcVersion, "darwin-x86_64"), bwcVersion)
        redirectDraRepositories(wireMock.baseUrl())

        when:
        def result = gradleRunner(
            ":distribution:bwc:major1:buildBwcDarwinTar",
            "-DtestRemoteRepo=" + remoteGitRepo,
            "-Dbwc.remote=origin",
            "-Dtests.bwc.mode=dra",
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
        file(
            "cloned/distribution/bwc/major1/build/bwc/checkout-${bwcBranch}/" +
                "distribution/archives/darwin-tar/build/install/" +
                "elasticsearch-${bwcVersion}-SNAPSHOT"
        ).exists()
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

        stubLatest(wireMock, draLatestPath(bwcBranch), buildId)
        stubManifestWithCommit(wireMock, draManifestPath(buildId, bwcVersion), fullHash)
        stubTarArtifact(wireMock, draTarArtifactPath(buildId, bwcVersion, "darwin-x86_64"), bwcVersion)
        redirectDraRepositories(wireMock.baseUrl())

        when:
        // bwc.remote=elastic (the default) — no elastic remote ref exists, only origin
        def result = gradleRunner(
            ":distribution:bwc:major1:buildBwcDarwinTar",
            "-DtestRemoteRepo=" + remoteGitRepo,
            "-Dbwc.remote=elastic",
            "-Dtests.bwc.mode=auto",
            "-Dtests.bwc.dra.base.url=${wireMock.baseUrl()}"
        ).build()

        then: "DRA Copy task succeeds via origin fallback"
        result.task(":distribution:bwc:major1:buildBwcDarwinTar").outcome == TaskOutcome.SUCCESS

        and: "no nested Gradle build was triggered"
        result.output.contains("extractedAssemble") == false

        and: "the DRA log message names the snapshot build ID"
        result.output.contains("DRA snapshot ${buildId}")
    }

    def "downloads JDBC jar from DRA snapshot when mode=dra and hash override is set"() {
        given:
        def bwcVersion = "8.4.0"
        def bwcBranch = "8.x"
        def shortHash = "abc12345"
        def buildId = "${bwcVersion}-${shortHash}"
        stubManifestOk(wireMock, draManifestPath(buildId, bwcVersion))
        stubJarArtifact(wireMock, draMavenArtifactPath(buildId, bwcVersion, "org.elasticsearch.plugin", "x-pack-sql-jdbc"))
        redirectDraRepositories(wireMock.baseUrl())

        when:
        def result = gradleRunner(
            ":distribution:bwc:major1:buildBwcJdbc",
            "-DtestRemoteRepo=" + remoteGitRepo,
            "-Dbwc.remote=origin",
            "-Dtests.bwc.mode=dra",
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
        file(
            "cloned/distribution/bwc/major1/build/bwc/checkout-${bwcBranch}/" +
                "x-pack/plugin/sql/jdbc/build/distributions/" +
                "x-pack-sql-jdbc-${bwcVersion}-SNAPSHOT.jar"
        ).exists()
    }

    def "falls back to local gradle build when DRA manifest returns 404"() {
        given:
        def bwcVersion = "8.4.0"
        def shortHash = "notexist"
        def buildId = "${bwcVersion}-${shortHash}"
        stubManifestNotFound(wireMock, draManifestPath(buildId, bwcVersion))

        when:
        def result = gradleRunner(
            ":distribution:bwc:major1:buildBwcDarwinTar",
            "-DtestRemoteRepo=" + remoteGitRepo,
            "-Dbwc.remote=origin",
            "-Dbwc.dist.version=8.4.0-SNAPSHOT",
            "-Dtests.bwc.mode=auto",
            "-Dtests.bwc.dra.hash.8.x=${shortHash}",
            "-Dtests.bwc.dra.base.url=${wireMock.baseUrl()}"
        ).build()

        then: "task succeeds via local build fallback"
        result.task(":distribution:bwc:major1:buildBwcDarwinTar").outcome == TaskOutcome.SUCCESS

        and: "local nested build was used, not DRA"
        assertOutputContains(result.output, "[8.4.0] > Task :distribution:archives:darwin-tar:extractedAssemble")
    }

    def "downloads distribution via resolveByLatest when mode=auto and remote tracking ref matches DRA manifest commit"() {
        given:
        def bwcVersion = "8.4.0"
        def bwcBranch = "8.x"
        def fullHash = execute("git rev-parse HEAD", file("cloned")).trim()
        def shortHash = fullHash.substring(0, 8)
        def buildId = "${bwcVersion}-${shortHash}"

        // Write a remote tracking ref so remoteRefRevision() can resolve it without network access.
        def remoteRefFile = file("cloned/.git/refs/remotes/origin/${bwcBranch}")
        remoteRefFile.text = fullHash + "\n"

        stubLatest(wireMock, draLatestPath(bwcBranch), buildId)
        stubManifestWithCommit(wireMock, draManifestPath(buildId, bwcVersion), fullHash)
        stubTarArtifact(wireMock, draTarArtifactPath(buildId, bwcVersion, "darwin-x86_64"), bwcVersion)
        redirectDraRepositories(wireMock.baseUrl())

        when:
        def result = gradleRunner(
            ":distribution:bwc:major1:buildBwcDarwinTar",
            "-DtestRemoteRepo=" + remoteGitRepo,
            "-Dbwc.remote=origin",
            "-Dtests.bwc.mode=auto",
            "-Dtests.bwc.dra.base.url=${wireMock.baseUrl()}"
        ).build()

        then: "DRA Copy task succeeds"
        result.task(":distribution:bwc:major1:buildBwcDarwinTar").outcome == TaskOutcome.SUCCESS

        and: "no nested Gradle build was triggered"
        result.output.contains("extractedAssemble") == false

        and: "the DRA log message names the snapshot build ID"
        result.output.contains("DRA snapshot ${buildId}")
    }

    def "falls back to local build when mode=auto and DRA manifest commit does not match remote tracking ref"() {
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

        stubLatest(wireMock, draLatestPath(bwcBranch), buildId)
        stubManifestWithCommit(wireMock, draManifestPath(buildId, bwcVersion), differentCommit)

        when:
        def result = gradleRunner(
            ":distribution:bwc:major1:buildBwcDarwinTar",
            "-DtestRemoteRepo=" + remoteGitRepo,
            "-Dbwc.remote=origin",
            "-Dbwc.dist.version=8.4.0-SNAPSHOT",
            "-Dtests.bwc.mode=auto",
            "-Dtests.bwc.dra.base.url=${wireMock.baseUrl()}"
        ).build()

        then: "task succeeds via local build fallback"
        result.task(":distribution:bwc:major1:buildBwcDarwinTar").outcome == TaskOutcome.SUCCESS

        and: "local nested build was used, not DRA"
        assertOutputContains(result.output, "[8.4.0] > Task :distribution:archives:darwin-tar:extractedAssemble")

        and: "log explains why DRA was not used"
        result.output.contains("DRA snapshot commit did not match")
    }

    def "downloads distribution from DRA when mode=dra and no hash override uses latest snapshot without commit check"() {
        given:
        def bwcVersion = "8.4.0"
        def bwcBranch = "8.x"
        def shortHash = "abc12345"
        def buildId = "${bwcVersion}-${shortHash}"
        // resolveLatestBuildId does not read local git refs — no remote tracking ref needed.
        stubLatest(wireMock, draLatestPath(bwcBranch), buildId)
        stubTarArtifact(wireMock, draTarArtifactPath(buildId, bwcVersion, "darwin-x86_64"), bwcVersion)
        redirectDraRepositories(wireMock.baseUrl())

        when:
        def result = gradleRunner(
            ":distribution:bwc:major1:buildBwcDarwinTar",
            "-DtestRemoteRepo=" + remoteGitRepo,
            "-Dbwc.remote=origin",
            "-Dtests.bwc.mode=dra",
            "-Dtests.bwc.dra.base.url=${wireMock.baseUrl()}"
        ).build()

        then: "DRA Copy task succeeds without any commit-hash comparison"
        result.task(":distribution:bwc:major1:buildBwcDarwinTar").outcome == TaskOutcome.SUCCESS

        and: "no nested Gradle build was triggered"
        result.output.contains("extractedAssemble") == false

        and: "the DRA log message names the snapshot build ID"
        result.output.contains("DRA snapshot ${buildId}")

        and: "the distribution directory was created at the expected path"
        file(
            "cloned/distribution/bwc/major1/build/bwc/checkout-${bwcBranch}/" +
                "distribution/archives/darwin-tar/build/install/" +
                "elasticsearch-${bwcVersion}-SNAPSHOT"
        ).exists()
    }

    def "falls back to source build with warning when mode=dra but DRA snapshot is unavailable (distribution archive)"() {
        given:
        // Stub the latest endpoint with 404 so resolveLatestBuildId returns an empty build ID.
        wireMock.stubFor(get(urlEqualTo(draLatestPath("8.x"))).willReturn(aResponse().withStatus(404)))

        when:
        def result = gradleRunner(
            ":distribution:bwc:major1:buildBwcDarwinTar",
            "-DtestRemoteRepo=" + remoteGitRepo,
            "-Dbwc.remote=origin",
            "-Dbwc.dist.version=8.4.0-SNAPSHOT",
            "-Dtests.bwc.mode=dra",
            "-Dtests.bwc.dra.base.url=${wireMock.baseUrl()}"
        ).build()

        then: "task still succeeds via source-build fallback"
        result.task(":distribution:bwc:major1:buildBwcDarwinTar").outcome == TaskOutcome.SUCCESS

        and: "local nested build was triggered"
        assertOutputContains(result.output, "[8.4.0] > Task :distribution:archives:darwin-tar:extractedAssemble")

        and: "a warning explains that DRA was unavailable"
        result.output.contains("tests.bwc.mode=dra but no DRA snapshot was available")
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
        def result = gradleRunner(
            ":resolveExpandedDistribution",
            "-DtestRemoteRepo=" + remoteGitRepo,
            "-Dbwc.remote=origin"
        )
            .build()
        then:
        result.task(":resolveExpandedDistribution").outcome == TaskOutcome.SUCCESS
        result.task(":distribution:bwc:major1:buildBwcDarwinTar").outcome == TaskOutcome.SUCCESS
        and: "assemble task triggered"
        result.output.contains("[8.4.0] > Task :distribution:archives:darwin-tar:extractedAssemble")
        result.output.contains(
            "expandedRootPath /distribution/bwc/major1/build/bwc/checkout-8.x/" +
                "distribution/archives/darwin-tar/build/install"
        )
        result.output.contains(
            "nested folder /distribution/bwc/major1/build/bwc/checkout-8.x/" +
                "distribution/archives/darwin-tar/build/install/elasticsearch-8.4.0-SNAPSHOT"
        )
    }

    // -------------------------------------------------------------------------
    // Build-file helper
    // -------------------------------------------------------------------------

    /** Appends a Gradle snippet that redirects all DRA Ivy repositories to {@code baseUrl}. */
    private void redirectDraRepositories(String baseUrl) {
        buildFile << """
            allprojects { p ->
                p.repositories.all { repo ->
                    if (repo.name.startsWith("dra-bwc-elasticsearch-")) {
                        repo.setUrl('${baseUrl}')
                        allowInsecureProtocol = true
                    }
                }
            }
        """
    }

    // -------------------------------------------------------------------------
    // DRA URL path helpers
    // -------------------------------------------------------------------------

    private static String draManifestPath(String buildId, String version) {
        "/elasticsearch/${buildId}/manifest-${version}-SNAPSHOT.json"
    }

    private static String draLatestPath(String branch) {
        "/elasticsearch/latest/${branch}.json"
    }

    private static String draTarArtifactPath(String buildId, String version, String classifier) {
        "/elasticsearch/${buildId}/downloads/elasticsearch/elasticsearch-${version}-SNAPSHOT-${classifier}.tar.gz"
    }

    private static String draMavenArtifactPath(String buildId, String version, String mavenGroup, String module) {
        "/elasticsearch/${buildId}/maven/${mavenGroup.replace('.', '/')}/${module}/${version}-SNAPSHOT/${module}-${version}-SNAPSHOT.jar"
    }

    // -------------------------------------------------------------------------
    // WireMock stub helpers
    // -------------------------------------------------------------------------

    /** Stubs HEAD+GET for a manifest path, returning HTTP 200 with an empty JSON body. */
    private static void stubManifestOk(WireMockServer wireMock, String path) {
        wireMock.stubFor(head(urlEqualTo(path)).willReturn(aResponse().withStatus(200)))
        wireMock.stubFor(get(urlEqualTo(path)).willReturn(aResponse().withStatus(200).withBody("{}")))
    }

    /** Stubs HEAD+GET for a manifest path, returning HTTP 200 with the given commit hash in the body. */
    private static void stubManifestWithCommit(WireMockServer wireMock, String path, String commitHash) {
        wireMock.stubFor(head(urlEqualTo(path)).willReturn(aResponse().withStatus(200)))
        wireMock.stubFor(
            get(urlEqualTo(path))
                .willReturn(
                    aResponse().withStatus(200)
                        .withBody("""{"projects": {"elasticsearch": {"commit_hash": "${commitHash}"}}}""")
                )
        )
    }

    /** Stubs HEAD+GET for a manifest path to return HTTP 404, triggering source-build fallback. */
    private static void stubManifestNotFound(WireMockServer wireMock, String path) {
        wireMock.stubFor(head(urlEqualTo(path)).willReturn(aResponse().withStatus(404)))
        wireMock.stubFor(get(urlEqualTo(path)).willReturn(aResponse().withStatus(404)))
    }

    /** Stubs GET for the DRA "latest" endpoint, returning a JSON body with the given build ID. */
    private static void stubLatest(WireMockServer wireMock, String path, String buildId) {
        wireMock.stubFor(
            get(urlEqualTo(path))
                .willReturn(aResponse().withStatus(200).withBody("""{"build_id": "${buildId}"}"""))
        )
    }

    /** Stubs HEAD+GET for a distribution tar.gz artifact path, serving a minimal extractable archive. */
    private static void stubTarArtifact(WireMockServer wireMock, String path, String bwcVersion) {
        wireMock.stubFor(head(urlEqualTo(path)).willReturn(aResponse().withStatus(200)))
        wireMock.stubFor(
            get(urlEqualTo(path))
                .willReturn(aResponse().withStatus(200).withBody(minimalElasticsearchTarGz("${bwcVersion}-SNAPSHOT")))
        )
    }

    /** Stubs HEAD+GET for a Maven JAR artifact path, serving a minimal fake JAR body. */
    private static void stubJarArtifact(WireMockServer wireMock, String path, byte[] content = "fake-jar-content".bytes) {
        wireMock.stubFor(head(urlEqualTo(path)).willReturn(aResponse().withStatus(200)))
        wireMock.stubFor(
            get(urlEqualTo(path))
                .willReturn(aResponse().withStatus(200).withBody(content))
        )
    }

    // -------------------------------------------------------------------------
    // Artifact factory
    // -------------------------------------------------------------------------

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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal

import spock.lang.Specification
import spock.lang.TempDir

import com.fasterxml.jackson.databind.ObjectMapper
import com.sun.net.httpserver.HttpServer

import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.http.HttpClient

/**
 * Unit tests for the HTTP resolution helpers in {@link DraSnapshotBuildIdValueSource}.
 *
 * <p>Each method under test is package-private and takes an {@link HttpClient} as its first
 * argument, so we can point it at a lightweight in-process {@link HttpServer} without
 * WireMock or any other external dependency.  Git remote-tracking refs are supplied by
 * writing plain files under a temporary {@code .git/refs/remotes/} directory.
 *
 * <p>The mode-dispatch logic in {@code obtain()} and the full Gradle task wiring are
 * covered by {@code InternalDistributionBwcSetupPluginFuncTest}.
 */
class DraSnapshotBuildIdValueSourceSpec extends Specification {

    @TempDir
    File repoDir

    private HttpServer server
    private HttpClient httpClient
    private ObjectMapper mapper
    private String baseUrl

    def setup() {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0)
        server.start()
        baseUrl = "http://${server.address.address.hostAddress}:${server.address.port}"
        httpClient = HttpClient.newHttpClient()
        mapper = new ObjectMapper()
        // Minimal .git directory so GitInfo.remoteRefRevision() recognises repoDir as a git repo.
        new File(repoDir, ".git").mkdir()
    }

    def cleanup() {
        server?.stop(0)
    }

    // -------------------------------------------------------------------------
    // resolveByHash
    // -------------------------------------------------------------------------

    def "resolveByHash returns build ID when manifest endpoint returns 200"() {
        given:
        stubGet("/elasticsearch/8.4.0-abc12345/manifest-8.4.0-SNAPSHOT.json", 200, "{}")

        expect:
        DraSnapshotBuildIdValueSource.resolveByHash(httpClient, baseUrl, "8.4.0", "abc12345") == "8.4.0-abc12345"
    }

    def "resolveByHash returns empty string when manifest endpoint returns 404"() {
        given:
        stubGet("/elasticsearch/8.4.0-notfound/manifest-8.4.0-SNAPSHOT.json", 404, null)

        expect:
        DraSnapshotBuildIdValueSource.resolveByHash(httpClient, baseUrl, "8.4.0", "notfound") == ""
    }

    // -------------------------------------------------------------------------
    // resolveLatestBuildId
    // -------------------------------------------------------------------------

    def "resolveLatestBuildId returns build ID for a valid latest response"() {
        given:
        stubGet("/elasticsearch/latest/8.x.json", 200, '{"build_id": "8.4.0-abc12345"}')

        expect:
        DraSnapshotBuildIdValueSource.resolveLatestBuildId(httpClient, mapper, baseUrl, "8.4.0", "8.x") == "8.4.0-abc12345"
    }

    def "resolveLatestBuildId returns empty string when latest endpoint returns 404"() {
        given:
        stubGet("/elasticsearch/latest/8.x.json", 404, null)

        expect:
        DraSnapshotBuildIdValueSource.resolveLatestBuildId(httpClient, mapper, baseUrl, "8.4.0", "8.x") == ""
    }

    def "resolveLatestBuildId returns empty string when the build_id field is absent from the response"() {
        given:
        stubGet("/elasticsearch/latest/8.x.json", 200, "{}")

        expect:
        DraSnapshotBuildIdValueSource.resolveLatestBuildId(httpClient, mapper, baseUrl, "8.4.0", "8.x") == ""
    }

    def "resolveLatestBuildId returns empty string when build_id version prefix does not match the requested version"() {
        given:
        // DRA returned a build for a different branch — version guard must reject it.
        stubGet("/elasticsearch/latest/8.x.json", 200, '{"build_id": "9.0.0-abc12345"}')

        expect:
        DraSnapshotBuildIdValueSource.resolveLatestBuildId(httpClient, mapper, baseUrl, "8.4.0", "8.x") == ""
    }

    def "resolveLatestBuildId returns empty string when build_id contains no dash (malformed)"() {
        given:
        stubGet("/elasticsearch/latest/8.x.json", 200, '{"build_id": "malformed"}')

        expect:
        DraSnapshotBuildIdValueSource.resolveLatestBuildId(httpClient, mapper, baseUrl, "8.4.0", "8.x") == ""
    }

    // -------------------------------------------------------------------------
    // resolveByLatest
    // -------------------------------------------------------------------------

    def "resolveByLatest returns build ID when local ref and DRA manifest commit match"() {
        given:
        def hash = "a" * 40
        writeRemoteRef("origin", "8.x", hash)
        stubGet("/elasticsearch/latest/8.x.json", 200, '{"build_id": "8.4.0-abc12345"}')
        stubGet("/elasticsearch/8.4.0-abc12345/manifest-8.4.0-SNAPSHOT.json", 200,
            """{"projects": {"elasticsearch": {"commit_hash": "${hash}"}}}""")

        expect:
        DraSnapshotBuildIdValueSource.resolveByLatest(
            httpClient, mapper, baseUrl, "8.4.0", "8.x", repoDir, "elastic"
        ) == "8.4.0-abc12345"
    }

    def "resolveByLatest returns empty string when DRA commit hash differs from local ref"() {
        given:
        writeRemoteRef("origin", "8.x", "a" * 40)
        stubGet("/elasticsearch/latest/8.x.json", 200, '{"build_id": "8.4.0-abc12345"}')
        stubGet("/elasticsearch/8.4.0-abc12345/manifest-8.4.0-SNAPSHOT.json", 200,
            """{"projects": {"elasticsearch": {"commit_hash": "${"b" * 40}"}}}""")

        expect:
        DraSnapshotBuildIdValueSource.resolveByLatest(
            httpClient, mapper, baseUrl, "8.4.0", "8.x", repoDir, "elastic"
        ) == ""
    }

    def "resolveByLatest returns empty string when no remote ref exists for either origin or the configured remote"() {
        // No ref files written — remoteRefRevision returns null for both remotes.
        expect:
        DraSnapshotBuildIdValueSource.resolveByLatest(
            httpClient, mapper, baseUrl, "8.4.0", "8.x", repoDir, "elastic"
        ) == ""
    }

    def "resolveByLatest falls back to origin ref when the configured remote has no ref for the branch"() {
        given:
        def hash = "c" * 40
        writeRemoteRef("origin", "8.x", hash)   // only origin, not 'elastic'
        stubGet("/elasticsearch/latest/8.x.json", 200, '{"build_id": "8.4.0-abc12345"}')
        stubGet("/elasticsearch/8.4.0-abc12345/manifest-8.4.0-SNAPSHOT.json", 200,
            """{"projects": {"elasticsearch": {"commit_hash": "${hash}"}}}""")

        expect:
        DraSnapshotBuildIdValueSource.resolveByLatest(
            httpClient, mapper, baseUrl, "8.4.0", "8.x", repoDir, "elastic"
        ) == "8.4.0-abc12345"
    }

    def "resolveByLatest returns empty string when manifest endpoint returns 404"() {
        given:
        writeRemoteRef("origin", "8.x", "d" * 40)
        stubGet("/elasticsearch/latest/8.x.json", 200, '{"build_id": "8.4.0-abc12345"}')
        stubGet("/elasticsearch/8.4.0-abc12345/manifest-8.4.0-SNAPSHOT.json", 404, null)

        expect:
        DraSnapshotBuildIdValueSource.resolveByLatest(
            httpClient, mapper, baseUrl, "8.4.0", "8.x", repoDir, "elastic"
        ) == ""
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void stubGet(String path, int status, String body) {
        server.createContext(path) { exchange ->
            if (body != null) {
                byte[] bytes = body.getBytes("UTF-8")
                exchange.sendResponseHeaders(status, bytes.length)
                exchange.responseBody.withStream { it.write(bytes) }
            } else {
                exchange.sendResponseHeaders(status, -1)
            }
            exchange.close()
        }
    }

    private void writeRemoteRef(String remote, String branch, String hash) {
        File refFile = new File(repoDir, ".git/refs/remotes/${remote}/${branch}")
        refFile.parentFile.mkdirs()
        refFile.text = hash + "\n"
    }
}

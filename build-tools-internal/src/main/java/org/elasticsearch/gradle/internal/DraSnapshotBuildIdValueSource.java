/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.elasticsearch.gradle.internal.conventions.info.GitInfo;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.ValueSource;
import org.gradle.api.provider.ValueSourceParameters;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Resolves the DRA (Distribution Release Artifacts) snapshot build ID for a BWC branch.
 *
 * <p>The behaviour is controlled by the {@link Params#getMode()} parameter, which mirrors the
 * {@code tests.bwc.mode} system property:
 *
 * <ul>
 *   <li>{@code gradle} (the default) — returns {@code ""} immediately with no network activity,
 *       so the caller falls back to a nested Gradle source build.</li>
 *   <li>{@code dra} — resolves the <em>latest</em> DRA snapshot build ID for the branch without
 *       comparing commit hashes.  Returns the build ID when the latest endpoint responds
 *       successfully, or {@code ""} on any error (the caller should treat {@code ""} as a fatal
 *       condition for this mode).</li>
 *   <li>{@code auto} — resolves the latest DRA snapshot build ID <em>and</em> verifies
 *       that the snapshot was built from the same commit as the local remote-tracking ref.
 *       Returns the build ID on a match, or {@code ""} when the hashes differ or when the
 *       endpoint is unreachable (the caller will then fall back to a source build).</li>
 * </ul>
 *
 * <p>In both DRA modes an optional commit-hash override can be supplied via
 * {@code -Dtests.bwc.dra.hash.{branch}}.  When set, the build ID is constructed directly as
 * {@code {version}-{hash}} and its existence is verified against the manifest endpoint — the
 * "latest" lookup and remote-tracking ref comparison are both skipped.  This lets you pin to a
 * specific DRA snapshot even after the branch tip has moved.
 */
public abstract class DraSnapshotBuildIdValueSource implements ValueSource<String, DraSnapshotBuildIdValueSource.Params> {

    private static final Logger logger = Logging.getLogger(DraSnapshotBuildIdValueSource.class);

    public interface Params extends ValueSourceParameters {
        /** BWC version string, e.g. {@code "9.4.2"}. */
        Property<String> getVersion();

        /** BWC branch name, e.g. {@code "9.4"}. */
        Property<String> getBranch();

        /** Root directory of the main git checkout, used to read remote tracking refs without forking a process. */
        Property<File> getRootProjectDir();

        /** Git remote name, e.g. {@code "elastic"}. */
        Property<String> getRemote();

        /**
         * BWC mode that controls how DRA snapshots are resolved.  Accepted values:
         * {@code "gradle"} (default — always build from source),
         * {@code "dra"} (always download latest DRA snapshot, no hash check), or
         * {@code "auto"} (download from DRA if commit hash matches, otherwise build from source).
         *
         * <p>Set via the {@code tests.bwc.mode} system property.
         */
        Property<String> getMode();

        /**
         * Optional abbreviated commit hash supplied via {@code -Dtests.bwc.dra.hash.{branch}}.
         * When set the DRA build ID is constructed directly as {@code {version}-{hash}}
         * (e.g. {@code 9.4.2-1a738181}) and its existence is verified against the manifest
         * endpoint — the "latest" lookup and remote-tracking ref comparison are both skipped.
         * This lets you pin to a specific DRA snapshot even after the branch tip has moved.
         */
        Property<String> getHashOverride();

        /**
         * Base URL for all DRA API and artifact requests.  Defaults to the real DRA server
         * ({@code https://artifacts-snapshot.elastic.co}).  Override via
         * {@code -Dtests.bwc.dra.base.url} in tests to redirect calls to a local mock server.
         */
        Property<String> getBaseUrl();
    }

    @Override
    public String obtain() {
        String mode = getParameters().getMode().getOrElse("gradle");
        if (mode.equals("gradle")) {
            return "";
        }
        try {
            String version = getParameters().getVersion().get();
            String branch = getParameters().getBranch().get();
            String baseUrl = getParameters().getBaseUrl().get();
            HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
            ObjectMapper mapper = new ObjectMapper();

            String hashOverride = getParameters().getHashOverride().getOrElse("");
            if (hashOverride.isEmpty() == false) {
                // Build ID is deterministic: {version}-{hash}. Skip the "latest" lookup and
                // commit comparison — just verify the specific build exists in DRA.
                return resolveByHash(client, baseUrl, version, hashOverride);
            }

            if (mode.equals("dra")) {
                // Always use DRA: return the latest build ID without checking the commit hash.
                return resolveLatestBuildId(client, mapper, baseUrl, version, branch);
            }

            // mode is "auto": only use DRA when the latest snapshot commit matches the
            // local remote-tracking ref.
            return resolveByLatest(
                client,
                mapper,
                baseUrl,
                version,
                branch,
                getParameters().getRootProjectDir().get(),
                getParameters().getRemote().get()
            );
        } catch (Exception e) {
            logger.debug("DRA snapshot resolution failed for version [{}]: {}", getParameters().getVersion().get(), e.getMessage());
            return "";
        }
    }

    private static String resolveByHash(HttpClient client, String baseUrl, String version, String hash) throws Exception {
        String buildId = version + "-" + hash;
        String manifestUrl = baseUrl + "/elasticsearch/" + buildId + "/manifest-" + version + "-SNAPSHOT.json";
        HttpResponse<String> response = client.send(
            HttpRequest.newBuilder().uri(URI.create(manifestUrl)).timeout(Duration.ofSeconds(10)).GET().build(),
            HttpResponse.BodyHandlers.ofString()
        );
        return response.statusCode() == 200 ? buildId : "";
    }

    /**
     * Fetches the latest DRA snapshot build ID for {@code branch} without comparing commit hashes.
     * Used for {@code dra} mode where the caller always wants the newest pre-built snapshot.
     */
    private static String resolveLatestBuildId(HttpClient client, ObjectMapper mapper, String baseUrl, String version, String branch)
        throws Exception {
        String latestUrl = baseUrl + "/elasticsearch/latest/" + branch + ".json";
        HttpResponse<String> latestResponse = client.send(
            HttpRequest.newBuilder().uri(URI.create(latestUrl)).timeout(Duration.ofSeconds(10)).GET().build(),
            HttpResponse.BodyHandlers.ofString()
        );
        if (latestResponse.statusCode() != 200) {
            return "";
        }
        JsonNode latestJson = mapper.readTree(latestResponse.body());
        String buildId = latestJson.path("build_id").asText("");
        if (buildId.isEmpty()) {
            return "";
        }
        int lastDash = buildId.lastIndexOf('-');
        if (lastDash < 0 || buildId.substring(0, lastDash).equals(version) == false) {
            return "";
        }
        return buildId;
    }

    /**
     * Fetches the latest DRA snapshot build ID for {@code branch} and returns it only when the
     * snapshot commit hash matches the local remote-tracking ref for that branch.  Used for
     * {@code auto} mode so that a stale snapshot does not silently replace a newer
     * local build.
     */
    private static String resolveByLatest(
        HttpClient client,
        ObjectMapper mapper,
        String baseUrl,
        String version,
        String branch,
        File rootProjectDir,
        String remote
    ) throws Exception {
        // CI checkouts always clone from elastic/elasticsearch as 'origin'; developer
        // setups may have the branch under a different remote name. Check 'origin' first
        // since it is authoritative in both environments.
        String branchTipHash = GitInfo.remoteRefRevision(rootProjectDir, "origin", branch);
        if (branchTipHash == null) {
            branchTipHash = GitInfo.remoteRefRevision(rootProjectDir, remote, branch);
        }
        if (branchTipHash == null) {
            return "";
        }

        String buildId = resolveLatestBuildId(client, mapper, baseUrl, version, branch);
        if (buildId.isEmpty()) {
            return "";
        }

        String manifestUrl = baseUrl + "/elasticsearch/" + buildId + "/manifest-" + version + "-SNAPSHOT.json";
        HttpResponse<String> manifestResponse = client.send(
            HttpRequest.newBuilder().uri(URI.create(manifestUrl)).timeout(Duration.ofSeconds(10)).GET().build(),
            HttpResponse.BodyHandlers.ofString()
        );
        if (manifestResponse.statusCode() != 200) {
            return "";
        }

        JsonNode manifestJson = mapper.readTree(manifestResponse.body());
        String draCommit = manifestJson.path("projects").path("elasticsearch").path("commit_hash").asText("");
        if (draCommit.isEmpty()) {
            return "";
        }

        if (branchTipHash.equals(draCommit) == false) {
            logger.info(
                "BWC DRA [{}]: commit mismatch for branch [{}] — local ref is [{}] but DRA build [{}] was built from [{}];"
                    + " falling back to source build",
                version,
                branch,
                branchTipHash,
                buildId,
                draCommit
            );
            return "";
        }
        return buildId;
    }
}

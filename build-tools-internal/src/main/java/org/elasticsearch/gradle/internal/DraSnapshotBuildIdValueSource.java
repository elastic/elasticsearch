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
 *   <li>{@code prebuilt} — resolves the <em>latest</em> DRA snapshot build ID for the branch without
 *       comparing commit hashes. Returns the build ID when the latest endpoint responds
 *       successfully, or {@code ""} on any error (the caller should treat {@code ""} as a fatal
 *       condition for this mode).</li>
 *   <li>{@code auto} — resolves the latest DRA snapshot build ID <em>and</em> verifies
 *       that the snapshot was built from the same commit as the local remote-tracking ref.
 *       Returns the build ID on a match, or {@code ""} when the hashes differ or when the
 *       endpoint is unreachable (the caller will then fall back to a source build).</li>
 * </ul>
 *
 * <ul>
 *   <li>{@code dra} — resolves the branch's <em>known-good</em> build: the last DRA build that passed bwc tests in CI,
 *       read from the public known-good store ({@link Params#getKnownGoodUrl()}). The record is used only when it is
 *       for this exact version and the build is still in DRA; otherwise this behaves like {@code prebuilt}. Locally
 *       this runs the same snapshots CI does, without credentials.</li>
 * </ul>
 *
 * <p>In all prebuilt modes an optional build hash override can be supplied via
 * {@code -Dtests.bwc.dra.hash.{branch}}.  When set, the build ID is constructed directly as
 * {@code {version}-{hash}} and its existence is verified against the manifest endpoint — the
 * "latest" lookup and remote-tracking ref comparison are both skipped.  This lets you pin to a
 * specific DRA snapshot even after the branch tip has moved; CI uses it to test the last snapshot
 * that passed bwc tests rather than the moving tip.  A pin is strict: when the pinned build cannot be
 * found the build fails instead of falling back, so a pinned run never silently tests something else.
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
         * BWC mode that controls how pre-built snapshot artifacts are resolved. Accepted values:
         * {@code "gradle"} (default — always build from source),
         * {@code "prebuilt"} (always download the latest pre-built snapshot artifacts, no hash check), or
         * {@code "auto"} (download pre-built snapshot artifacts if the commit hash matches, otherwise build from source), or
         * {@code "dra"} (download the branch's known-good build, or the latest one when there is none).
         *
         * <p>Set via the {@code tests.bwc.mode} system property.
         */
        Property<String> getMode();

        /**
         * Optional DRA build hash (the suffix of the DRA build ID, not a git commit) supplied via
         * {@code -Dtests.bwc.dra.hash.{branch}}. When set the DRA build ID is constructed directly as
         * {@code {version}-{hash}} (e.g. {@code 9.4.2-1a738181}) and its existence is verified against the
         * manifest endpoint — the "latest" lookup and remote-tracking ref comparison are both skipped.
         * This lets you pin to a specific DRA snapshot even after the branch tip has moved. If the pinned
         * build cannot be verified the build fails.
         */
        Property<String> getHashOverride();

        /**
         * Base URL for all DRA API and artifact requests.  Defaults to the real DRA server
         * ({@code https://artifacts-snapshot.elastic.co}).  Override via
         * {@code -Dtests.bwc.dra.base.url} in tests to redirect calls to a local mock server.
         */
        Property<String> getBaseUrl();

        /**
         * Base URL of the known-good store read in {@code dra} mode: one {@code <branch>.txt} object per branch holding the
         * build ID, written by CI when a bwc run on that DRA build passes. It is a public bucket, read without credentials.
         */
        Property<String> getKnownGoodUrl();
    }

    @Override
    public String obtain() {
        String mode = getParameters().getMode().getOrElse("gradle");
        if (mode.equals("gradle")) {
            return "";
        }
        String hashOverride = getParameters().getHashOverride().getOrElse("");
        if (hashOverride.isEmpty() == false) {
            // Build ID is deterministic: {version}-{hash}. Skip the "latest" lookup and commit
            // comparison — just verify the specific build exists in DRA. Not caught below: a
            // pinned build that cannot be resolved must fail the build.
            HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
            return resolvePinned(
                client,
                getParameters().getBaseUrl().get(),
                getParameters().getVersion().get(),
                getParameters().getBranch().get(),
                hashOverride
            );
        }
        try {
            String version = getParameters().getVersion().get();
            String branch = getParameters().getBranch().get();
            String baseUrl = getParameters().getBaseUrl().get();
            HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
            ObjectMapper mapper = new ObjectMapper();

            if (mode.equals("dra")) {
                String knownGood = resolveKnownGood(client, getParameters().getKnownGoodUrl().get(), baseUrl, version, branch);
                if (knownGood.isEmpty() == false) {
                    return knownGood;
                }
            }
            if (mode.equals("prebuilt") || mode.equals("dra")) {
                // Always use the latest pre-built snapshot artifacts: return the latest build ID
                // without checking the commit hash.
                return resolveLatestBuildId(client, mapper, baseUrl, version, branch);
            }

            // mode is "auto": only use pre-built snapshot artifacts when the latest snapshot
            // commit matches the local remote-tracking ref.
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

    /**
     * Resolves a build pinned via {@code -Dtests.bwc.dra.hash.{branch}}, failing with a message that names the pin
     * when the build is missing or the snapshot server is unreachable.
     */
    static String resolvePinned(HttpClient client, String baseUrl, String version, String branch, String hash) {
        String buildId = version + "-" + hash;
        String pin = "-Dtests.bwc.dra.hash." + branch + "=" + hash;
        int status;
        try {
            status = fetchManifestStatus(client, baseUrl, version, buildId);
        } catch (Exception e) {
            throw new IllegalStateException(
                "BWC [" + version + "]: could not verify pinned DRA snapshot build [" + buildId + "] (" + pin + ") at " + baseUrl,
                e
            );
        }
        if (status != 200) {
            throw new IllegalStateException(
                "BWC ["
                    + version
                    + "]: pinned DRA snapshot build ["
                    + buildId
                    + "] ("
                    + pin
                    + ") is not available: manifest returned HTTP "
                    + status
                    + ". The pin is not replaced by the latest snapshot or a source build; update or remove it."
            );
        }
        return buildId;
    }

    /**
     * Reads the branch's known-good build from the store and returns it when the record is for {@code version} and the
     * build is still in DRA. Returns {@code ""} otherwise, including when the store cannot be read, so that {@code dra}
     * mode falls back to the latest build.
     */
    static String resolveKnownGood(HttpClient client, String knownGoodUrl, String baseUrl, String version, String branch) {
        String recordUrl = knownGoodUrl + "/" + branch + ".txt";
        try {
            HttpResponse<String> response = client.send(
                HttpRequest.newBuilder().uri(URI.create(recordUrl)).timeout(Duration.ofSeconds(10)).GET().build(),
                HttpResponse.BodyHandlers.ofString()
            );
            if (response.statusCode() != 200) {
                logger.info(
                    "BWC DRA [{}]: no known-good build at {} (HTTP {}); using the latest build",
                    version,
                    recordUrl,
                    response.statusCode()
                );
                return "";
            }
            String buildId = response.body().trim();
            if (buildId.startsWith(version + "-") == false) {
                logger.info(
                    "BWC DRA [{}]: known-good build [{}] of [{}] is for another version; using the latest build",
                    version,
                    buildId,
                    branch
                );
                return "";
            }
            if (fetchManifestStatus(client, baseUrl, version, buildId) != 200) {
                logger.info("BWC DRA [{}]: known-good build [{}] is no longer in DRA; using the latest build", version, buildId);
                return "";
            }
            return buildId;
        } catch (Exception e) {
            logger.info(
                "BWC DRA [{}]: could not read the known-good build from {}; using the latest build: {}",
                version,
                recordUrl,
                e.getMessage()
            );
            return "";
        }
    }

    private static int fetchManifestStatus(HttpClient client, String baseUrl, String version, String buildId) throws Exception {
        String manifestUrl = baseUrl + "/elasticsearch/" + buildId + "/manifest-" + version + "-SNAPSHOT.json";
        HttpResponse<String> response = client.send(
            HttpRequest.newBuilder().uri(URI.create(manifestUrl)).timeout(Duration.ofSeconds(10)).GET().build(),
            HttpResponse.BodyHandlers.ofString()
        );
        return response.statusCode();
    }

    /**
     * Fetches the latest DRA snapshot build ID for {@code branch} without comparing commit hashes.
     * Used for {@code prebuilt} mode where the caller always wants the newest pre-built snapshot.
     */
    static String resolveLatestBuildId(HttpClient client, ObjectMapper mapper, String baseUrl, String version, String branch)
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
    static String resolveByLatest(
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

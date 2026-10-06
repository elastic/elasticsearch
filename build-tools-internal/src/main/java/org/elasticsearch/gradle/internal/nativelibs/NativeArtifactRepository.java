/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.nativelibs;

import org.gradle.api.GradleException;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Reads and writes native library artifacts, addressed by the hash of the sources they were built
 * from.
 *
 * <p>Not modelled as a Gradle dependency: a dependency's version must be known at configuration time,
 * and deriving this one means reading the sources then, which makes them a configuration-cache input —
 * so every edit to the native sources would reconfigure the whole build.
 */
class NativeArtifactRepository {

    private static final Logger LOGGER = Logging.getLogger(NativeArtifactRepository.class);

    private static final int CONNECT_TIMEOUT_MILLIS = 30_000;
    private static final int READ_TIMEOUT_MILLIS = 60_000;

    private final String baseUrl;

    NativeArtifactRepository(String baseUrl) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
    }

    /**
     * Fetches the artifact for {@code hash}, or reports its absence. An absent artifact means "needs building".
     */
    Optional<byte[]> download(String artifactName, String hash) {
        String url = artifactUrl(artifactName, hash);
        HttpURLConnection connection = open(url, "GET");
        try {
            int status = connection.getResponseCode();
            if (status == HttpURLConnection.HTTP_NOT_FOUND) {
                LOGGER.info("No published {} for hash {}", artifactName, hash);
                return Optional.empty();
            }
            if (status != HttpURLConnection.HTTP_OK) {
                throw new GradleException("Unexpected status " + status + " fetching " + url);
            }
            try (InputStream in = connection.getInputStream()) {
                return Optional.of(in.readAllBytes());
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to fetch " + url, e);
        } finally {
            connection.disconnect();
        }
    }

    /**
     * Uploads the artifact for {@code hash}. Publishing a hash that already exists succeeds
     * as long as the artifact there is correct.
     *
     * @param checkCorrectness throws if a published archive cannot serve as this library's artifact
     */
    void publish(String artifactName, String hash, byte[] content, String apiKey, Consumer<byte[]> checkCorrectness) {
        String url = artifactUrl(artifactName, hash);
        int status = put(url, content, apiKey);

        if (status / 100 == 2) {
            verifyPublished(artifactName, hash, content);
            LOGGER.lifecycle("Published {} for hash {}", artifactName, hash);
            return;
        }

        // Rather than interpreting the status, check if the published artifact is present and correct.
        LOGGER.lifecycle("Publishing {} for hash {} was refused with status {}; checking what is published", artifactName, hash, status);
        byte[] published = download(artifactName, hash).orElseThrow(
            () -> new GradleException("Failed to publish " + url + ": status " + status + ", and nothing is published for this hash")
        );
        try {
            checkCorrectness.accept(published);
        } catch (RuntimeException e) {
            throw new GradleException(
                "Failed to publish " + url + ": status " + status + ". The artifact already published for this hash is not usable.",
                e
            );
        }
        LOGGER.lifecycle("{} for hash {} was already published by another build", artifactName, hash);
    }

    private int put(String url, byte[] content, String apiKey) {
        HttpURLConnection connection = open(url, "PUT");
        connection.setDoOutput(true);
        connection.setRequestProperty("X-JFrog-Art-Api", apiKey);
        connection.setRequestProperty("Content-Type", "application/zip");
        try {
            try (OutputStream out = connection.getOutputStream()) {
                out.write(content);
            }
            return connection.getResponseCode();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to publish " + url, e);
        } finally {
            connection.disconnect();
        }
    }

    /**
     * Confirms the published artifact matches what was uploaded. An interrupted upload leaves a
     * truncated artifact under a hash that still looks correct, so later builds would trust it.
     */
    void verifyPublished(String artifactName, String hash, byte[] expected) {
        byte[] actual = download(artifactName, hash).orElseThrow(
            () -> new GradleException("Published " + artifactName + " for hash " + hash + " but it cannot be read back")
        );
        if (Arrays.equals(expected, actual) == false) {
            throw new GradleException(
                "Published "
                    + artifactName
                    + " for hash "
                    + hash
                    + " does not match what was uploaded ("
                    + expected.length
                    + " bytes sent, "
                    + actual.length
                    + " bytes read back). The artifact may be truncated and must be removed before retrying."
            );
        }
    }

    private String artifactUrl(String artifactName, String hash) {
        return baseUrl + "/org/elasticsearch/" + artifactName + "/" + hash + "/" + artifactName + "-" + hash + ".zip";
    }

    private static HttpURLConnection open(String url, String method) {
        try {
            HttpURLConnection connection = (HttpURLConnection) URI.create(url).toURL().openConnection();
            connection.setRequestMethod(method);
            connection.setConnectTimeout(CONNECT_TIMEOUT_MILLIS);
            connection.setReadTimeout(READ_TIMEOUT_MILLIS);
            return connection;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to open " + method + " " + url, e);
        }
    }
}

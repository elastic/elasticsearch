/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.docker;

import org.elasticsearch.gradle.LoggedExec;
import org.gradle.api.GradleException;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.process.ExecOperations;
import org.gradle.process.ExecSpec;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Helpers for making Docker images available locally, pulling them with retries if needed.
 */
public final class DockerImagePullSupport {

    private static final Logger LOGGER = Logging.getLogger(DockerImagePullSupport.class);
    private static final int DEFAULT_MAX_PULL_ATTEMPTS = 5;
    private static final long INITIAL_PULL_BACKOFF_MS = 2_000;
    private static final Pattern FROM_INSTRUCTION = Pattern.compile(
        "^\\s*FROM\\s+(?:--platform=\\S+\\s+)?(\\S+)",
        Pattern.CASE_INSENSITIVE
    );

    private DockerImagePullSupport() {}

    /**
     * Makes sure the given image is available locally. If it is already present (e.g. pre-cached on CI agents at image
     * bake time) no registry access is needed. Otherwise the image is pulled with retries and exponential backoff.
     * <p>
     * Only use this for references that are immutable (pinned by digest) or where reusing a possibly outdated local
     * image for a moving tag is acceptable.
     *
     * @param platform the Docker platform (e.g. {@code linux/amd64}) the image must be available for, or {@code null}
     *                 for the host platform
     */
    public static void ensureImageAvailable(ExecOperations execOperations, String dockerExecutable, String image, String platform) {
        if (isImagePresent(execOperations, dockerExecutable, image, platform)) {
            LOGGER.info("Docker image {} for platform {} is available locally, skipping pull", image, describe(platform));
            return;
        }
        pullImage(execOperations, dockerExecutable, image, platform, DEFAULT_MAX_PULL_ATTEMPTS);
    }

    /**
     * Pulls the given image with retries and exponential backoff, regardless of whether it is already present locally.
     *
     * @param platform the Docker platform to pull, or {@code null} for the host platform
     */
    public static void pullImage(ExecOperations execOperations, String dockerExecutable, String image, String platform, int maxAttempts) {
        pullImage(execOperations, dockerExecutable, image, platform, maxAttempts, INITIAL_PULL_BACKOFF_MS);
    }

    /**
     * Package-private variant of {@link #pullImage(ExecOperations, String, String, String, int)} that allows the
     * initial backoff to be overridden. Tests pass {@code 0} to avoid real sleeps between attempts.
     */
    static void pullImage(
        ExecOperations execOperations,
        String dockerExecutable,
        String image,
        String platform,
        int maxAttempts,
        long initialBackoffMs
    ) {
        long backoffMs = initialBackoffMs;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                LoggedExec.exec(execOperations, spec -> {
                    maybeConfigureDockerConfig(spec);
                    spec.executable(dockerExecutable);
                    spec.args("pull");
                    if (platform != null) {
                        spec.args("--platform", platform);
                    }
                    spec.args(image);
                });
                return;
            } catch (Exception e) {
                if (attempt == maxAttempts) {
                    throw new GradleException(
                        "Failed to pull Docker image [" + image + "] for platform [" + describe(platform) + "], all attempts failed",
                        e
                    );
                }
                LOGGER.warn(
                    "Attempt {}/{} to pull Docker image {} for platform {} failed, retrying in {}ms",
                    attempt,
                    maxAttempts,
                    image,
                    describe(platform),
                    backoffMs
                );
                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new GradleException("Interrupted while waiting to retry pulling Docker image [" + image + "]", ie);
                }
                backoffMs *= 2;
            }
        }
    }

    /**
     * Resolves the local image ID (content-addressed digest) of the given image reference as currently stored in the
     * Docker image store. Unlike a tag, which the daemon rebinds to whichever platform variant was pulled last, the
     * returned image ID is immutable and platform-specific. Running a container by this ID is therefore safe against a
     * concurrent pull of a different platform variant for the same tag: the classic image store holds a single variant
     * per tag, but keeps each pulled image object around.
     *
     * @return the image ID (e.g. {@code sha256:...}), or {@code null} if the image is not present locally
     */
    public static String resolveLocalImageId(ExecOperations execOperations, String dockerExecutable, String image) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        var result = execOperations.exec(spec -> {
            maybeConfigureDockerConfig(spec);
            spec.executable(dockerExecutable);
            spec.args("image", "inspect", "--format", "{{.Id}}", image);
            spec.setStandardOutput(output);
            spec.setErrorOutput(new ByteArrayOutputStream());
            spec.setIgnoreExitValue(true);
        });
        if (result.getExitValue() != 0) {
            return null;
        }
        String id = output.toString(StandardCharsets.UTF_8).trim();
        return id.isEmpty() ? null : id;
    }

    /**
     * Returns whether the image reference is pinned by digest, i.e. immutable.
     */
    public static boolean isDigestPinned(String image) {
        return image.contains("@sha256:");
    }

    /**
     * Returns the digest-pinned external images referenced by {@code FROM} instructions in the given Dockerfile.
     * References to build stages, build args (e.g. {@code ${base_image}}) and unpinned tags are ignored. This keeps the
     * Dockerfile (which is updated by Renovate) the single source of truth for those images.
     */
    public static List<String> digestPinnedBaseImages(File dockerfile) {
        Set<String> images = new LinkedHashSet<>();
        try {
            for (String line : Files.readAllLines(dockerfile.toPath(), StandardCharsets.UTF_8)) {
                Matcher matcher = FROM_INSTRUCTION.matcher(line);
                if (matcher.find()) {
                    String image = matcher.group(1);
                    if (isDigestPinned(image) && image.contains("$") == false) {
                        images.add(image);
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read Dockerfile " + dockerfile, e);
        }
        return List.copyOf(images);
    }

    private static boolean isImagePresent(ExecOperations execOperations, String dockerExecutable, String image, String platform) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        var result = execOperations.exec(spec -> {
            maybeConfigureDockerConfig(spec);
            spec.executable(dockerExecutable);
            spec.args("image", "inspect", "--format", "{{.Os}}/{{.Architecture}}", image);
            spec.setStandardOutput(output);
            spec.setErrorOutput(new ByteArrayOutputStream());
            spec.setIgnoreExitValue(true);
        });
        if (result.getExitValue() != 0) {
            return false;
        }
        return platform == null || output.toString(StandardCharsets.UTF_8).trim().toLowerCase(Locale.ROOT).equals(platform);
    }

    private static String describe(String platform) {
        return platform == null ? "host" : platform;
    }

    static void maybeConfigureDockerConfig(ExecSpec spec) {
        String dockerConfig = System.getenv("DOCKER_CONFIG");
        if (dockerConfig != null) {
            spec.environment("DOCKER_CONFIG", dockerConfig);
        }
    }
}

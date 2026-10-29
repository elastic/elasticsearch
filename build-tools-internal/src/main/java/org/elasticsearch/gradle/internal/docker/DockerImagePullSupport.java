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
import java.nio.charset.StandardCharsets;

/**
 * Helpers for making a Docker image available locally for a given platform, pulling it with retries if needed.
 */
public final class DockerImagePullSupport {

    private static final Logger LOGGER = Logging.getLogger(DockerImagePullSupport.class);
    private static final int MAX_PULL_ATTEMPTS = 5;
    private static final long INITIAL_PULL_BACKOFF_MS = 2_000;

    private DockerImagePullSupport() {}

    /**
     * Makes sure the given image is available locally for the requested platform. If it is already present (e.g.
     * pre-cached on CI agents at image bake time) no registry access is needed. Otherwise the image is pulled explicitly
     * with retries and exponential backoff to provide resilience against transient registry or network errors.
     */
    public static void ensureImageAvailable(ExecOperations execOperations, String dockerExecutable, String imageTag, String platform) {
        if (isImagePresent(execOperations, dockerExecutable, imageTag, platform)) {
            LOGGER.info("Docker image {} for platform {} is available locally, skipping pull", imageTag, platform);
            return;
        }
        pullImage(execOperations, dockerExecutable, imageTag, platform);
    }

    private static boolean isImagePresent(ExecOperations execOperations, String dockerExecutable, String imageTag, String platform) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        var result = execOperations.exec(spec -> {
            maybeConfigureDockerConfig(spec);
            spec.executable(dockerExecutable);
            spec.args("image", "inspect", "--format", "{{.Os}}/{{.Architecture}}", imageTag);
            spec.setStandardOutput(output);
            spec.setErrorOutput(new ByteArrayOutputStream());
            spec.setIgnoreExitValue(true);
        });
        return result.getExitValue() == 0 && output.toString(StandardCharsets.UTF_8).trim().equals(platform);
    }

    private static void pullImage(ExecOperations execOperations, String dockerExecutable, String imageTag, String platform) {
        long backoffMs = INITIAL_PULL_BACKOFF_MS;
        for (int attempt = 1; attempt <= MAX_PULL_ATTEMPTS; attempt++) {
            try {
                LoggedExec.exec(execOperations, spec -> {
                    maybeConfigureDockerConfig(spec);
                    spec.executable(dockerExecutable);
                    spec.args("pull", "--platform", platform, imageTag);
                });
                return;
            } catch (Exception e) {
                if (attempt == MAX_PULL_ATTEMPTS) {
                    throw new GradleException(
                        "Failed to pull Docker image [" + imageTag + "] for platform [" + platform + "], all attempts failed",
                        e
                    );
                }
                LOGGER.warn(
                    "Attempt {}/{} to pull Docker image {} for platform {} failed, retrying in {}ms",
                    attempt,
                    MAX_PULL_ATTEMPTS,
                    imageTag,
                    platform,
                    backoffMs
                );
                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new GradleException("Interrupted while waiting to retry pulling Docker image [" + imageTag + "]", ie);
                }
                backoffMs *= 2;
            }
        }
    }

    static void maybeConfigureDockerConfig(ExecSpec spec) {
        String dockerConfig = System.getenv("DOCKER_CONFIG");
        if (dockerConfig != null) {
            spec.environment("DOCKER_CONFIG", dockerConfig);
        }
    }
}

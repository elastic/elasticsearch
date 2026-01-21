/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.fixtures.testcontainers;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.PullImageResultCallback;
import com.github.dockerjava.api.exception.NotFoundException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.images.ImagePullPolicy;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.LazyFuture;

import java.util.function.Supplier;

/**
 * A {@link java.util.concurrent.Future} implementation that attempts to pull a remote Docker image,
 * falling back to building from a Dockerfile if the pull fails.
 * <p>
 * This is useful when you want to use a pre-built image from a registry (faster) but have a
 * fallback option to build locally if the registry is unavailable or the image doesn't exist.
 * <p>
 * Example usage:
 * <pre>{@code
 * new GenericContainer<>(
 *     new PullOrBuildImage(
 *         "myregistry.io/myimage:v1.0",
 *         new ImageFromDockerfile()
 *             .withFileFromClasspath("Dockerfile", "/docker/Dockerfile")
 *     )
 * );
 * }</pre>
 */
public class PullOrBuildImage extends LazyFuture<String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PullOrBuildImage.class);

    private final DockerImageName remoteImageName;
    private final ImageFromDockerfile fallbackImage;
    private final Supplier<DockerClient> dockerClientSupplier;
    private final ImagePullPolicy imagePullPolicy;

    /**
     * Creates a new PullOrBuildImage with the specified remote image name and fallback Dockerfile.
     *
     * @param remoteImageName the name of the remote image to pull (e.g., "myregistry.io/myimage:v1.0")
     * @param fallbackImage   the ImageFromDockerfile to build if the pull fails
     */
    public PullOrBuildImage(String remoteImageName, ImageFromDockerfile fallbackImage) {
        this(DockerImageName.parse(remoteImageName), fallbackImage);
    }

    /**
     * Creates a new PullOrBuildImage with the specified remote image name and fallback Dockerfile.
     *
     * @param remoteImageName the DockerImageName of the remote image to pull
     * @param fallbackImage   the ImageFromDockerfile to build if the pull fails
     */
    public PullOrBuildImage(DockerImageName remoteImageName, ImageFromDockerfile fallbackImage) {
        this(remoteImageName, fallbackImage, () -> DockerClientFactory.instance().client(), PullPolicy.defaultPolicy());
    }

    // Package-private constructor for testing
    PullOrBuildImage(
        DockerImageName remoteImageName,
        ImageFromDockerfile fallbackImage,
        Supplier<DockerClient> dockerClientSupplier,
        ImagePullPolicy imagePullPolicy
    ) {
        this.remoteImageName = remoteImageName;
        this.fallbackImage = fallbackImage;
        this.dockerClientSupplier = dockerClientSupplier;
        this.imagePullPolicy = imagePullPolicy;
    }

    @Override
    protected String resolve() {
        if (tryPull()) {
            return remoteImageName.asCanonicalNameString();
        }
        LOGGER.info("Building image from Dockerfile as fallback");
        return fallbackImage.get();
    }

    private boolean tryPull() {
        try {
            // Check if image exists locally first (using Testcontainers' cache and pull policy)
            if (imagePullPolicy.shouldPull(remoteImageName) == false) {
                LOGGER.info("Image {} found locally, skipping pull", remoteImageName);
                return true;
            }

            LOGGER.info("Attempting to pull remote image: {}", remoteImageName);
            dockerClientSupplier.get()
                .pullImageCmd(remoteImageName.asCanonicalNameString())
                .exec(new PullImageResultCallback())
                .awaitCompletion();
            LOGGER.info("Successfully pulled remote image: {}", remoteImageName);
            return true;
        } catch (NotFoundException e) {
            LOGGER.info("Remote image not found: {}. Falling back to Dockerfile build.", remoteImageName);
            return false;
        } catch (Exception e) {
            LOGGER.info("Failed to pull remote image: {}. Falling back to Dockerfile build. Reason: {}", remoteImageName, e.getMessage());
            return false;
        }
    }
}

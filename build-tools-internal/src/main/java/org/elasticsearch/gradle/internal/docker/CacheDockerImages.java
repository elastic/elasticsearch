/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.docker;

import org.gradle.api.DefaultTask;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.SetProperty;
import org.gradle.api.services.ServiceReference;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.TaskAction;
import org.gradle.process.ExecOperations;

import javax.inject.Inject;

/**
 * Pulls Docker images required by the build into the local Docker image store, so they can be baked into CI agent
 * images (see {@link DockerImageCachePlugin}) rather than being pulled from a remote registry at job time.
 * Images already present locally are not pulled again. The task declares no outputs, so it is never up-to-date.
 */
public abstract class CacheDockerImages extends DefaultTask {

    @Inject
    public CacheDockerImages() {
        onlyIf("There are images to cache", t -> getImages().get().isEmpty() == false);
        onlyIf("Docker is available", t -> getDockerSupport().get().getDockerAvailability().isAvailable());
    }

    /** Images to cache. */
    @Input
    public abstract SetProperty<String> getImages();

    /** Docker platform to cache the images for, e.g. {@code linux/amd64}. */
    @Input
    public abstract Property<String> getPlatform();

    @ServiceReference(DockerSupportPlugin.DOCKER_SUPPORT_SERVICE_NAME)
    public abstract Property<DockerSupportService> getDockerSupport();

    @Inject
    public abstract ExecOperations getExecOperations();

    @TaskAction
    public void cacheImages() {
        String docker = getDockerSupport().get().getResolvedDockerExecutable();
        String platform = getPlatform().get();
        for (String image : getImages().get()) {
            getLogger().lifecycle("Caching Docker image {} for platform {}", image, platform);
            DockerImagePullSupport.ensureImageAvailable(getExecOperations(), docker, image, platform);
        }
    }
}

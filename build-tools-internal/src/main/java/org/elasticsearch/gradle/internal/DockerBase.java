/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

/**
 * This class models the different Docker base images that are used to build Docker distributions of Elasticsearch.
 */
public enum DockerBase {
    // "latest" here is intentional, since the image name specifies "8"
    DEFAULT("docker.elastic.co/ubi8/ubi-minimal:latest", "", "microdnf"),

    // The Iron Bank base image is UBI (albeit hardened), but we are required to parameterize the Docker build
    IRON_BANK("${BASE_REGISTRY}/${BASE_IMAGE}:${BASE_TAG}", "-ironbank", "yum"),

    // Chainguard based wolfi image with latest jdk
    // This is usually updated via renovatebot
    // spotless:off
    WOLFI("docker.elastic.co/wolfi/chainguard-base:latest@sha256:32f06b169bb4b0f257fbb10e8c8379f06d3ee1355c89b3327cb623781a29590e",
        "-wolfi",
        "apk"
    ),
    // spotless:on
    // Based on WOLFI above, with more extras. We don't set a base image because
    // we programmatically extend from the wolfi image.
    CLOUD_ESS(null, "-cloud-ess", "apk");

    private final String image;
    private final String suffix;
    private final String packageManager;

    DockerBase(String image, String suffix) {
        this(image, suffix, "apt-get");
    }

    DockerBase(String image, String suffix, String packageManager) {
        this.image = image;
        this.suffix = suffix;
        this.packageManager = packageManager;
    }

    public String getImage() {
        return image;
    }

    public String getSuffix() {
        return suffix;
    }

    public String getPackageManager() {
        return packageManager;
    }
}

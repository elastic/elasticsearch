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
    // "latest" here is intentional, since the image name specifies "9"
    DEFAULT("redhat/ubi9-minimal:latest", "", "microdnf", "dockerfiles/default/Dockerfile"),

    // The Iron Bank base image is UBI (albeit hardened), but we are required to parameterize the Docker build
    IRON_BANK("${BASE_REGISTRY}/${BASE_IMAGE}:${BASE_TAG}", "-ironbank", "yum", "Dockerfile"),

    // Chainguard based wolfi image with latest jdk
    WOLFI(
        null,
        "-wolfi",
        "apk",
        "dockerfiles/wolfi/Dockerfile"
    ),
    // Based on WOLFI above, with more extras. We don't set a base image because
    // we programmatically extend from the wolfi image.
    CLOUD_ESS(null, "-cloud-ess", "apk", "Dockerfile.ess"),

    CLOUD_ESS_FIPS(
        null,
        "-cloud-ess-fips",
        "apk",
        "dockerfiles/cloud_ess_fips/Dockerfile"
    );

    private final String image;
    private final String suffix;
    private final String packageManager;
    private final String dockerfile;

    DockerBase(String image, String suffix) {
        this(image, suffix, "apt-get", "dockerfile");
    }

    DockerBase(String image, String suffix, String packageManager, String dockerfile) {
        this.image = image;
        this.suffix = suffix;
        this.packageManager = packageManager;
        this.dockerfile = dockerfile;
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

    public String getDockerfile() {
        return dockerfile;
    }
}

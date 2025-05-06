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
    DEFAULT("redhat/ubi9-minimal:latest", "", "microdnf", "Dockerfile.default"),

    // The Iron Bank base image is UBI (albeit hardened), but we are required to parameterize the Docker build
    IRON_BANK("${BASE_REGISTRY}/${BASE_IMAGE}:${BASE_TAG}", "-ironbank", "yum", "Dockerfile"),

    // Chainguard based wolfi image with latest jdk
    // This is usually updated via renovatebot
    // spotless:off
    WOLFI(
        "docker.elastic.co/wolfi/chainguard-base:latest@sha256:29150cd940cc7f69407d978d5a19c86f4d9e67cf44e4d6ded787a497e8f27c9a",
        "-wolfi",
        "apk",
        "Dockerfile"
    ),
    // spotless:on
    // Based on WOLFI above, with more extras. We don't set a base image because
    // we programmatically extend from the wolfi image.
    CLOUD_ESS(null, "-cloud-ess", "apk", "Dockerfile.ess"),

    CLOUD_ESS_FIPS(
        "docker.elastic.co/wolfi/chainguard-base-fips:sha256-ebfc3f1d7dba992231747a2e05ad1b859843e81b5e676ad342859d7cf9e425a7@sha256:ebfc3f1d7dba992231747a2e05ad1b859843e81b5e676ad342859d7cf9e425a7",
        "-cloud-ess-fips",
        "apk",
        "Dockerfile.ess-fips"
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

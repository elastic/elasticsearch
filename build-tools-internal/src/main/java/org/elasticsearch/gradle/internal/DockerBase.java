/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

/**
 * This class models the different Docker base images that are used to build Docker distributions of Elasticsearch.
 */
public enum DockerBase {
    CENTOS("centos:8", ""),

    // "latest" here is intentional, since the image name specifies "8"
    UBI("docker.elastic.co/ubi8/ubi-minimal:latest", "-ubi8"),

    // The Iron Bank base image is UBI (albeit hardened), but we are required to parameterize the Docker build
    IRON_BANK("${BASE_REGISTRY}/${BASE_IMAGE}:${BASE_TAG}", "-ironbank"),

    // Base image with extras for Cloud
    CLOUD("centos:8", "-cloud"),

    // Based on CLOUD above, with more extras
    CLOUD_ESS("centos:8", "-cloud-ess");

    private final String image;
    private final String suffix;

    DockerBase(String image, String suffix) {
        this.image = image;
        this.suffix = suffix;
    }

    public String getImage() {
        return image;
    }

    public String getSuffix() {
        return suffix;
    }
}

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle;

/**
 * This class models the different Docker base images that are used to build Docker distributions of Elasticsearch.
 */
public enum DockerBase {
    CENTOS("centos:8"),
    // "latest" here is intentional, since the image name specifies "8"
    UBI("docker.elastic.co/ubi8/ubi-minimal:latest"),
    // The Iron Bank base image is UBI (albeit hardened), but we are required to parameterize the Docker build
    IRON_BANK("${BASE_REGISTRY}/${BASE_IMAGE}:${BASE_TAG}");

    private final String image;

    DockerBase(String image) {
        this.image = image;
    }

    public String getImage() {
        return image;
    }
}

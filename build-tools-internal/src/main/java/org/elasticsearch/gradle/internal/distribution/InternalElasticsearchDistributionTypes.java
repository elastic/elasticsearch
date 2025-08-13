/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.distribution;

import org.elasticsearch.gradle.ElasticsearchDistributionType;

import java.util.List;

public class InternalElasticsearchDistributionTypes {

    public static final ElasticsearchDistributionType DEB = new DebElasticsearchDistributionType();
    public static final ElasticsearchDistributionType RPM = new RpmElasticsearchDistributionType();
    public static final ElasticsearchDistributionType DOCKER = new DockerElasticsearchDistributionType();
    public static final ElasticsearchDistributionType DOCKER_IRONBANK = new DockerIronBankElasticsearchDistributionType();
    public static final ElasticsearchDistributionType DOCKER_CLOUD_ESS = new DockerCloudEssElasticsearchDistributionType();
    public static final ElasticsearchDistributionType DOCKER_WOLFI = new DockerWolfiElasticsearchDistributionType();

    public static final List<ElasticsearchDistributionType> ALL_INTERNAL = List.of(
        DEB,
        RPM,
        DOCKER,
        DOCKER_IRONBANK,
        DOCKER_WOLFI,
        DOCKER_CLOUD_ESS
    );
}

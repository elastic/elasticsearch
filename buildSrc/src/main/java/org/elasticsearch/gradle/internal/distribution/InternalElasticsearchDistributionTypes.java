/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.distribution;

import org.elasticsearch.gradle.ElasticsearchDistributionType;

import java.util.List;

public class InternalElasticsearchDistributionTypes {
    public static ElasticsearchDistributionType DEB = new DebElasticsearchDistributionType();
    public static ElasticsearchDistributionType RPM = new RpmElasticsearchDistributionType();
    public static ElasticsearchDistributionType DOCKER = new DockerElasticsearchDistributionType();
    public static ElasticsearchDistributionType DOCKER_UBI = new DockerUbiElasticsearchDistributionType();
    public static ElasticsearchDistributionType DOCKER_IRONBANK = new DockerIronBankElasticsearchDistributionType();

    public static List<ElasticsearchDistributionType> ALL_INTERNAL = List.of(DEB, RPM, DOCKER, DOCKER_UBI, DOCKER_IRONBANK);
}

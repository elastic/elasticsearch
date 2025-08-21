/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import fixture.aws.DynamicAwsCredentials;
import fixture.aws.DynamicRegionSupplier;
import fixture.aws.imds.Ec2ImdsHttpFixture;
import fixture.aws.imds.Ec2ImdsServiceBuilder;
import fixture.aws.imds.Ec2ImdsVersion;
import fixture.s3.S3HttpFixture;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.fixtures.testcontainers.TestContainersThreadFilter;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.Set;
import java.util.function.Supplier;

@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
@ThreadLeakScope(ThreadLeakScope.Scope.NONE) // https://github.com/elastic/elasticsearch/issues/102482
public class RepositoryS3EcsCredentialsRestIT extends AbstractRepositoryS3RestTestCase {

    private static final String PREFIX = getIdentifierPrefix("RepositoryS3EcsCredentialsRestIT");
    private static final String BUCKET = PREFIX + "bucket";
    private static final String BASE_PATH = PREFIX + "base_path";
    private static final String CLIENT = "ecs_credentials_client";

    private static final Supplier<String> regionSupplier = new DynamicRegionSupplier();
    private static final DynamicAwsCredentials dynamicCredentials = new DynamicAwsCredentials(regionSupplier, "s3");

    private static final Ec2ImdsHttpFixture ec2ImdsHttpFixture = new Ec2ImdsHttpFixture(
        new Ec2ImdsServiceBuilder(Ec2ImdsVersion.V1).newCredentialsConsumer(dynamicCredentials::addValidCredentials)
            .alternativeCredentialsEndpoints(Set.of("/ecs_credentials_endpoint"))
    );

    private static final S3HttpFixture s3Fixture = new S3HttpFixture(true, BUCKET, BASE_PATH, dynamicCredentials::isAuthorized);

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-s3")
        .setting("s3.client." + CLIENT + ".endpoint", s3Fixture::getAddress)
        .environment("AWS_CONTAINER_CREDENTIALS_FULL_URI", () -> ec2ImdsHttpFixture.getAddress() + "/ecs_credentials_endpoint")
        .environment("AWS_REGION", regionSupplier) // Region is supplied by environment variable when running in ECS
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(ec2ImdsHttpFixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected String getBucketName() {
        return BUCKET;
    }

    @Override
    protected String getBasePath() {
        return BASE_PATH;
    }

    @Override
    protected String getClientName() {
        return CLIENT;
    }
}

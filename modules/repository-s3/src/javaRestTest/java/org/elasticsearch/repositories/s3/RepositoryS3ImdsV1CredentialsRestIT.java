/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import fixture.aws.imds.Ec2ImdsHttpFixture;
import fixture.aws.imds.Ec2ImdsServiceBuilder;
import fixture.aws.imds.Ec2ImdsVersion;
import fixture.s3.DynamicS3Credentials;
import fixture.s3.S3HttpFixture;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.fixtures.testcontainers.TestContainersThreadFilter;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
@ThreadLeakScope(ThreadLeakScope.Scope.NONE) // https://github.com/elastic/elasticsearch/issues/102482
public class RepositoryS3ImdsV1CredentialsRestIT extends AbstractRepositoryS3RestTestCase {

    private static final String PREFIX = getIdentifierPrefix("RepositoryS3ImdsV1CredentialsRestIT");
    private static final String BUCKET = PREFIX + "bucket";
    private static final String BASE_PATH = PREFIX + "base_path";
    private static final String CLIENT = "imdsv1_credentials_client";

    private static final DynamicS3Credentials dynamicS3Credentials = new DynamicS3Credentials();

    private static final Ec2ImdsHttpFixture ec2ImdsHttpFixture = new Ec2ImdsHttpFixture(
        new Ec2ImdsServiceBuilder(Ec2ImdsVersion.V1).newCredentialsConsumer(dynamicS3Credentials::addValidCredentials)
    );

    private static final S3HttpFixture s3Fixture = new S3HttpFixture(true, BUCKET, BASE_PATH, dynamicS3Credentials::isAuthorized);

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-s3")
        .setting("s3.client." + CLIENT + ".endpoint", s3Fixture::getAddress)
        .systemProperty("com.amazonaws.sdk.ec2MetadataServiceEndpointOverride", ec2ImdsHttpFixture::getAddress)
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(ec2ImdsHttpFixture).around(s3Fixture).around(cluster);

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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import fixture.aws.DynamicRegionSupplier;
import fixture.s3.S3HttpFixture;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.fixtures.testcontainers.TestContainersThreadFilter;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.function.Supplier;

import static fixture.aws.AwsCredentialsUtils.fixedAccessKey;
import static org.hamcrest.Matchers.startsWith;

@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
@ThreadLeakScope(ThreadLeakScope.Scope.NONE) // https://github.com/elastic/elasticsearch/issues/102482
public class RepositoryS3ExplicitProtocolRestIT extends AbstractRepositoryS3RestTestCase {

    private static final String PREFIX = getIdentifierPrefix("RepositoryS3ExplicitProtocolRestIT");
    private static final String BUCKET = PREFIX + "bucket";
    private static final String BASE_PATH = PREFIX + "base_path";
    private static final String ACCESS_KEY = PREFIX + "access-key";
    private static final String SECRET_KEY = PREFIX + "secret-key";
    private static final String CLIENT = "explicit_protocol_client";

    private static final Supplier<String> regionSupplier = new DynamicRegionSupplier();
    private static final S3HttpFixture s3Fixture = new S3HttpFixture(
        true,
        BUCKET,
        BASE_PATH,
        fixedAccessKey(ACCESS_KEY, regionSupplier, "s3")
    );

    private static String getEndpoint() {
        final var s3FixtureAddress = s3Fixture.getAddress();
        assertThat(s3FixtureAddress, startsWith("http://"));
        return s3FixtureAddress.substring("http://".length());
    }

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-s3")
        .systemProperty("aws.region", regionSupplier)
        .keystore("s3.client." + CLIENT + ".access_key", ACCESS_KEY)
        .keystore("s3.client." + CLIENT + ".secret_key", SECRET_KEY)
        .setting("s3.client." + CLIENT + ".endpoint", RepositoryS3ExplicitProtocolRestIT::getEndpoint)
        .setting("s3.client." + CLIENT + ".protocol", () -> "http")
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(cluster);

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

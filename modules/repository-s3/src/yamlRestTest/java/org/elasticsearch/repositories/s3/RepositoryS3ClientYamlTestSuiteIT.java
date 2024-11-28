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
import fixture.s3.DynamicS3Credentials;
import fixture.s3.S3HttpFixture;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.fixtures.testcontainers.TestContainersThreadFilter;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.Set;

@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
@ThreadLeakScope(ThreadLeakScope.Scope.NONE) // https://github.com/elastic/elasticsearch/issues/102482
public class RepositoryS3ClientYamlTestSuiteIT extends AbstractRepositoryS3ClientYamlTestSuiteIT {

    private static final String HASHED_SEED = Integer.toString(Murmur3HashFunction.hash(System.getProperty("tests.seed")));
    private static final String TEMPORARY_SESSION_TOKEN = "session_token-" + HASHED_SEED;

    private static final S3HttpFixture s3Fixture = new S3HttpFixture();

    private static final S3HttpFixture s3HttpFixtureWithSessionToken = new S3HttpFixture(
        true,
        "session_token_bucket",
        "session_token_base_path_integration_tests",
        S3HttpFixture.fixedAccessKeyAndToken(System.getProperty("s3TemporaryAccessKey"), TEMPORARY_SESSION_TOKEN)
    );

    private static final DynamicS3Credentials dynamicS3Credentials = new DynamicS3Credentials();

    private static final Ec2ImdsHttpFixture ec2ImdsHttpFixture = new Ec2ImdsHttpFixture(
        dynamicS3Credentials::addValidCredentials,
        Set.of()
    );

    private static final S3HttpFixture s3HttpFixtureWithImdsSessionToken = new S3HttpFixture(
        true,
        "ec2_bucket",
        "ec2_base_path",
        dynamicS3Credentials::isAuthorized
    );

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-s3")
        .keystore("s3.client.integration_test_permanent.access_key", System.getProperty("s3PermanentAccessKey"))
        .keystore("s3.client.integration_test_permanent.secret_key", System.getProperty("s3PermanentSecretKey"))
        .keystore("s3.client.integration_test_temporary.access_key", System.getProperty("s3TemporaryAccessKey"))
        .keystore("s3.client.integration_test_temporary.secret_key", System.getProperty("s3TemporarySecretKey"))
        .keystore("s3.client.integration_test_temporary.session_token", TEMPORARY_SESSION_TOKEN)
        .setting("s3.client.integration_test_permanent.endpoint", s3Fixture::getAddress)
        .setting("s3.client.integration_test_temporary.endpoint", s3HttpFixtureWithSessionToken::getAddress)
        .setting("s3.client.integration_test_ec2.endpoint", s3HttpFixtureWithImdsSessionToken::getAddress)
        .systemProperty("com.amazonaws.sdk.ec2MetadataServiceEndpointOverride", ec2ImdsHttpFixture::getAddress)
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture)
        .around(s3HttpFixtureWithSessionToken)
        .around(s3HttpFixtureWithImdsSessionToken)
        .around(ec2ImdsHttpFixture)
        .around(cluster);

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters(
            new String[] {
                "repository_s3/10_basic",
                "repository_s3/20_repository_permanent_credentials",
                "repository_s3/30_repository_temporary_credentials",
                "repository_s3/40_repository_ec2_credentials" }
        );
    }

    public RepositoryS3ClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}

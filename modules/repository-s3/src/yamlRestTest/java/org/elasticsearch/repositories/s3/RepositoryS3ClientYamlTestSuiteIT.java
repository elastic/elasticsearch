/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.s3;

import fixture.s3.S3HttpFixture;
import fixture.s3.S3HttpFixtureWithEC2;
import fixture.s3.S3HttpFixtureWithSessionToken;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.fixtures.testcontainers.TestContainersThreadFilter;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
@ThreadLeakScope(ThreadLeakScope.Scope.NONE) // https://github.com/elastic/elasticsearch/issues/102482
public class RepositoryS3ClientYamlTestSuiteIT extends AbstractRepositoryS3ClientYamlTestSuiteIT {

    public static final S3HttpFixture s3Fixture = new S3HttpFixture();
    public static final S3HttpFixtureWithSessionToken s3HttpFixtureWithSessionToken = new S3HttpFixtureWithSessionToken();
    public static final S3HttpFixtureWithEC2 s3Ec2 = new S3HttpFixtureWithEC2();

    private static final String s3TemporarySessionToken = "session_token";

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-s3")
        .keystore("s3.client.integration_test_permanent.access_key", System.getProperty("s3PermanentAccessKey"))
        .keystore("s3.client.integration_test_permanent.secret_key", System.getProperty("s3PermanentSecretKey"))
        .keystore("s3.client.integration_test_temporary.access_key", System.getProperty("s3TemporaryAccessKey"))
        .keystore("s3.client.integration_test_temporary.secret_key", System.getProperty("s3TemporarySecretKey"))
        .keystore("s3.client.integration_test_temporary.session_token", s3TemporarySessionToken)
        .setting("s3.client.integration_test_permanent.endpoint", s3Fixture::getAddress)
        .setting("s3.client.integration_test_temporary.endpoint", s3HttpFixtureWithSessionToken::getAddress)
        .setting("s3.client.integration_test_ec2.endpoint", s3Ec2::getAddress)
        .systemProperty("com.amazonaws.sdk.ec2MetadataServiceEndpointOverride", s3Ec2::getAddress)
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(s3Ec2).around(s3HttpFixtureWithSessionToken).around(cluster);

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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.s3;

import fixture.s3.S3HttpFixture;
import fixture.s3.S3HttpFixtureWithSTS;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

public class RepositoryS3StsClientYamlTestSuiteIT extends AbstractRepositoryS3ClientYamlTestSuiteIT {

    public static final S3HttpFixture s3Fixture = new S3HttpFixture();
    private static final S3HttpFixtureWithSTS s3Sts = new S3HttpFixtureWithSTS();

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-s3")
        .setting("s3.client.integration_test_sts.endpoint", s3Sts::getAddress)
        .systemProperty("com.amazonaws.sdk.stsMetadataServiceEndpointOverride", () -> s3Sts.getAddress() + "/assume-role-with-web-identity")
        .configFile("repository-s3/aws-web-identity-token-file", Resource.fromClasspath("aws-web-identity-token-file"))
        .environment("AWS_WEB_IDENTITY_TOKEN_FILE", System.getProperty("awsWebIdentityTokenExternalLocation"))
        // // The AWS STS SDK requires the role and session names to be set. We can verify that they are sent to S3S in the
        // // S3HttpFixtureWithSTS fixture
        .environment("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/FederatedWebIdentityRole")
        .environment("AWS_ROLE_SESSION_NAME", "sts-fixture-test")
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(s3Sts).around(cluster);

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters(new String[] { "repository_s3/60_repository_sts_credentials" });
    }

    public RepositoryS3StsClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}

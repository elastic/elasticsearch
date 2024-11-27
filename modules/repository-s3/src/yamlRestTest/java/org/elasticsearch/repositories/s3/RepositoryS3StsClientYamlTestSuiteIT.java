/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import fixture.aws.sts.AwsStsHttpFixture;
import fixture.s3.DynamicS3Credentials;
import fixture.s3.S3HttpFixture;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

public class RepositoryS3StsClientYamlTestSuiteIT extends AbstractRepositoryS3ClientYamlTestSuiteIT {

    private static final DynamicS3Credentials dynamicS3Credentials = new DynamicS3Credentials();

    private static final S3HttpFixture s3HttpFixture = new S3HttpFixture(
        true,
        "sts_bucket",
        "sts_base_path",
        dynamicS3Credentials::isAuthorized
    );

    private static final AwsStsHttpFixture stsHttpFixture = new AwsStsHttpFixture(dynamicS3Credentials::addValidCredentials, """
        Atza|IQEBLjAsAhRFiXuWpUXuRvQ9PZL3GMFcYevydwIUFAHZwXZXXXXXXXXJnrulxKDHwy87oGKPznh0D6bEQZTSCzyoCtL_8S07pLpr0zMbn6w1lfVZKNTBdDans\
        FBmtGnIsIapjI6xKR02Yc_2bQ8LZbUXSGm6Ry6_BG7PrtLZtj_dfCTj92xNGed-CrKqjG7nPBjNIL016GGvuS5gSvPRUxWES3VYfm1wl7WTI7jn-Pcb6M-buCgHhFO\
        zTQxod27L9CqnOLio7N3gZAGpsp6n1-AJBOCJckcyXe2c6uD0srOJeZlKUm2eTDVMf8IehDVI0r1QOnTV6KzzAI3OY87Vd_cVMQ""");

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-s3")
        .setting("s3.client.integration_test_sts.endpoint", s3HttpFixture::getAddress)
        .systemProperty(
            "com.amazonaws.sdk.stsMetadataServiceEndpointOverride",
            () -> stsHttpFixture.getAddress() + "/assume-role-with-web-identity"
        )
        .configFile("repository-s3/aws-web-identity-token-file", Resource.fromClasspath("aws-web-identity-token-file"))
        .environment("AWS_WEB_IDENTITY_TOKEN_FILE", System.getProperty("awsWebIdentityTokenExternalLocation"))
        // // The AWS STS SDK requires the role and session names to be set. We can verify that they are sent to S3S in the
        // // S3HttpFixtureWithSTS fixture
        .environment("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/FederatedWebIdentityRole")
        .environment("AWS_ROLE_SESSION_NAME", "sts-fixture-test")
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3HttpFixture).around(stsHttpFixture).around(cluster);

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

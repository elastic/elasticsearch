/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.s3;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.junit.ClassRule;

public class RepositoryS3RegionalStsClientYamlTestSuiteIT extends AbstractRepositoryS3ClientYamlTestSuiteIT {
    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-s3")
        .configFile("repository-s3/aws-web-identity-token-file", Resource.fromClasspath("aws-web-identity-token-file"))
        .environment("AWS_WEB_IDENTITY_TOKEN_FILE", System.getProperty("awsWebIdentityTokenExternalLocation"))
        // The AWS STS SDK requires the role and session names to be set. We can verify that they are sent to S3S in the
        // S3HttpFixtureWithSTS fixture
        .environment("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/FederatedWebIdentityRole")
        .environment("AWS_ROLE_SESSION_NAME", "sts-fixture-test")
        .environment("AWS_STS_REGIONAL_ENDPOINTS", "regional")
        .environment("AWS_REGION", "ap-southeast-2")
        .build();

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters(new String[] { "repository_s3/10_basic" });
    }

    public RepositoryS3RegionalStsClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}

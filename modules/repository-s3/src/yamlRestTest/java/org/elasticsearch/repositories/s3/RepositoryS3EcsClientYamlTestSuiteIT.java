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

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.Set;

public class RepositoryS3EcsClientYamlTestSuiteIT extends AbstractRepositoryS3ClientYamlTestSuiteIT {

    private static final DynamicS3Credentials dynamicS3Credentials = new DynamicS3Credentials();

    private static final Ec2ImdsHttpFixture ec2ImdsHttpFixture = new Ec2ImdsHttpFixture(
        dynamicS3Credentials::addValidCredentials,
        Set.of("/ecs_credentials_endpoint")
    );

    private static final S3HttpFixture s3Fixture = new S3HttpFixture(
        true,
        "ecs_bucket",
        "ecs_base_path",
        dynamicS3Credentials::isAuthorized
    );

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-s3")
        .setting("s3.client.integration_test_ecs.endpoint", s3Fixture::getAddress)
        .environment("AWS_CONTAINER_CREDENTIALS_FULL_URI", () -> ec2ImdsHttpFixture.getAddress() + "/ecs_credentials_endpoint")
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(ec2ImdsHttpFixture).around(cluster);

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters(new String[] { "repository_s3/50_repository_ecs_credentials" });
    }

    public RepositoryS3EcsClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}

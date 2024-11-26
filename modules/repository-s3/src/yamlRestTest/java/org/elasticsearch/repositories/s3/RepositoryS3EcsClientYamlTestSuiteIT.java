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
import fixture.s3.S3HttpFixtureWithSessionToken;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.Set;

public class RepositoryS3EcsClientYamlTestSuiteIT extends AbstractRepositoryS3ClientYamlTestSuiteIT {

    private static final String HASHED_SEED = Integer.toString(Murmur3HashFunction.hash(System.getProperty("tests.seed")));
    private static final String ECS_ACCESS_KEY = "ecs-access-key-" + HASHED_SEED;
    private static final String ECS_SESSION_TOKEN = "ecs-session-token-" + HASHED_SEED;

    private static final S3HttpFixtureWithSessionToken s3Fixture = new S3HttpFixtureWithSessionToken(
        "ecs_bucket",
        "ecs_base_path",
        ECS_ACCESS_KEY,
        ECS_SESSION_TOKEN
    );

    private static final Ec2ImdsHttpFixture ec2ImdsHttpFixture = new Ec2ImdsHttpFixture(
        ECS_ACCESS_KEY,
        ECS_SESSION_TOKEN,
        Set.of("/ecs_credentials_endpoint")
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

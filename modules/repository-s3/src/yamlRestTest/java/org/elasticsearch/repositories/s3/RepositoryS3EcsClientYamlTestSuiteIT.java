/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.s3;

import fixture.s3.S3HttpFixtureWithECS;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

public class RepositoryS3EcsClientYamlTestSuiteIT extends AbstractRepositoryS3ClientYamlTestSuiteIT {
    private static final S3HttpFixtureWithECS s3Ecs = new S3HttpFixtureWithECS();

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-s3")
        .setting("s3.client.integration_test_ecs.endpoint", s3Ecs::getAddress)
        .environment("AWS_CONTAINER_CREDENTIALS_FULL_URI", () -> (s3Ecs.getAddress() + "/ecs_credentials_endpoint"))
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Ecs).around(cluster);

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

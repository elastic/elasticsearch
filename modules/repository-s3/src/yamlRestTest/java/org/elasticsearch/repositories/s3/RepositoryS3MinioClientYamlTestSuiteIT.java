/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.s3;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.fixtures.minio.MinioFixtureTestContainer;
import org.elasticsearch.test.fixtures.testcontainers.TestContainersThreadFilter;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.List;

@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
public class RepositoryS3MinioClientYamlTestSuiteIT extends AbstractRepositoryS3ClientYamlTestSuiteIT {

    public static MinioFixtureTestContainer minioFixtureTestContainer = new MinioFixtureTestContainer(USE_FIXTURE);

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-s3")
        .keystore("s3.client.integration_test_permanent.access_key", System.getProperty("s3PermanentAccessKey"))
        .keystore("s3.client.integration_test_permanent.secret_key", System.getProperty("s3PermanentSecretKey"))
        .setting("s3.client.integration_test_permanent.endpoint", () -> minioFixtureTestContainer.getServiceUrl())
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(minioFixtureTestContainer).around(cluster);

    @BeforeClass
    public static void onlyWhenRunWithTestFixture() {
        assumeTrue("Only run with fixture enabled", USE_FIXTURE);
    }

    protected List<String> blackListed() {
        return List.of(
            "repository_s3/30_repository_temporary_credentials/*",
            "repository_s3/40_repository_ec2_credentials/*",
            "repository_s3/50_repository_ecs_credentials/*",
            "repository_s3/60_repository_sts_credentials/*"
        );
    }

    public RepositoryS3MinioClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}

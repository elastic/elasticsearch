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
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.fixtures.minio.MinioTestContainer;
import org.elasticsearch.test.fixtures.testcontainers.TestContainersThreadFilter;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
public class RepositoryS3MinioClientYamlTestSuiteIT extends AbstractRepositoryS3ClientYamlTestSuiteIT {

    public static MinioTestContainer minio = new MinioTestContainer();

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-s3")
        .keystore("s3.client.integration_test_permanent.access_key", System.getProperty("s3PermanentAccessKey"))
        .keystore("s3.client.integration_test_permanent.secret_key", System.getProperty("s3PermanentSecretKey"))
        .setting("s3.client.integration_test_permanent.endpoint", () -> minio.getAddress())
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(minio).around(cluster);

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters(new String[] { "repository_s3/10_basic", "repository_s3/20_repository_permanent_credentials" });
    }

    public RepositoryS3MinioClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}

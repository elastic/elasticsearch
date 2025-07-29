/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import fixture.s3.S3HttpFixture;

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

import static fixture.aws.AwsCredentialsUtils.ANY_REGION;
import static fixture.aws.AwsCredentialsUtils.fixedAccessKey;

@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
@ThreadLeakScope(ThreadLeakScope.Scope.NONE) // https://github.com/elastic/elasticsearch/issues/102482
public class RepositoryS3ClientYamlTestSuiteIT extends AbstractRepositoryS3ClientYamlTestSuiteIT {

    private static final String ACCESS_KEY = "RepositoryS3ClientYamlTestSuiteIT-access-key";
    private static final String SECRET_KEY = "RepositoryS3ClientYamlTestSuiteIT-secret-key";

    private static final S3HttpFixture s3Fixture = new S3HttpFixture(
        true,
        "bucket",
        "base_path_integration_tests",
        fixedAccessKey(ACCESS_KEY, ANY_REGION, "s3")
    );

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-s3")
        .keystore("s3.client.integration_test_permanent.access_key", ACCESS_KEY)
        .keystore("s3.client.integration_test_permanent.secret_key", SECRET_KEY)
        .setting("s3.client.integration_test_permanent.endpoint", s3Fixture::getAddress)
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(cluster);

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters(new String[] { "repository_s3/10_basic", "repository_s3/20_repository_permanent_credentials" });
    }

    public RepositoryS3ClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}

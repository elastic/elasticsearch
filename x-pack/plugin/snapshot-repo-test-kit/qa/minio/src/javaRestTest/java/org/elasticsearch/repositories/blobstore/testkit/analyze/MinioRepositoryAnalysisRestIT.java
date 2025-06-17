/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.repositories.blobstore.testkit.analyze;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.fixtures.minio.MinioTestContainer;
import org.elasticsearch.test.fixtures.testcontainers.TestContainersThreadFilter;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
public class MinioRepositoryAnalysisRestIT extends AbstractRepositoryAnalysisRestTestCase {

    public static final MinioTestContainer minioFixture = new MinioTestContainer(
        true,
        "s3_test_access_key",
        "s3_test_secret_key",
        "bucket"
    );

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .keystore("s3.client.repository_test_kit.access_key", "s3_test_access_key")
        .keystore("s3.client.repository_test_kit.secret_key", "s3_test_secret_key")
        .setting("s3.client.repository_test_kit.endpoint", minioFixture::getAddress)
        .setting("xpack.security.enabled", "false")
        // Skip listing of pre-existing uploads during a CAS because MinIO sometimes leaks them; also reduce the delay before proceeding
        // TODO do not set these if running a MinIO version in which https://github.com/minio/minio/issues/21189 is fixed
        .setting("repository_s3.compare_and_exchange.time_to_live", "-1")
        .setting("repository_s3.compare_and_exchange.anti_contention_delay", "100ms")
        .setting("xpack.ml.enabled", "false")
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(minioFixture).around(cluster);

    @Override
    protected String repositoryType() {
        return "s3";
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings repositorySettings() {
        final String bucket = "bucket";
        final String basePath = "repository_test_kit_tests";
        return Settings.builder().put("client", "repository_test_kit").put("bucket", bucket).put("base_path", basePath).build();
    }
}

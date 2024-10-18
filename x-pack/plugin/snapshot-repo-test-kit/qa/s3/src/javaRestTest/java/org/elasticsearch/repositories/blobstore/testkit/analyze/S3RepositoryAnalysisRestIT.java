/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.repositories.blobstore.testkit.analyze;

import fixture.s3.S3HttpFixture;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.not;

public class S3RepositoryAnalysisRestIT extends AbstractRepositoryAnalysisRestTestCase {

    static final boolean USE_FIXTURE = Boolean.parseBoolean(System.getProperty("tests.use.fixture", "true"));

    public static final S3HttpFixture s3Fixture = new S3HttpFixture(USE_FIXTURE);

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .keystore("s3.client.repo_test_kit.access_key", System.getProperty("s3AccessKey"))
        .keystore("s3.client.repo_test_kit.secret_key", System.getProperty("s3SecretKey"))
        .setting("s3.client.repo_test_kit.protocol", () -> "http", (n) -> USE_FIXTURE)
        .setting("s3.client.repo_test_kit.endpoint", s3Fixture::getAddress, (n) -> USE_FIXTURE)
        .setting("xpack.security.enabled", "false")
        // Additional tracing related to investigation into https://github.com/elastic/elasticsearch/issues/102294
        .setting("logger.org.elasticsearch.repositories.s3", "TRACE")
        .setting("logger.org.elasticsearch.repositories.blobstore.testkit", "TRACE")
        .setting("logger.com.amazonaws.request", "DEBUG")
        .setting("logger.org.apache.http.wire", "DEBUG")
        // Necessary to permit setting the above two restricted loggers to DEBUG
        .jvmArg("-Des.insecure_network_trace_enabled=true")
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected String repositoryType() {
        return "s3";
    }

    @Override
    protected Settings repositorySettings() {
        final String bucket = System.getProperty("test.s3.bucket");
        assertThat(bucket, not(blankOrNullString()));

        final String basePath = System.getProperty("test.s3.base_path");
        assertThat(basePath, not(blankOrNullString()));

        return Settings.builder()
            .put("client", "repo_test_kit")
            .put("bucket", bucket)
            .put("base_path", basePath)
            .put("delete_objects_max_size", between(1, 1000))
            .build();
    }
}

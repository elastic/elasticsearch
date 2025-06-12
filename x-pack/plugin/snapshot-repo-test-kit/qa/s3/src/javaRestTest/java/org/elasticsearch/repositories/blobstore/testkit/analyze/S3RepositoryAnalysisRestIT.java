/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.repositories.blobstore.testkit.analyze;

import fixture.s3.S3HttpFixture;
import fixture.s3.S3HttpHandler;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class S3RepositoryAnalysisRestIT extends AbstractRepositoryAnalysisRestTestCase {

    static final boolean USE_FIXTURE = Boolean.parseBoolean(System.getProperty("tests.use.fixture", "true"));

    public static final S3HttpFixture s3Fixture = new S3HttpFixture(USE_FIXTURE) {
        @Override
        protected HttpHandler createHandler() {
            final var delegateHandler = asInstanceOf(S3HttpHandler.class, super.createHandler());
            final var repoAnalysisStarted = new AtomicBoolean();
            return exchange -> {
                ensurePurposeParameterPresent(exchange, repoAnalysisStarted);
                delegateHandler.handle(exchange);
            };
        }
    };

    private static void ensurePurposeParameterPresent(HttpExchange exchange, AtomicBoolean repoAnalysisStarted) {
        final var requestPath = exchange.getRequestURI().getPath();
        if (requestPath.startsWith("/bucket/base_path_integration_tests/temp-analysis-")) {
            repoAnalysisStarted.set(true);
        }
        final var queryString = exchange.getRequestURI().getQuery();
        if (repoAnalysisStarted.get() == false) {
            if (Regex.simpleMatch("/bucket/base_path_integration_tests/tests-*/master.dat", requestPath)
                || Regex.simpleMatch("/bucket/base_path_integration_tests/tests-*/data-*.dat", requestPath)
                || queryString.contains("prefix=base_path_integration_tests/tests-")
                || queryString.contains("delete")) {
                // verify repository is not part of repo analysis so will have different/missing x-purpose parameter
                return;
            }
            if (queryString.contains("prefix=base_path_integration_tests/index-")) {
                // getRepositoryData looking for root index-N blob will have different/missing x-purpose parameter
                return;
            }
            repoAnalysisStarted.set(true);
        }
        assertThat(queryString, containsString("x-purpose=RepositoryAnalysis"));
    }

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .keystore("s3.client.repo_test_kit.access_key", System.getProperty("s3AccessKey"))
        .keystore("s3.client.repo_test_kit.secret_key", System.getProperty("s3SecretKey"))
        .setting("s3.client.repo_test_kit.protocol", () -> "http", (n) -> USE_FIXTURE)
        .setting("s3.client.repo_test_kit.endpoint", s3Fixture::getAddress, (n) -> USE_FIXTURE)
        .setting("xpack.security.enabled", "false")
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
            .put(randomFrom(Settings.EMPTY, Settings.builder().put("add_purpose_custom_query_parameter", randomBoolean()).build()))
            .build();
    }
}

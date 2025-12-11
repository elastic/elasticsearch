/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.analyze;

import fixture.aws.DynamicRegionSupplier;
import fixture.s3.S3ConsistencyModel;
import fixture.s3.S3HttpFixture;
import fixture.s3.S3HttpHandler;

import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;

import java.util.function.Supplier;

import static fixture.aws.AwsCredentialsUtils.fixedAccessKey;
import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.not;

public abstract class AbstractS3RepositoryAnalysisRestTestCase extends AbstractRepositoryAnalysisRestTestCase {

    static final boolean USE_FIXTURE = Booleans.parseBoolean(System.getProperty("tests.use.fixture", "true"));

    protected static final Supplier<String> regionSupplier = new DynamicRegionSupplier();

    protected static class RepositoryAnalysisHttpFixture extends S3HttpFixture {
        RepositoryAnalysisHttpFixture(S3ConsistencyModel consistencyModel) {
            super(
                USE_FIXTURE,
                "bucket",
                "base_path_integration_tests",
                () -> consistencyModel,
                fixedAccessKey("s3_test_access_key", regionSupplier, "s3")
            );
        }

        private volatile boolean repoAnalysisStarted;

        @Override
        protected HttpHandler createHandler() {
            final var delegateHandler = asInstanceOf(S3HttpHandler.class, super.createHandler());
            return exchange -> {
                ensurePurposeParameterPresent(delegateHandler.parseRequest(exchange));
                delegateHandler.handle(exchange);
            };
        }

        private void ensurePurposeParameterPresent(S3HttpHandler.S3Request request) {
            if (request.path().startsWith("/bucket/base_path_integration_tests/temp-analysis-")) {
                repoAnalysisStarted = true;
            }
            if (repoAnalysisStarted == false) {
                if (Regex.simpleMatch("/bucket/base_path_integration_tests/tests-*/master.dat", request.path())
                    || Regex.simpleMatch("/bucket/base_path_integration_tests/tests-*/data-*.dat", request.path())
                    || (request.isListObjectsRequest()
                        && request.getQueryParamOnce("prefix").startsWith("base_path_integration_tests/tests-"))
                    || (request.isMultiObjectDeleteRequest())) {
                    // verify repository is not part of repo analysis so will have different/missing x-purpose parameter
                    return;
                }
                if (request.isListObjectsRequest() && request.getQueryParamOnce("prefix").equals("base_path_integration_tests/index-")) {
                    // getRepositoryData looking for root index-N blob will have different/missing x-purpose parameter
                    return;
                }
                repoAnalysisStarted = true;
            }
            assertTrue(request.toString(), request.hasQueryParamOnce("x-purpose"));
            assertEquals(request.toString(), "RepositoryAnalysis", request.getQueryParamOnce("x-purpose"));
        }
    }

    protected static final String CLIENT_NAME = "repo_test_kit";

    protected static ElasticsearchCluster buildCluster(S3HttpFixture s3HttpFixture) {
        final var clientPrefix = "s3.client." + CLIENT_NAME + ".";
        return ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .keystore(clientPrefix + "access_key", System.getProperty("s3AccessKey"))
            .keystore(clientPrefix + "secret_key", System.getProperty("s3SecretKey"))
            .setting(clientPrefix + "protocol", () -> "http", n -> USE_FIXTURE)
            .setting(clientPrefix + "region", regionSupplier, n -> USE_FIXTURE)
            .setting(clientPrefix + "add_purpose_custom_query_parameter", () -> randomFrom("true", "false"), n -> randomBoolean())
            .setting(clientPrefix + "endpoint", s3HttpFixture::getAddress, n -> USE_FIXTURE)
            .setting(
                "repository_s3.compare_and_exchange.anti_contention_delay",
                () -> randomFrom("1s" /* == default */, "1ms"),
                n -> randomBoolean()
            )
            .setting("xpack.security.enabled", "false")
            .setting("thread_pool.snapshot.max", "10")
            .build();
    }

    abstract S3ConsistencyModel consistencyModel();

    @Override
    protected Settings repositorySettings() {
        final String bucket = System.getProperty("test.s3.bucket");
        assertThat(bucket, not(blankOrNullString()));

        final String basePath = System.getProperty("test.s3.base_path");
        assertThat(basePath, not(blankOrNullString()));

        return Settings.builder()
            .put("client", CLIENT_NAME)
            .put("bucket", bucket)
            .put("base_path", basePath)
            .put("delete_objects_max_size", between(1, 1000))
            .put("buffer_size", ByteSizeValue.ofMb(5)) // so some uploads are multipart ones
            .put("max_copy_size_before_multipart", ByteSizeValue.ofMb(5))
            // verify we always set the x-purpose header even if disabled for other repository operations
            .put(randomBooleanSetting("add_purpose_custom_query_parameter"))
            // this parameter is ignored for repo analysis
            .put("unsafely_incompatible_with_s3_conditional_writes", consistencyModel().hasConditionalWrites() == false)
            .build();
    }

    private static Settings randomBooleanSetting(String settingKey) {
        return randomFrom(Settings.EMPTY, Settings.builder().put(settingKey, randomBoolean()).build());
    }

    @Override
    protected String repositoryType() {
        return "s3";
    }

}

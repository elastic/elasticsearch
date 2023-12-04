/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.s3.advancedstoragetiering;

import fixture.s3.S3HttpFixture;

import com.sun.net.httpserver.HttpExchange;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;

public class S3AdvancedStorageTieringRestTest extends ESRestTestCase {

    private static final Logger logger = LogManager.getLogger(S3AdvancedStorageTieringRestTest.class);

    private static final String BUCKET = "test_bucket";
    private static final String BASE_PATH = "test_base_path";
    private static final String CLIENT_NAME = "advanced_storage_tiering";
    private static final String ACCESS_KEY = "test_access_key";

    public static final S3HttpFixture s3Fixture = new S3HttpFixture(true, BUCKET, BASE_PATH, ACCESS_KEY) {
        @Override
        protected void validatePutObjectRequest(HttpExchange exchange) {
            logger.error("validatePutObjectRequest: {} {}", exchange.getRequestMethod(), exchange.getRequestURI());
        }

        @Override
        protected void validateInitiateMultipartUploadRequest(HttpExchange exchange) {
            logger.error("validateInitiateMultipartUploadRequest: {} {}", exchange.getRequestMethod(), exchange.getRequestURI());
        }
    };

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "false")
        .keystore("s3.client." + CLIENT_NAME + ".access_key", ACCESS_KEY)
        .keystore("s3.client." + CLIENT_NAME + ".secret_key", "s3_test_secret_key")
        .setting("s3.client." + CLIENT_NAME + ".protocol", "http")
        .setting("s3.client." + CLIENT_NAME + ".endpoint", s3Fixture::getAddress)
        .setting("logger.org.apache.http.wire", "DEBUG")
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testAdvancedTiering() throws IOException {
        final var repoName = randomIdentifier();

        registerRepository(
            repoName,
            "s3",
            true,
            Settings.builder()
                .put("client", CLIENT_NAME)
                .put("bucket", BUCKET)
                .put("base_path", BASE_PATH)
                .put("storage_class", "onezone_ia")
                .put("metadata_storage_class", "standard_ia")
                .build()
        );

        fail("boom");
    }
}

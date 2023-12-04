/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.s3.advancedstoragetiering;

import com.sun.net.httpserver.HttpExchange;

import fixture.s3.S3HttpFixture;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

public class S3AdvancedStorageTieringRestTest extends ESRestTestCase {

    private static final Logger logger = LogManager.getLogger(S3AdvancedStorageTieringRestTest.class);

    public static final S3HttpFixture s3Fixture = new S3HttpFixture(true) {
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
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testAdvancedTiering() {

    }
}

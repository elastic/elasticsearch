/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import fixture.aws.DynamicRegionSupplier;
import fixture.s3.S3ConsistencyModel;
import fixture.s3.S3HttpFixture;
import fixture.s3.S3HttpHandler;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.fixtures.testcontainers.TestContainersThreadFilter;
import org.elasticsearch.test.fixtures.tls.TestTlsCertificate;
import org.elasticsearch.test.fixtures.tls.TestTrustStore;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static fixture.aws.AwsCredentialsUtils.fixedAccessKey;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.matchesPattern;

@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
public class RepositoryS3OverHttpsRestIT extends AbstractRepositoryS3RestTestCase {

    private static final String PREFIX = getIdentifierPrefix("RepositoryS3OverHttpsRestIT");
    private static final String BUCKET = PREFIX + "bucket";
    private static final String BASE_PATH = PREFIX + "base_path";
    private static final String ACCESS_KEY = PREFIX + "access-key";
    private static final String SECRET_KEY = PREFIX + "secret-key";
    private static final String CLIENT = "https_s3_client";

    protected static final TestTlsCertificate testTlsCertificate = TestTlsCertificate.generate("localhost");

    protected static final TestTrustStore trustStore = new TestTrustStore(testTlsCertificate::getPemCertificateStream);

    private static final Supplier<String> regionSupplier = new DynamicRegionSupplier();

    private static final S3HttpFixture s3Fixture = new S3HttpFixture(
        true,
        testTlsCertificate,
        BUCKET,
        BASE_PATH,
        S3ConsistencyModel::randomConsistencyModel,
        fixedAccessKey(ACCESS_KEY, regionSupplier, "s3")
    ) {
        @Override
        protected HttpHandler createHandler() {
            final var delegate = asInstanceOf(S3HttpHandler.class, super.createHandler());
            return exchange -> {
                final var request = delegate.parseRequest(exchange);
                if ((request.isUploadPartRequest() || request.isPutObjectRequest())
                    && Optional.ofNullable(exchange.getRequestHeaders().get(S3HttpHandler.COPY_SOURCE_HEADER))
                        .orElse(List.of())
                        .isEmpty()) {
                    assertThat(
                        exchange.getRequestHeaders().getFirst(S3HttpHandler.CONTENT_SHA256_HEADER),
                        anyOf(equalTo("STREAMING-UNSIGNED-PAYLOAD-TRAILER"), matchesPattern(S3HttpHandler.SHA256_PATTERN))
                    );
                }
                delegate.handle(exchange);
            };
        }
    };

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-s3")
        .systemProperty("aws.region", regionSupplier)
        .keystore("s3.client." + CLIENT + ".access_key", ACCESS_KEY)
        .keystore("s3.client." + CLIENT + ".secret_key", SECRET_KEY)
        .setting("s3.client." + CLIENT + ".endpoint", s3Fixture::getAddress)
        .setting("s3.client." + CLIENT + ".disable_chunked_encoding", () -> randomFrom("true", "false"), ignored -> randomBoolean())
        .setting("s3.client." + CLIENT + ".path_style_access", "true")
        .systemProperty("es.insecure_network_trace_enabled", "true")
        .setting("logger.org.apache.http.wire", "TRACE")
        .apply(builder -> trustStore.apply(builder, true))
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(trustStore).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected String getBucketName() {
        return BUCKET;
    }

    @Override
    protected String getBasePath() {
        return BASE_PATH;
    }

    @Override
    protected String getClientName() {
        return CLIENT;
    }
}

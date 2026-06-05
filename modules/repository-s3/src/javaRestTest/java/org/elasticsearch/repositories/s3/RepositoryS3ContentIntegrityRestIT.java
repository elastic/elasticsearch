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

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.fixtures.testcontainers.TestContainersThreadFilter;
import org.elasticsearch.test.fixtures.tls.TestTlsCertificate;
import org.elasticsearch.test.fixtures.tls.TestTrustStore;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static fixture.aws.AwsCredentialsUtils.fixedAccessKey;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.matchesPattern;

@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
public class RepositoryS3ContentIntegrityRestIT extends AbstractRepositoryS3RestTestCase {

    private static final String PREFIX = getIdentifierPrefix("RepositoryS3ContentIntegrityRestIT");
    private static final String BUCKET = PREFIX + "bucket";
    private static final String BASE_PATH = PREFIX + "base_path";
    private static final String ACCESS_KEY = PREFIX + "access-key";
    private static final String SECRET_KEY = PREFIX + "secret-key";

    private static final Supplier<String> regionSupplier = new DynamicRegionSupplier();

    private static final TestTlsCertificate testTlsCertificate = TestTlsCertificate.generate("localhost");

    private static final TestTrustStore trustStore = new TestTrustStore(testTlsCertificate::getPemCertificateStream);

    private static final class ContentIntegrityS3HttpFixture extends S3HttpFixture {
        ContentIntegrityS3HttpFixture(TestTlsCertificate tlsCertificate) {
            super(
                true,
                tlsCertificate,
                BUCKET,
                BASE_PATH,
                S3ConsistencyModel::randomConsistencyModel,
                fixedAccessKey(ACCESS_KEY, regionSupplier, "s3")
            );
        }

        @SuppressForbidden(reason = "implementing HTTP server for test fixture")
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
                        anyOf(
                            matchesPattern(S3HttpHandler.SHA256_PATTERN),
                            equalTo(TestConfig.fromPath(request.path()).getExpectedContentSha256Header())
                        )
                    );
                }
                delegate.handle(exchange);
            };
        }
    }

    private static final S3HttpFixture httpS3Fixture = new ContentIntegrityS3HttpFixture(null);

    private static final S3HttpFixture httpsS3Fixture = new ContentIntegrityS3HttpFixture(testTlsCertificate);

    private static final List<TestConfig> TEST_CONFIGS;

    static {
        final var testConfigs = new ArrayList<TestConfig>();
        for (final boolean https : new boolean[] { false, true }) {
            for (final boolean chunkedEncoding : new boolean[] { false, true }) {
                for (final boolean alwaysSignUploads : new boolean[] { false, true }) {
                    testConfigs.add(new TestConfig(https, chunkedEncoding, alwaysSignUploads));
                }
            }
        }
        TEST_CONFIGS = List.copyOf(testConfigs);
    }

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-s3")
        .systemProperty("aws.region", regionSupplier)
        .apply(builder -> trustStore.apply(builder, true))
        .apply(builder -> {
            for (final var testConfig : TEST_CONFIGS) {
                testConfig.applyClientSettings(builder);
            }
        })
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(httpS3Fixture).around(trustStore).around(httpsS3Fixture).around(cluster);

    private final TestConfig testConfig;

    public RepositoryS3ContentIntegrityRestIT(@Name("testConfig") TestConfig testConfig) {
        this.testConfig = testConfig;
    }

    @ParametersFactory(argumentFormatting = "testConfig=%1$s")
    public static Iterable<Object[]> parameters() {
        return () -> Iterators.map(TEST_CONFIGS.iterator(), c -> new Object[] { c });
    }

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
        return testConfig.getRepositoryBasePath();
    }

    @Override
    protected String getClientName() {
        return testConfig.getClientName();
    }

    @Override
    protected Settings extraRepositorySettings() {
        return testConfig.getRepositorySettings();
    }

    /// Test suite is parametric in this config.
    public static final class TestConfig {
        private static final String DISABLE_CHUNKED_ENCODING = "disable_chunked_encoding";

        private final boolean https;
        private final boolean chunkedEncoding;

        TestConfig(boolean https, boolean chunkedEncoding) {
            this.https = https;
            this.chunkedEncoding = chunkedEncoding;
        }

        @Override
        public String toString() {
            return Strings.format("{scheme=%s,chunkedEncoding=%s}", https ? "https" : "http", chunkedEncoding);
        }

        Settings getRepositorySettings() {
            final var builder = Settings.builder();
            if (chunkedEncoding) {
                if (randomBoolean()) {
                    builder.put(DISABLE_CHUNKED_ENCODING, false);
                } else {
                    builder.putNull(DISABLE_CHUNKED_ENCODING);
                }
            } else {
                builder.put(DISABLE_CHUNKED_ENCODING, true);
            }
            return builder.build();
        }

        String getExpectedContentSha256Header() {
            return https
                ? (chunkedEncoding ? "STREAMING-UNSIGNED-PAYLOAD-TRAILER" : "UNSIGNED-PAYLOAD")
                : (chunkedEncoding ? "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER" : "STREAMING-AWS4-HMAC-SHA256-PAYLOAD");
        }

        void applyClientSettings(LocalClusterSpecBuilder<?> builder) {
            final String client = getClientName();
            builder.keystore("s3.client." + client + ".access_key", ACCESS_KEY);
            builder.keystore("s3.client." + client + ".secret_key", SECRET_KEY);
            builder.setting("s3.client." + client + ".endpoint", getS3Fixture()::getAddress);
            builder.setting("s3.client." + client + ".path_style_access", () -> "true", n -> https || randomBoolean());
        }

        S3HttpFixture getS3Fixture() {
            return https ? httpsS3Fixture : httpS3Fixture;
        }

        String getClientName() {
            return "content_integrity_client_" + (https ? "https" : "http") + "_chunked_encoding_" + chunkedEncoding;
        }

        String getRepositoryBasePath() {
            return BASE_PATH + "/" + (https ? "https" : "http") + "_chunked_encoding_" + chunkedEncoding;
        }

        static TestConfig fromPath(String path) {
            for (final var testConfig : TEST_CONFIGS) {
                if (path.contains(testConfig.getRepositoryBasePath())) {
                    return testConfig;
                }
            }
            throw new AssertionError("no case matches request URI [" + path + "]");
        }
    }
}

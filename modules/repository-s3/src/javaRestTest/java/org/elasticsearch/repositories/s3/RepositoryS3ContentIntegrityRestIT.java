/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import fixture.aws.AwsCredentialsUtils;
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
import org.hamcrest.Matcher;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static fixture.aws.DynamicIdentifierSupplier.testClassIdentifierSupplier;
import static org.elasticsearch.repositories.s3.S3ChecksumRestTestSupport.DATA_SIZE_FORCING_MULTIPART_UPLOADS;
import static org.elasticsearch.repositories.s3.S3ChecksumRestTestSupport.MULTIPART_BUFFER_SIZE;
import static org.elasticsearch.repositories.s3.S3ChecksumRestTestSupport.SINGLE_SHARD_INDEX_SETTINGS;
import static org.elasticsearch.repositories.s3.S3ChecksumRestTestSupport.UNINDEXED_PAYLOAD_MAPPING;
import static org.elasticsearch.repositories.s3.S3ChecksumRestTestSupport.assertNoChecksumRequestHeaders;
import static org.elasticsearch.repositories.s3.S3ChecksumRestTestSupport.assertSomeChecksumRequestHeader;
import static org.elasticsearch.repositories.s3.S3ChecksumRestTestSupport.createSnapshotAndAssertSuccess;
import static org.elasticsearch.repositories.s3.S3ChecksumRestTestSupport.deleteSnapshotAndAssertOk;
import static org.elasticsearch.repositories.s3.S3ChecksumRestTestSupport.indexIncompressibleData;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
public class RepositoryS3ContentIntegrityRestIT extends AbstractRepositoryS3RestTestCase {

    private static final String PREFIX = getIdentifierPrefix("RepositoryS3ContentIntegrityRestIT");
    private static final String ACCESS_KEY = PREFIX + "access-key";
    private static final String SECRET_KEY = PREFIX + "secret-key";

    private static final Supplier<String> regionSupplier = new DynamicRegionSupplier();
    private static final Supplier<String> bucketSupplier = testClassIdentifierSupplier("bucket");
    private static final Supplier<String> basePathSupplier = testClassIdentifierSupplier("base_path");

    private static final TestTlsCertificate testTlsCertificate = TestTlsCertificate.generate("localhost");

    private static final TestTrustStore trustStore = new TestTrustStore(testTlsCertificate::getPemCertificateStream);

    private static final List<TestConfig> TEST_CONFIGS;

    static {
        final var testConfigs = new ArrayList<TestConfig>();
        for (final boolean https : new boolean[] { false, true }) {
            for (final boolean chunkedEncoding : new boolean[] { false, true }) {
                for (final boolean alwaysSignRequests : new boolean[] { false, true }) {
                    for (final boolean unsafelyOmitRequestBodyChecksums : new boolean[] { false, true }) {
                        // legacy_content_md5 only changes DeleteObjects, so it doesn't need crossing with everything else. One config
                        // per scheme and signing mode is enough to show it still works next to the other settings.
                        final boolean legacyContentMd5 = chunkedEncoding && unsafelyOmitRequestBodyChecksums == false;
                        testConfigs.add(
                            new TestConfig(https, chunkedEncoding, alwaysSignRequests, legacyContentMd5, unsafelyOmitRequestBodyChecksums)
                        );
                    }
                }
            }
        }
        TEST_CONFIGS = List.copyOf(testConfigs);
    }

    /**
     * Test configs for which the fixture saw an UploadPart request. The fixture only checks the headers of the requests it sees, so
     * without this a test that never sent a part would pass without checking the multipart path at all.
     */
    private static final Set<String> CONFIGS_WITH_UPLOAD_PART_REQUESTS = ConcurrentHashMap.newKeySet();

    private static final class ContentIntegrityS3HttpFixture extends S3HttpFixture {
        ContentIntegrityS3HttpFixture(TestTlsCertificate tlsCertificate) {
            super(
                true,
                tlsCertificate,
                bucketSupplier,
                basePathSupplier,
                S3ConsistencyModel::randomConsistencyModel,
                (authorizationHeader, sessionTokenHeader) -> authorizationHeader != null
                    && TEST_CONFIGS.stream()
                        .anyMatch(
                            testConfig -> AwsCredentialsUtils.isValidAwsV4SignedAuthorizationHeader(
                                testConfig.getAccessKey(),
                                regionSupplier.get(),
                                "s3",
                                authorizationHeader
                            )
                        )
            );
        }

        @SuppressForbidden(reason = "implementing HTTP server for test fixture")
        @Override
        protected HttpHandler createHandler() {
            final var delegate = asInstanceOf(S3HttpHandler.class, super.createHandler());
            return exchange -> {
                final var request = delegate.parseRequest(exchange);
                final var testConfig = TestConfig.fromAuthorizationHeader(exchange.getRequestHeaders().getFirst("Authorization"));
                final var contentSha256Header = exchange.getRequestHeaders().getFirst(S3HttpHandler.CONTENT_SHA256_HEADER);
                if ((request.isUploadPartRequest() || request.isPutObjectRequest())
                    && Optional.ofNullable(exchange.getRequestHeaders().get(S3HttpHandler.COPY_SOURCE_HEADER))
                        .orElse(List.of())
                        .isEmpty()) {
                    assertThat(contentSha256Header, testConfig.getExpectedUploadContentSha256HeaderMatcher());
                    if (testConfig.unsafelyOmitRequestBodyChecksums) {
                        assertNoChecksumRequestHeaders(exchange.getRequestHeaders().keySet());
                    } else {
                        assertSomeChecksumRequestHeader(exchange.getRequestHeaders().keySet());
                    }
                    if (request.isUploadPartRequest()) {
                        CONFIGS_WITH_UPLOAD_PART_REQUESTS.add(testConfig.toString());
                    }
                }
                if (request.isMultiObjectDeleteRequest()) {
                    // The S3 API requires a checksum on DeleteObjects, so unsafely_omit_request_body_checksums must never remove it
                    assertThat(exchange.getRequestHeaders().getFirst("x-amz-checksum-crc32"), notNullValue());
                    assertThat(exchange.getRequestHeaders().getFirst("x-amz-sdk-checksum-algorithm"), equalTo("CRC32"));
                }
                if (testConfig.alwaysSignRequests) {
                    assertThat(
                        contentSha256Header,
                        allOf(
                            anyOf(
                                // We either have a SHA256 hash of the body, covered by the request signature, ...
                                matchesPattern(S3HttpHandler.SHA256_PATTERN),
                                // ... or there's a properly-signed footer
                                equalTo("STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"),
                                // ... or signed chunks with no footer, which is what we get when unsafely_omit_request_body_checksums is
                                // set
                                equalTo("STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
                            ),

                            // Redundant check given the above condition but this is the thing that the S3 docs tell users to forbid:
                            // https://docs.aws.amazon.com/AmazonS3/latest/developerguide/bucket-policy-s3-sigv4-conditions.html
                            not("UNSIGNED-PAYLOAD"),

                            // Other possibilities that can also imply an unsigned body, even if not excluded by the suggested condition:
                            not(nullValue()),
                            not("STREAMING-UNSIGNED-PAYLOAD-TRAILER")
                        )
                    );
                }
                final var contentMd5Header = exchange.getRequestHeaders().getFirst("Content-MD5");
                if (request.isMultiObjectDeleteRequest() && testConfig.legacyContentMd5) {
                    assertThat(contentMd5Header, notNullValue());
                } else {
                    assertThat(contentMd5Header, nullValue());
                }
                delegate.handle(exchange);
            };
        }
    }

    private static final S3HttpFixture httpS3Fixture = new ContentIntegrityS3HttpFixture(null);

    private static final S3HttpFixture httpsS3Fixture = new ContentIntegrityS3HttpFixture(testTlsCertificate);

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
        return bucketSupplier.get();
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

    /// Multipart uploads go through UploadPart rather than PutObject, so they have to obey the same checksum rules. The fixture handler
    /// asserts the headers on every request, and this test makes sure that at least one UploadPart was really sent for the config.
    public void testSnapshotOfLargeShardFilesUsesMultipartUploads() throws Exception {
        final var repository = new TestRepository(
            randomIdentifier(),
            getClientName(),
            getBucketName(),
            getBasePath(),
            extraRepositorySettings()
        );
        final var indexName = randomIdentifier();
        final var snapshotName = randomIdentifier();
        // forget any part seen earlier, so this test has to be the one that sends them
        CONFIGS_WITH_UPLOAD_PART_REQUESTS.remove(testConfig.toString());
        try (
            var ignored = repository.register(
                s -> Settings.builder().put(s).put("buffer_size", MULTIPART_BUFFER_SIZE.getStringRep()).build()
            )
        ) {
            createIndex(indexName, SINGLE_SHARD_INDEX_SETTINGS, UNINDEXED_PAYLOAD_MAPPING);
            indexIncompressibleData(client(), indexName, DATA_SIZE_FORCING_MULTIPART_UPLOADS);

            createSnapshotAndAssertSuccess(client(), repository.repositoryName(), snapshotName);
            assertThat(CONFIGS_WITH_UPLOAD_PART_REQUESTS, hasItem(testConfig.toString()));

            // deleting the snapshot sends a DeleteObjects request, which the fixture checks still carries its checksum
            deleteSnapshotAndAssertOk(client(), repository.repositoryName(), snapshotName);
        }
    }

    /// Test suite is parametric in this config.
    public static final class TestConfig {
        private final boolean https;
        private final boolean chunkedEncoding;
        private final boolean alwaysSignRequests;
        private final boolean legacyContentMd5;
        private final boolean unsafelyOmitRequestBodyChecksums;

        TestConfig(
            boolean https,
            boolean chunkedEncoding,
            boolean alwaysSignRequests,
            boolean legacyContentMd5,
            boolean unsafelyOmitRequestBodyChecksums
        ) {
            this.https = https;
            this.chunkedEncoding = chunkedEncoding;
            this.alwaysSignRequests = alwaysSignRequests;
            this.legacyContentMd5 = legacyContentMd5;
            this.unsafelyOmitRequestBodyChecksums = unsafelyOmitRequestBodyChecksums;
        }

        @Override
        public String toString() {
            return Strings.format(
                "{scheme=%s,chunkedEncoding=%s,alwaysSignRequests=%s,legacyContentMd5=%s,unsafelyOmitRequestBodyChecksums=%s}",
                https ? "https" : "http",
                chunkedEncoding,
                alwaysSignRequests,
                legacyContentMd5,
                unsafelyOmitRequestBodyChecksums
            );
        }

        Settings getRepositorySettings() {
            final var builder = Settings.builder();
            addBooleanSettingDefaultFalse(builder, "disable_chunked_encoding", chunkedEncoding == false);
            addBooleanSettingDefaultFalse(builder, "always_sign_requests", alwaysSignRequests);
            addBooleanSettingDefaultFalse(builder, "legacy_content_md5", legacyContentMd5);
            addBooleanSettingDefaultFalse(builder, "unsafely_omit_request_body_checksums", unsafelyOmitRequestBodyChecksums);
            return builder.build();
        }

        private void addBooleanSettingDefaultFalse(Settings.Builder builder, String key, boolean value) {
            if (value == false && randomBoolean()) {
                builder.putNull(key);
            } else {
                builder.put(key, value);
            }
        }

        String getExpectedUploadContentSha256Header() {
            return (https && alwaysSignRequests == false)
                ? (chunkedEncoding ? "STREAMING-UNSIGNED-PAYLOAD-TRAILER" : "UNSIGNED-PAYLOAD")
                : (chunkedEncoding ? "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER" : "STREAMING-AWS4-HMAC-SHA256-PAYLOAD");
        }

        /// What the {@code x-amz-content-sha256} header of an upload must look like for this config, which depends on whether the client
        /// is sending a checksum trailer with the body.
        Matcher<String> getExpectedUploadContentSha256HeaderMatcher() {
            return unsafelyOmitRequestBodyChecksums
                ? expectedUploadContentSha256HeaderWithoutChecksum()
                : expectedUploadContentSha256HeaderWithChecksum();
        }

        /// With a checksum the header is the streaming value for the scheme and signing mode, which includes the trailer, or a SHA-256
        /// of the body.
        private Matcher<String> expectedUploadContentSha256HeaderWithChecksum() {
            return anyOf(matchesPattern(S3HttpHandler.SHA256_PATTERN), equalTo(getExpectedUploadContentSha256Header()));
        }

        /// With no checksum there's no trailer, so the header is the unsigned marker over unsigned HTTPS, signed chunks with no footer
        /// when chunked, and a SHA-256 of the body when chunked encoding is disabled.
        private Matcher<String> expectedUploadContentSha256HeaderWithoutChecksum() {
            if (https && alwaysSignRequests == false) {
                return equalTo("UNSIGNED-PAYLOAD");
            }
            return chunkedEncoding ? equalTo("STREAMING-AWS4-HMAC-SHA256-PAYLOAD") : matchesPattern(S3HttpHandler.SHA256_PATTERN);
        }

        void applyClientSettings(LocalClusterSpecBuilder<?> builder) {
            final String client = getClientName();
            builder.keystore("s3.client." + client + ".access_key", getAccessKey());
            builder.keystore("s3.client." + client + ".secret_key", SECRET_KEY);
            builder.setting("s3.client." + client + ".endpoint", getS3Fixture()::getAddress);
            builder.setting("s3.client." + client + ".path_style_access", () -> "true", n -> https || randomBoolean());
        }

        S3HttpFixture getS3Fixture() {
            return https ? httpsS3Fixture : httpS3Fixture;
        }

        String getClientName() {
            return "content_integrity_client_"
                + (https ? "https" : "http")
                + "_chunked_encoding_"
                + chunkedEncoding
                + "_always_sign_requests_"
                + alwaysSignRequests
                + "_legacy_content_md5_"
                + legacyContentMd5
                + "_unsafely_omit_request_body_checksums_"
                + unsafelyOmitRequestBodyChecksums;
        }

        String getRepositoryBasePath() {
            return basePathSupplier.get()
                + "/"
                + (https ? "https" : "http")
                + "_chunked_encoding_"
                + chunkedEncoding
                + "_always_sign_requests_"
                + alwaysSignRequests
                + "_legacy_content_md5_"
                + legacyContentMd5
                + "_unsafely_omit_request_body_checksums_"
                + unsafelyOmitRequestBodyChecksums;
        }

        String getAccessKey() {
            return ACCESS_KEY
                + "-"
                + (https ? "https" : "http")
                + "-"
                + chunkedEncoding
                + "-"
                + alwaysSignRequests
                + "-"
                + legacyContentMd5
                + "-"
                + unsafelyOmitRequestBodyChecksums;
        }

        static TestConfig fromAuthorizationHeader(String authorizationHeader) {
            if (authorizationHeader != null) {
                for (final var testConfig : TEST_CONFIGS) {
                    if (authorizationHeader.contains(testConfig.getAccessKey())) {
                        return testConfig;
                    }
                }
            }
            throw new AssertionError("no case matches authorization header [" + authorizationHeader + "]");
        }
    }
}

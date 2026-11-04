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

import org.elasticsearch.client.Request;
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
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static fixture.aws.DynamicIdentifierSupplier.testClassIdentifierSupplier;
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
                    for (final boolean requestChecksumWhenRequired : new boolean[] { false, true }) {
                        // legacy_content_md5 only changes DeleteObjects, so it doesn't need crossing with everything else. One config
                        // per scheme and signing mode is enough to show it still works next to the other settings.
                        final boolean legacyContentMd5 = chunkedEncoding && requestChecksumWhenRequired == false;
                        testConfigs.add(
                            new TestConfig(https, chunkedEncoding, alwaysSignRequests, legacyContentMd5, requestChecksumWhenRequired)
                        );
                    }
                }
            }
        }
        TEST_CONFIGS = List.copyOf(testConfigs);
    }

    /// Test configs for which the fixture saw an UploadPart request, so tests can prove they really exercised multipart uploads.
    private static final Set<String> CONFIGS_WITH_UPLOAD_PART_REQUESTS = ConcurrentHashMap.newKeySet();

    private static void assertNoChecksumRequestHeaders(Set<String> requestHeaderNames) {
        for (final var headerName : requestHeaderNames) {
            final var lowerCaseName = headerName.toLowerCase(Locale.ROOT);
            assertFalse("unexpected header [" + headerName + "]", lowerCaseName.startsWith("x-amz-checksum-"));
            assertNotEquals("unexpected header [" + headerName + "]", "x-amz-trailer", lowerCaseName);
            assertNotEquals("unexpected header [" + headerName + "]", "x-amz-sdk-checksum-algorithm", lowerCaseName);
        }
    }

    private static void assertChecksumRequestHeadersPresent(Set<String> requestHeaderNames) {
        final var hasChecksumHeader = requestHeaderNames.stream()
            .map(headerName -> headerName.toLowerCase(Locale.ROOT))
            .anyMatch(headerName -> headerName.startsWith("x-amz-checksum-") || headerName.equals("x-amz-trailer"));
        assertTrue("expected a checksum header or trailer declaration in " + requestHeaderNames, hasChecksumHeader);
    }

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
                    if (testConfig.requestChecksumWhenRequired) {
                        assertNoChecksumRequestHeaders(exchange.getRequestHeaders().keySet());
                    } else {
                        assertChecksumRequestHeadersPresent(exchange.getRequestHeaders().keySet());
                    }
                    if (request.isUploadPartRequest()) {
                        CONFIGS_WITH_UPLOAD_PART_REQUESTS.add(testConfig.toString());
                    }
                }
                if (request.isMultiObjectDeleteRequest()) {
                    // DeleteObjects requires a checksum so it must carry one whatever the setting says
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
                                // ... or signed chunks with no footer, which is what we get when request_checksum_when_required is set
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
        try (var ignored = repository.register(s -> Settings.builder().put(s).put("buffer_size", "5mb").build())) {
            createIndex(indexName, Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build(), """
                {"properties":{"payload":{"type":"object","enabled":false}}}""");
            // random text doesn't compress, so the stored fields file ends up larger than the 5mb buffer and has to be sent in parts
            final int batches = 4;
            final int docsPerBatch = 300;
            for (int batch = 0; batch < batches; batch++) {
                final var bulk = new StringBuilder();
                for (int doc = 0; doc < docsPerBatch; doc++) {
                    bulk.append("{\"index\":{}}\n{\"payload\":{\"text\":\"").append(randomAlphaOfLength(8000)).append("\"}}\n");
                }
                final var bulkRequest = new Request("POST", "/" + indexName + "/_bulk");
                bulkRequest.setJsonEntity(bulk.toString());
                assertFalse(assertOKAndCreateObjectPath(client().performRequest(bulkRequest)).evaluate("errors"));
            }

            final var createSnapshotRequest = new Request("PUT", "/_snapshot/" + repository.repositoryName() + "/" + snapshotName);
            createSnapshotRequest.addParameter("wait_for_completion", "true");
            final var response = assertOKAndCreateObjectPath(client().performRequest(createSnapshotRequest));
            assertEquals("SUCCESS", response.evaluate("snapshot.state"));
            assertThat(response.evaluate("snapshot.shards.failed"), equalTo(0));
            assertThat(CONFIGS_WITH_UPLOAD_PART_REQUESTS, hasItem(testConfig.toString()));

            // deleting the snapshot sends a DeleteObjects request, which must carry its checksum whatever the setting says
            assertOK(client().performRequest(new Request("DELETE", "/_snapshot/" + repository.repositoryName() + "/" + snapshotName)));
        }
    }

    /// Test suite is parametric in this config.
    public static final class TestConfig {
        private final boolean https;
        private final boolean chunkedEncoding;
        private final boolean alwaysSignRequests;
        private final boolean legacyContentMd5;
        private final boolean requestChecksumWhenRequired;

        TestConfig(
            boolean https,
            boolean chunkedEncoding,
            boolean alwaysSignRequests,
            boolean legacyContentMd5,
            boolean requestChecksumWhenRequired
        ) {
            this.https = https;
            this.chunkedEncoding = chunkedEncoding;
            this.alwaysSignRequests = alwaysSignRequests;
            this.legacyContentMd5 = legacyContentMd5;
            this.requestChecksumWhenRequired = requestChecksumWhenRequired;
        }

        @Override
        public String toString() {
            return Strings.format(
                "{scheme=%s,chunkedEncoding=%s,alwaysSignRequests=%s,legacyContentMd5=%s,requestChecksumWhenRequired=%s}",
                https ? "https" : "http",
                chunkedEncoding,
                alwaysSignRequests,
                legacyContentMd5,
                requestChecksumWhenRequired
            );
        }

        Settings getRepositorySettings() {
            final var builder = Settings.builder();
            addBooleanSettingDefaultFalse(builder, "disable_chunked_encoding", chunkedEncoding == false);
            addBooleanSettingDefaultFalse(builder, "always_sign_requests", alwaysSignRequests);
            addBooleanSettingDefaultFalse(builder, "legacy_content_md5", legacyContentMd5);
            addBooleanSettingDefaultFalse(builder, "request_checksum_when_required", requestChecksumWhenRequired);
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

        /// Without the setting the client sends a checksum with every upload, so the body hash header is either a SHA-256 of the body or
        /// the streaming value for the scheme and signing mode. With it the client sends no checksum, so there is no footer. That
        /// leaves the unsigned payload marker over unsigned HTTPS, signed chunks without a footer when chunked, and a SHA-256 of the
        /// body when chunked encoding is disabled.
        Matcher<String> getExpectedUploadContentSha256HeaderMatcher() {
            if (requestChecksumWhenRequired == false) {
                return anyOf(matchesPattern(S3HttpHandler.SHA256_PATTERN), equalTo(getExpectedUploadContentSha256Header()));
            }
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
                + "_request_checksum_when_required_"
                + requestChecksumWhenRequired;
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
                + "_request_checksum_when_required_"
                + requestChecksumWhenRequired;
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
                + requestChecksumWhenRequired;
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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import fixture.s3.S3ConsistencyModel;
import fixture.s3.S3HttpFixture;
import fixture.s3.S3HttpHandler;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.fixtures.testcontainers.TestContainersThreadFilter;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static fixture.aws.AwsCredentialsUtils.fixedAccessKey;
import static fixture.aws.DynamicIdentifierSupplier.testClassIdentifierSupplier;
import static org.elasticsearch.repositories.s3.S3ChecksumRestTestSupport.DATA_SIZE_FORCING_MULTIPART_UPLOADS;
import static org.elasticsearch.repositories.s3.S3ChecksumRestTestSupport.MULTIPART_BUFFER_SIZE;
import static org.elasticsearch.repositories.s3.S3ChecksumRestTestSupport.SINGLE_SHARD_INDEX_SETTINGS;
import static org.elasticsearch.repositories.s3.S3ChecksumRestTestSupport.UNINDEXED_PAYLOAD_MAPPING;
import static org.elasticsearch.repositories.s3.S3ChecksumRestTestSupport.assertNoChecksumRequestHeaders;
import static org.elasticsearch.repositories.s3.S3ChecksumRestTestSupport.createSnapshotAndAssertSuccess;
import static org.elasticsearch.repositories.s3.S3ChecksumRestTestSupport.deleteSnapshotAndAssertOk;
import static org.elasticsearch.repositories.s3.S3ChecksumRestTestSupport.indexIncompressibleData;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * An operator who already tells the AWS SDK to only checksum when required, through the {@code aws.requestChecksumCalculation} system
 * property, must keep that behaviour while {@code unsafely_omit_request_body_checksums} is unset or {@code false}. The setting only ever
 * switches the SDK to {@code WHEN_REQUIRED}, so leaving it off must never push the client back to {@code WHEN_SUPPORTED}.
 * <p>
 * This needs its own cluster because the system property applies to the whole node, which the content integrity suite can't vary per
 * config.
 */
@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
public class RepositoryS3RequestChecksumCalculationRestIT extends ESRestTestCase {

    private static final String PREFIX = AbstractRepositoryS3RestTestCase.getIdentifierPrefix(
        "RepositoryS3RequestChecksumCalculationRestIT"
    );
    private static final String ACCESS_KEY = PREFIX + "access-key";
    private static final String SECRET_KEY = PREFIX + "secret-key";
    private static final String CLIENT = "node_wide_when_required_client";

    private static final Supplier<String> regionSupplier = () -> "us-east-1";
    private static final Supplier<String> bucketSupplier = testClassIdentifierSupplier("bucket");
    private static final Supplier<String> basePathSupplier = testClassIdentifierSupplier("base_path");

    private static final AtomicBoolean sawUploadPart = new AtomicBoolean();
    private static final AtomicBoolean sawDeleteObjects = new AtomicBoolean();

    private static final S3HttpFixture s3Fixture = new S3HttpFixture(
        true,
        null,
        bucketSupplier,
        basePathSupplier,
        S3ConsistencyModel::randomConsistencyModel,
        fixedAccessKey(ACCESS_KEY, regionSupplier, "s3")
    ) {
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
                    assertNoChecksumRequestHeaders(exchange.getRequestHeaders().keySet());
                    final var contentSha256Header = exchange.getRequestHeaders().getFirst(S3HttpHandler.CONTENT_SHA256_HEADER);
                    assertNotTrailerValue(contentSha256Header);
                    if (request.isUploadPartRequest()) {
                        sawUploadPart.set(true);
                    }
                }
                if (request.isMultiObjectDeleteRequest()) {
                    // The S3 API requires a checksum on DeleteObjects, so the node wide property can't remove it either
                    assertThat(exchange.getRequestHeaders().getFirst("x-amz-checksum-crc32"), notNullValue());
                    assertThat(exchange.getRequestHeaders().getFirst("x-amz-sdk-checksum-algorithm"), equalTo("CRC32"));
                    sawDeleteObjects.set(true);
                }
                delegate.handle(exchange);
            };
        }
    };

    private static void assertNotTrailerValue(String contentSha256Header) {
        assertThat(contentSha256Header, notNullValue());
        assertFalse("unexpected body hash [" + contentSha256Header + "]", contentSha256Header.endsWith("-TRAILER"));
    }

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-s3")
        .systemProperty("aws.region", regionSupplier)
        .systemProperty("aws.requestChecksumCalculation", "when_required")
        .keystore("s3.client." + CLIENT + ".access_key", ACCESS_KEY)
        .keystore("s3.client." + CLIENT + ".secret_key", SECRET_KEY)
        .setting("s3.client." + CLIENT + ".endpoint", s3Fixture::getAddress)
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Before
    public void resetObservedRequests() {
        sawUploadPart.set(false);
        sawDeleteObjects.set(false);
    }

    /**
     * The client setting is absent, so the node-wide property is the only thing deciding the checksum behaviour.
     */
    public void testUnsetSettingLeavesTheNodeWideWhenRequiredPropertyInEffect() throws Exception {
        // the repository registration helper randomly adds the key, so clear it to leave the setting unset
        assertUploadsCarryNoChecksum(s -> Settings.builder().put(s).putNull("unsafely_omit_request_body_checksums").build());
    }

    /**
     * An explicit {@code false} means the same as unset, so it must not override the operator's property either.
     */
    public void testExplicitFalseSettingLeavesTheNodeWideWhenRequiredPropertyInEffect() throws Exception {
        assertUploadsCarryNoChecksum(s -> Settings.builder().put(s).put("unsafely_omit_request_body_checksums", false).build());
    }

    private void assertUploadsCarryNoChecksum(UnaryOperator<Settings> repositorySettings) throws Exception {
        final var repository = new AbstractRepositoryS3RestTestCase.TestRepository(
            randomIdentifier(),
            CLIENT,
            bucketSupplier.get(),
            basePathSupplier.get(),
            Settings.EMPTY
        );
        final var indexName = randomIdentifier();
        final var snapshotName = randomIdentifier();
        try (
            var ignored = repository.register(
                s -> repositorySettings.apply(Settings.builder().put(s).put("buffer_size", MULTIPART_BUFFER_SIZE.getStringRep()).build())
            )
        ) {
            createIndex(indexName, SINGLE_SHARD_INDEX_SETTINGS, UNINDEXED_PAYLOAD_MAPPING);
            indexIncompressibleData(client(), indexName, DATA_SIZE_FORCING_MULTIPART_UPLOADS);

            createSnapshotAndAssertSuccess(client(), repository.repositoryName(), snapshotName);
            assertTrue("expected the snapshot to send at least one UploadPart", sawUploadPart.get());

            deleteSnapshotAndAssertOk(client(), repository.repositoryName(), snapshotName);
            assertTrue("expected the snapshot delete to send DeleteObjects", sawDeleteObjects.get());
        }
    }
}

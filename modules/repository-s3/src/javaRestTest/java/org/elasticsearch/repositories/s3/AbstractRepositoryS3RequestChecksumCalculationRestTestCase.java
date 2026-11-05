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

import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Before;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static fixture.aws.AwsCredentialsUtils.fixedAccessKey;
import static fixture.aws.DynamicIdentifierSupplier.testClassIdentifierSupplier;
import static org.elasticsearch.repositories.s3.S3ChecksumRestTestSupport.checksumRequestHeaderNames;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;

/**
 * When {@code unsafely_omit_optional_request_body_checksums} is unset or {@code false}, we don't tell the SDK anything, so an operator
 * who already set {@code WHEN_REQUIRED} (via env var) keeps it. Telling the SDK {@code WHEN_SUPPORTED} instead would override their setting
 * because a value set on the client builder beats the system property and the env var.
 * <p>
 * The operator can set it with the {@code aws.requestChecksumCalculation} system property or the {@code AWS_REQUEST_CHECKSUM_CALCULATION}
 * env var. Both apply to the whole node, which the content integrity suite can't vary per config, so each subclass starts its own cluster
 * with one of them set and runs the same test.
 * <p>
 * The test checks a single-part {@code PutObject}, which is what registering a repository sends. The setting applies to the whole client,
 * so one operation the SDK would otherwise checksum is enough to show that we left the operator's choice alone.
 */
public abstract class AbstractRepositoryS3RequestChecksumCalculationRestTestCase extends ESRestTestCase {

    private static final String PREFIX = AbstractRepositoryS3RestTestCase.getIdentifierPrefix(
        "AbstractRepositoryS3RequestChecksumCalculationRestTestCase"
    );
    private static final String ACCESS_KEY = PREFIX + "access-key";
    private static final String SECRET_KEY = PREFIX + "secret-key";
    private static final String CLIENT = "node_wide_when_required_client";

    private static final Supplier<String> regionSupplier = () -> "us-east-1";
    private static final Supplier<String> bucketSupplier = testClassIdentifierSupplier("bucket");
    private static final Supplier<String> basePathSupplier = testClassIdentifierSupplier("base_path");

    /** How many uploads the fixture has seen, so a test can tell that it checked something. */
    private static final AtomicInteger putObjectCount = new AtomicInteger();

    /** A description of each upload the fixture saw that carried a checksum. */
    private static final List<String> checksummedUploads = new CopyOnWriteArrayList<>();

    /** A fixture that records the uploads it sees, and which of them carried a checksum or a checksum trailer. */
    static S3HttpFixture newS3Fixture() {
        return new S3HttpFixture(
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
                    // Server-side copy is also a PUT, but it names its source in a header and sends no body, so there's no upload to check
                    final var isCopy = exchange.getRequestHeaders()
                        .getOrDefault(S3HttpHandler.COPY_SOURCE_HEADER, List.of())
                        .isEmpty() == false;
                    if (request.isPutObjectRequest() && isCopy == false) {
                        putObjectCount.incrementAndGet();
                        final var checksumHeaders = checksumRequestHeaderNames(exchange.getRequestHeaders().keySet());
                        final var contentSha256Header = exchange.getRequestHeaders().getFirst(S3HttpHandler.CONTENT_SHA256_HEADER);
                        final var hasChecksumTrailer = contentSha256Header != null && contentSha256Header.endsWith("-TRAILER");
                        if (checksumHeaders.isEmpty() == false || hasChecksumTrailer) {
                            checksummedUploads.add(
                                "PutObject with checksum headers " + checksumHeaders + " and body hash " + contentSha256Header
                            );
                        }
                    }
                    delegate.handle(exchange);
                };
            }
        };
    }

    /**
     * A cluster with one S3 client pointing at the fixture, which a subclass finishes by saying how the operator asked the SDK to only
     * checksum when required.
     */
    static LocalClusterSpecBuilder<ElasticsearchCluster> localClusterWithS3Client(S3HttpFixture s3Fixture) {
        return ElasticsearchCluster.local()
            .module("repository-s3")
            .systemProperty("aws.region", regionSupplier)
            .keystore("s3.client." + CLIENT + ".access_key", ACCESS_KEY)
            .keystore("s3.client." + CLIENT + ".secret_key", SECRET_KEY)
            .setting("s3.client." + CLIENT + ".endpoint", s3Fixture::getAddress);
    }

    @Before
    public void forgetUploadsSeenEarlier() {
        putObjectCount.set(0);
        checksummedUploads.clear();
    }

    /**
     * The client setting is absent, so the operator's setting is the only thing deciding the checksum behaviour. An explicit
     * {@code false} is the same thing, because it's the default, so it doesn't need a test of its own.
     */
    public void testPutObjectStaysChecksumFreeWhenTheSettingIsUnsetAndTheOperatorAsksForWhenRequired() throws Exception {
        final var repository = new AbstractRepositoryS3RestTestCase.TestRepository(
            randomIdentifier(),
            CLIENT,
            bucketSupplier.get(),
            basePathSupplier.get(),
            Settings.EMPTY
        );
        // The registration helper randomly adds the key, so clear it to leave the setting unset. Registering the repository writes a
        // small blob to check that it works, which is a single PutObject.
        try (
            var ignored = repository.register(
                s -> Settings.builder().put(s).putNull("unsafely_omit_optional_request_body_checksums").build()
            )
        ) {
            assertThat("expected registering the repository to send a PutObject", putObjectCount.get(), greaterThan(0));
            assertThat("uploads must not carry a checksum", checksummedUploads, empty());
        }
    }
}

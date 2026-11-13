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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static fixture.aws.AwsCredentialsUtils.fixedAccessKey;
import static fixture.aws.DynamicIdentifierSupplier.testClassIdentifierSupplier;
import static org.hamcrest.Matchers.greaterThan;

/**
 * For S3 repositories only {@code unsafely_omit_request_body_checksums} decides whether uploads carry a checksum. When it's
 * unset we tell the SDK {@code WHEN_SUPPORTED}, so an operator who set {@code AWS_REQUEST_CHECKSUM_CALCULATION=when_required} on the node
 * doesn't get what they asked for, because a value set on the client builder beats the env var. The value of the env var doesn't matter to
 * the code, so the test picks one at random. Only {@code when_required} would show a regression, because {@code when_supported} is the
 * SDK default and sends checksums either way.
 * <p>
 * The test checks a single-part {@code PutObject}, which is what registering a repository sends. The setting applies to the whole client,
 * so one operation the SDK would otherwise checksum is enough. The system property and the profile file go through the same SDK resolver
 * as the env var, so they aren't tested separately. This needs its own cluster because the env var applies to the whole node.
 */
@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
public class RepositoryS3RequestChecksumEnvVarIgnoredRestIT extends ESRestTestCase {

    private static final String PREFIX = AbstractRepositoryS3RestTestCase.getIdentifierPrefix(
        "RepositoryS3RequestChecksumEnvVarIgnoredRestIT"
    );
    private static final String ACCESS_KEY = PREFIX + "access-key";
    private static final String SECRET_KEY = PREFIX + "secret-key";
    private static final String CLIENT = "env_var_ignored_client";

    private static final Supplier<String> regionSupplier = new DynamicRegionSupplier();
    private static final Supplier<String> bucketSupplier = testClassIdentifierSupplier("bucket");
    private static final Supplier<String> basePathSupplier = testClassIdentifierSupplier("base_path");

    /** How many uploads the fixture has seen, so the test can tell that it checked something. */
    private static final AtomicInteger putObjectCount = new AtomicInteger();

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
                // A server-side copy is also a PUT, but it names its source in a header and sends no body, so there's no upload to check
                final var isCopy = exchange.getRequestHeaders()
                    .getOrDefault(S3HttpHandler.COPY_SOURCE_HEADER, List.of())
                    .isEmpty() == false;
                if (request.isPutObjectRequest() && isCopy == false) {
                    putObjectCount.incrementAndGet();
                    final var headerNames = exchange.getRequestHeaders().keySet();
                    if (headerNames.stream().noneMatch(AbstractRepositoryS3RestTestCase::isChecksumRequestHeader)) {
                        throw new AssertionError("expected a checksum on the PutObject, but its headers were " + headerNames);
                    }
                }
                delegate.handle(exchange);
            };
        }
    };

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-s3")
        .systemProperty("aws.region", regionSupplier)
        .environment("AWS_REQUEST_CHECKSUM_CALCULATION", () -> randomFrom("when_required", "when_supported"))
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
    public void forgetUploadsSeenEarlier() {
        putObjectCount.set(0);
    }

    public void testEnvVarIsIgnoredWhenTheSettingIsUnset() throws Exception {
        final var repository = new AbstractRepositoryS3RestTestCase.TestRepository(
            randomIdentifier(),
            CLIENT,
            bucketSupplier.get(),
            basePathSupplier.get(),
            Settings.EMPTY
        );
        // The registration helper randomly adds the key, so clear it to leave the setting unset. Registering the repository writes a
        // small blob to check that it works, which is a single PutObject.
        try (var ignored = repository.register(s -> Settings.builder().put(s).putNull("unsafely_omit_request_body_checksums").build())) {
            assertThat("expected registering the repository to send a PutObject", putObjectCount.get(), greaterThan(0));
        }
    }
}

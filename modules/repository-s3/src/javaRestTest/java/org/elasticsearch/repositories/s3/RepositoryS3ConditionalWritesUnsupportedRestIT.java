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
import fixture.s3.S3HttpFixture;
import fixture.s3.S3HttpHandler;

import com.carrotsearch.randomizedtesting.annotations.SuppressForbidden;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.LogType;
import org.elasticsearch.test.fixtures.testcontainers.TestContainersThreadFilter;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static fixture.aws.AwsCredentialsUtils.fixedAccessKey;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;

@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
@ThreadLeakScope(ThreadLeakScope.Scope.NONE) // https://github.com/elastic/elasticsearch/issues/102482
@SuppressForbidden("HttpExchange and Headers are ok here")
public class RepositoryS3ConditionalWritesUnsupportedRestIT extends AbstractRepositoryS3RestTestCase {

    private static final String PREFIX = getIdentifierPrefix("RepositoryS3BasicCredentialsRestIT");
    private static final String BUCKET = PREFIX + "bucket";
    private static final String BASE_PATH = PREFIX + "base_path";
    private static final String ACCESS_KEY = PREFIX + "access-key";
    private static final String SECRET_KEY = PREFIX + "secret-key";
    private static final String CLIENT = "no_conditional_writes_client";

    private static final Supplier<String> regionSupplier = new DynamicRegionSupplier();

    private static final S3HttpFixture s3Fixture = new S3HttpFixture(
        true,
        BUCKET,
        BASE_PATH,
        fixedAccessKey(ACCESS_KEY, regionSupplier, "s3")
    ) {
        @Override
        @SuppressForbidden("HttpExchange and Headers are ok here")
        protected HttpHandler createHandler() {
            return new AssertNoConditionalWritesHandler(asInstanceOf(S3HttpHandler.class, super.createHandler()));
        }
    };

    @SuppressForbidden("HttpExchange and Headers are ok here")
    private static class AssertNoConditionalWritesHandler implements HttpHandler {

        private final S3HttpHandler delegateHandler;

        private AssertNoConditionalWritesHandler(S3HttpHandler delegateHandler) {
            this.delegateHandler = delegateHandler;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (exchange.getRequestHeaders().containsKey("if-match") || exchange.getRequestHeaders().containsKey("if-none-match")) {
                final var exception = new AssertionError(
                    Strings.format(
                        "unsupported conditional write: [%s] with headers [%s]",
                        delegateHandler.parseRequest(exchange),
                        exchange.getRequestHeaders()
                    )
                );
                ExceptionsHelper.maybeDieOnAnotherThread(exception);
                throw exception;
            }
            delegateHandler.handle(exchange);
        }
    }

    @Override
    protected Settings extraRepositorySettings() {
        return Settings.builder()
            .put(super.extraRepositorySettings())
            .put(S3Repository.UNSAFELY_INCOMPATIBLE_WITH_S3_CONDITIONAL_WRITES.getKey(), true)
            .build();
    }

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-s3")
        .systemProperty("aws.region", regionSupplier)
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

    public void testWarningLog() throws IOException {
        final var repoName = randomIdentifier();
        final var testRepository = new TestRepository(repoName, getClientName(), getBucketName(), getBasePath(), extraRepositorySettings());
        try (var ignored = testRepository.register(UnaryOperator.identity()); var logStream = cluster.getNodeLog(0, LogType.SERVER)) {
            assertThat(
                Streams.readAllLines(logStream),
                hasItem(
                    allOf(
                        containsString("WARN"),
                        containsString(repoName),
                        containsString("""
                            is configured to unsafely avoid conditional writes which may lead to repository corruption; to resolve this \
                            warning, upgrade your storage to a system that is fully compatible with AWS S3 and then remove the \
                            [unsafely_incompatible_with_s3_conditional_writes] repository setting"""),
                        containsString(ReferenceDocs.S3_COMPATIBLE_REPOSITORIES.toString())
                    )
                )
            );
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package fixture.s3;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;

import static fixture.aws.AwsCredentialsUtils.ANY_REGION;
import static fixture.aws.AwsCredentialsUtils.checkAuthorization;
import static fixture.aws.AwsCredentialsUtils.fixedAccessKey;
import static fixture.aws.AwsFixtureUtils.getLocalFixtureAddress;

public class S3HttpFixture extends ExternalResource {

    private static final Logger logger = LogManager.getLogger(S3HttpFixture.class);

    private HttpServer server;
    private ExecutorService executorService;

    private final boolean enabled;
    private final String bucket;
    private final String basePath;
    private final BiPredicate<String, String> authorizationPredicate;

    public S3HttpFixture(boolean enabled) {
        this(enabled, "bucket", "base_path_integration_tests", fixedAccessKey("s3_test_access_key", ANY_REGION, "s3"));
    }

    public S3HttpFixture(boolean enabled, String bucket, String basePath, BiPredicate<String, String> authorizationPredicate) {
        this.enabled = enabled;
        this.bucket = bucket;
        this.basePath = basePath;
        this.authorizationPredicate = authorizationPredicate;
    }

    protected HttpHandler createHandler() {
        return new S3HttpHandler(bucket, basePath) {
            @Override
            public void handle(final HttpExchange exchange) throws IOException {
                try {
                    if (checkAuthorization(authorizationPredicate, exchange)) {
                        super.handle(exchange);
                    }
                } catch (Error e) {
                    // HttpServer catches Throwable, so we must throw errors on another thread
                    ExceptionsHelper.maybeDieOnAnotherThread(e);
                    throw e;
                }
            }
        };
    }

    public String getAddress() {
        return "http://" + server.getAddress().getHostString() + ":" + server.getAddress().getPort();
    }

    public void stop(int delay) {
        server.stop(delay);
    }

    protected void before() throws Throwable {
        if (enabled) {
            this.executorService = EsExecutors.newScaling(
                "s3-http-fixture",
                1,
                100,
                30,
                TimeUnit.SECONDS,
                true,
                EsExecutors.daemonThreadFactory("s3-http-fixture"),
                new ThreadContext(Settings.EMPTY)
            );

            this.server = HttpServer.create(getLocalFixtureAddress(), 0);
            this.server.createContext("/", Objects.requireNonNull(createHandler()));
            this.server.setExecutor(executorService);
            server.start();
            logger.info("running S3HttpFixture at " + getAddress());
        }
    }

    @Override
    protected void after() {
        if (enabled) {
            stop(0);
            ThreadPool.terminate(executorService, 10, TimeUnit.SECONDS);
        }
    }
}

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
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

import org.apache.lucene.util.Constants;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.KeyStoreUtil;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.TestEsExecutors;
import org.elasticsearch.test.fixtures.tls.TestTlsCertificate;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.function.Supplier;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;

import static fixture.aws.AwsCredentialsUtils.ANY_REGION;
import static fixture.aws.AwsCredentialsUtils.checkAuthorization;
import static fixture.aws.AwsCredentialsUtils.fixedAccessKey;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.startsWith;

public class S3HttpFixture extends ExternalResource {

    private static final Logger logger = LogManager.getLogger(S3HttpFixture.class);

    private HttpServer server;
    private ExecutorService executorService;

    private final boolean enabled;
    @Nullable // if using HTTP
    private final TestTlsCertificate tlsCertificate;
    private final String bucket;
    private final String basePath;
    private final BiPredicate<String, String> authorizationPredicate;
    private final Supplier<S3ConsistencyModel> consistencyModel;

    public S3HttpFixture(boolean enabled, Supplier<S3ConsistencyModel> consistencyModel) {
        this(
            enabled,
            null,
            "bucket",
            "base_path_integration_tests",
            consistencyModel,
            fixedAccessKey("s3_test_access_key", ANY_REGION, "s3")
        );
    }

    public S3HttpFixture(
        boolean enabled,
        @Nullable /* to use HTTP */ TestTlsCertificate tlsCertificate,
        String bucket,
        String basePath,
        Supplier<S3ConsistencyModel> consistencyModel,
        BiPredicate<String, String> authorizationPredicate
    ) {
        this.tlsCertificate = tlsCertificate;
        this.enabled = enabled;
        this.bucket = bucket;
        this.basePath = basePath;
        this.authorizationPredicate = authorizationPredicate;
        this.consistencyModel = consistencyModel;
    }

    protected HttpHandler createHandler() {
        return new S3HttpHandler(bucket, basePath, consistencyModel.get()) {
            @Override
            public void handle(final HttpExchange exchange) throws IOException {
                try {
                    assertThat(exchange.getRequestHeaders().get("user-agent"), contains(startsWith("elasticsearch/")));
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
        final var socketAddress = server.getAddress();
        final var hostAddress = socketAddress.getAddress();
        return Strings.format(
            "%s://%s:%d",
            tlsCertificate == null ? "http" : "https",
            tlsCertificate == null ? InetAddresses.toUriString(hostAddress)
                : Constants.WINDOWS && hostAddress.isLoopbackAddress() ? "localhost" /* otherwise yields "127.0.0.1" -> cert mismatch */
                : hostAddress.getHostName(),
            socketAddress.getPort()
        );
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
                TestEsExecutors.testOnlyDaemonThreadFactory("s3-http-fixture"),
                new ThreadContext(Settings.EMPTY)
            );

            if (tlsCertificate == null) {
                this.server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
            } else {
                final SSLContext sslContext = SSLContext.getInstance("TLS");
                sslContext.init(
                    new KeyManager[] {
                        KeyStoreUtil.createKeyManager(
                            new Certificate[] { tlsCertificate.certificate() },
                            tlsCertificate.privateKey(),
                            null
                        ) },
                    null,
                    new SecureRandom()
                );
                final var httpsServer = HttpsServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
                this.server = httpsServer;
                httpsServer.setHttpsConfigurator(new HttpsConfigurator(sslContext));
            }
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

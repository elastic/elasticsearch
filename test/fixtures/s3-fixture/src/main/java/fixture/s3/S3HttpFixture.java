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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.ssl.KeyStoreUtil;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.fixtures.tls.TestTlsCertificate;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.util.Objects;
import java.util.function.BiPredicate;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;

import static fixture.aws.AwsCredentialsUtils.ANY_REGION;
import static fixture.aws.AwsCredentialsUtils.checkAuthorization;
import static fixture.aws.AwsCredentialsUtils.fixedAccessKey;

public class S3HttpFixture extends ExternalResource {

    private HttpServer server;

    private final boolean enabled;
    @Nullable // if using HTTP
    private final TestTlsCertificate tlsCertificate;
    private final String bucket;
    private final String basePath;
    private final BiPredicate<String, String> authorizationPredicate;

    public S3HttpFixture(boolean enabled) {
        this(enabled, null, "bucket", "base_path_integration_tests", fixedAccessKey("s3_test_access_key", ANY_REGION, "s3"));
    }

    public S3HttpFixture(
        boolean enabled,
        @Nullable /* to use HTTP */ TestTlsCertificate tlsCertificate,
        String bucket,
        String basePath,
        BiPredicate<String, String> authorizationPredicate
    ) {
        this.enabled = enabled;
        this.tlsCertificate = tlsCertificate;
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
        return Strings.format(
            "%s://%s:%d",
            tlsCertificate == null ? "http" : "https",
            tlsCertificate == null
                ? InetAddresses.toUriString(server.getAddress().getAddress())
                : server.getAddress().getAddress().getHostName(),
            server.getAddress().getPort()
        );
    }

    public void stop(int delay) {
        server.stop(delay);
    }

    protected void before() throws Throwable {
        if (enabled) {
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
            server.start();
        }
    }

    @Override
    protected void after() {
        if (enabled) {
            stop(0);
        }
    }
}

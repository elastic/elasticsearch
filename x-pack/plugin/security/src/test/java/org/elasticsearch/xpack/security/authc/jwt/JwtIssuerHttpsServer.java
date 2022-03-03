/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

public class JwtIssuerHttpsServer implements Closeable {
    private static final Logger LOGGER = LogManager.getLogger(JwtIssuerHttpsServer.class);

    final HttpsServer httpsServer;
    final String address;
    final int port;
    final String url;
    final int stopDelaySeconds;

    public JwtIssuerHttpsServer(
        final String address, // EX: localhost, 127.0.0.1, ::1, hostname, FQDN, etc
        final int port, // EX: 443, 0 (ephemeral port)
        final int backlog, // EX: 0 no limit, >0 limited
        final int stopDelaySeconds, // EX: 0 hard stop, >0 graceful stop
        final List<Path> trustedCertPaths, // EX: ca_certs/ca1.pem
        final HttpHandler httpHandler
    ) throws Exception {
        final SSLContext sslContext = this.createSslContext(trustedCertPaths);
        this.httpsServer = MockHttpServer.createHttps(new InetSocketAddress(address, port), backlog);
        this.httpsServer.setHttpsConfigurator(new HttpsConfigurator(sslContext));
        this.httpsServer.createContext("/", httpHandler);
        this.address = address;
        this.port = this.httpsServer.getAddress().getPort();
        this.url = "https://" + address + ":" + this.port + "/";
        this.stopDelaySeconds = stopDelaySeconds;
        LOGGER.trace("Starting HTTPS server " + this.url);
        this.httpsServer.start();
        LOGGER.debug("Started HTTPS server " + this.url);
    }

    @Override
    public void close() throws IOException {
        if (this.httpsServer != null) {
            LOGGER.trace("Stopping HTTPS server");
            this.httpsServer.stop(this.stopDelaySeconds);
            LOGGER.debug("Stopped HTTPS server");
        }
    }

    private SSLContext createSslContext(final List<Path> trustedCertPaths) throws Exception {
        final TrustManager[] trustManagers = new TrustManager[] { CertParsingUtils.getTrustManagerFromPEM(trustedCertPaths) };
        final SSLContext sslContext = SSLContext.getInstance("TLSv1.2"); // ex: TLSv1.2, TLSv1.3
        sslContext.init(null, trustManagers, JwtTestCase.SECURE_RANDOM);
        return sslContext;
    }
}

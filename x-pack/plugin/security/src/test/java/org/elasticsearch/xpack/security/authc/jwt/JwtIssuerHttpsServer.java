/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;

/**
 * HTTPS server for JWT issuer to host a public PKC JWKSet.
 */
public class JwtIssuerHttpsServer extends JwtRealmTestCase implements Closeable {
    private static final Logger LOGGER = LogManager.getLogger(JwtIssuerHttpsServer.class);

    private static final String ADDRESS = "localhost"; // localhost, 127.0.0.1, ::1, hostname, FQDN, etc
    private static final int PORT = 0; // 443, 0 (ephemeral port)
    private static final int BACKLOG = 0; // max queued incoming connections
    private static final int STOP_DELAY_SECONDS = 0; // 0 no limit, >0 limited
    private static final String PATH = "/valid/"; // Tests can call other paths like "/invalid/" to verify graceful HTTP 404 error handling

    private static final String CERT = "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt";
    private static final String KEY = "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem";
    private static final char[] PASSWORD = "testnode".toCharArray();

    final HttpsServer httpsServer;
    final String url; // JWT realm needs this for HTTP GET requests
    final String certPath; // JWT realm needs this for HTTPS handshake

    public JwtIssuerHttpsServer(final byte[] encodedJwkSetPkcPublicBytes) throws Exception {
        this.certPath = super.getDataPath(CERT).toAbsolutePath().toString();
        this.httpsServer = MockHttpServer.createHttps(new InetSocketAddress(ADDRESS, PORT), BACKLOG);
        this.url = "https://" + ADDRESS + ":" + this.httpsServer.getAddress().getPort() + PATH; // get ephemeral port
        this.httpsServer.setHttpsConfigurator(new HttpsConfigurator(this.createSslContext()));
        this.httpsServer.createContext("/", new JwtIssuerHttpHandler(encodedJwkSetPkcPublicBytes));
        LOGGER.trace("Starting [{}]", this.url);
        this.httpsServer.start();
        LOGGER.debug("Started [{}]", this.url);
    }

    @Override
    public void close() throws IOException {
        if (this.httpsServer != null) {
            LOGGER.trace("Stopping [{}]", this.url);
            this.httpsServer.stop(STOP_DELAY_SECONDS);
            LOGGER.debug("Stopped [{}]", this.url);
        }
    }

    private SSLContext createSslContext() throws Exception {
        final SSLContext sslContext = SSLContext.getInstance(randomFrom("TLSv1.2", "TLSv1.3"));
        final KeyManager keyManager = CertParsingUtils.getKeyManagerFromPEM(super.getDataPath(CERT), super.getDataPath(KEY), PASSWORD);
        sslContext.init(new KeyManager[] { keyManager }, null, JwtTestCase.SECURE_RANDOM);
        return sslContext;
    }

    private record JwtIssuerHttpHandler(byte[] encodedJwkSetPkcPublicBytes) implements HttpHandler {
        @Override
        public void handle(final HttpExchange httpExchange) throws IOException {
            try {
                final String path = httpExchange.getRequestURI().getPath(); // EX: "/", "/valid/", "/valid/pkc_jwkset.json"
                LOGGER.trace("Request: [{}]", path);
                try (OutputStream os = httpExchange.getResponseBody()) {
                    httpExchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, this.encodedJwkSetPkcPublicBytes.length);
                    os.write(this.encodedJwkSetPkcPublicBytes);
                }
                LOGGER.trace("Response: [{}]", path); // Confirm client didn't disconnect before flush
            } catch (Throwable t) {
                LOGGER.warn("Exception: ", t); // Log something, else -Djavax.net.debug=all is too verbose
                throw t;
            }
        }
    }
}

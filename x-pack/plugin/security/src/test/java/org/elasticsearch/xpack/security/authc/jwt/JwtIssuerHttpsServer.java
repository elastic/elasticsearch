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
import java.net.URL;
import java.nio.charset.StandardCharsets;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;

/**
 * HTTPS server for JWT issuer to host a public PKC JWKSet.
 */
public class JwtIssuerHttpsServer extends JwtRealmTestCase implements Closeable {
    private static final Logger LOGGER = LogManager.getLogger(JwtIssuerHttpsServer.class);

    private static final String CERT = "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt";
    private static final String KEY = "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem";
    private static final char[] PASSWORD = "testnode".toCharArray();

    final HttpsServer httpsServer;
    final String address;
    final int port;
    final int stopDelaySeconds;
    final String baseUrl; // JWT tests need this for verifying HTTP 404 (Not Found) error handling
    final String jwkSetUrl; // JWT realm needs this for HTTP GET requests
    final String certPath; // JWT realm needs this for HTTPS handshake

    public JwtIssuerHttpsServer(
        final String address, // EX: localhost, 127.0.0.1, ::1, hostname, FQDN, etc
        final int port, // EX: 443, 0 (ephemeral port)
        final int backlog, // EX: 0 no limit, >0 limited
        final int stopDelaySeconds, // EX: 0 hard stop, >0 graceful stop
        final byte[] encodedJwkSetPkcPublicBytes
    ) throws Exception {
        this.httpsServer = MockHttpServer.createHttps(new InetSocketAddress(address, port), backlog);
        this.address = address; // use parameter value, not resolved value
        this.port = this.httpsServer.getAddress().getPort(); // if parameter was 0, use resolved port
        this.baseUrl = "https://" + address + ":" + this.port + "/";
        this.jwkSetUrl = "https://" + address + ":" + this.port + "/" + (randomBoolean() ? "" : "pkc_jwk_set.json");
        this.httpsServer.setHttpsConfigurator(new HttpsConfigurator(this.createSslContext()));
        this.httpsServer.createContext("/", new JwtIssuerHttpHandler(this.jwkSetUrl, encodedJwkSetPkcPublicBytes));
        this.stopDelaySeconds = stopDelaySeconds;
        this.certPath = super.getDataPath(CERT).toFile().getAbsolutePath();
        LOGGER.trace("Starting HTTPS server for URL [" + this.jwkSetUrl + "].");
        this.httpsServer.start();
        LOGGER.debug("Started HTTPS server for URL [" + this.jwkSetUrl + "].");
    }

    @Override
    public void close() throws IOException {
        if (this.httpsServer != null) {
            LOGGER.trace("Stopping HTTPS server");
            this.httpsServer.stop(this.stopDelaySeconds);
            LOGGER.debug("Stopped HTTPS server");
        }
    }

    private SSLContext createSslContext() throws Exception {
        final SSLContext sslContext = SSLContext.getInstance(randomFrom("TLSv1.2", "TLSv1.3"));
        final KeyManager keyManager = CertParsingUtils.getKeyManagerFromPEM(super.getDataPath(CERT), super.getDataPath(KEY), PASSWORD);
        sslContext.init(new KeyManager[] { keyManager }, null, JwtTestCase.SECURE_RANDOM);
        return sslContext;
    }

    private record JwtIssuerHttpHandler(String jwkSetPkcUrl, byte[] encodedJwkSetPkcPublicBytes) implements HttpHandler {
        @Override
        public void handle(final HttpExchange httpExchange) throws IOException {
            LOGGER.trace("Received request.");
            try {
                final String configuredPath = new URL(this.jwkSetPkcUrl).getPath();
                final String requestedPath = httpExchange.getRequestURI().getPath();
                LOGGER.trace("Checking if Requested [" + requestedPath + "] matches Configured [" + configuredPath + "].");
                try (OutputStream os = httpExchange.getResponseBody()) {
                    if (configuredPath.equals(requestedPath)) {
                        LOGGER.trace("Requested [" + requestedPath + "] matched.");
                        httpExchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, this.encodedJwkSetPkcPublicBytes.length);
                        os.write(this.encodedJwkSetPkcPublicBytes);
                    } else {
                        LOGGER.trace("Requested [" + requestedPath + "] did not match.");
                        final byte[] msg = ("URL [" + requestedPath + "] != [" + configuredPath + "].").getBytes(StandardCharsets.UTF_8);
                        httpExchange.sendResponseHeaders(HttpURLConnection.HTTP_NOT_FOUND, msg.length);
                        os.write(msg);
                    }
                    LOGGER.trace("Response [" + requestedPath + "] written.");
                }
                LOGGER.trace("Response [" + requestedPath + "] closed."); // Useful in case client disconnects during close/flush
            } catch (Throwable t) {
                LOGGER.warn("Unexpected exception: ", t);
                throw t;
            }
        }
    }
}

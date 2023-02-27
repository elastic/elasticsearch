/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.testclusters;

import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;

/**
 * A utility to wait for a specific HTTP resource to be available, optionally with customized TLS trusted CAs.
 * This is logically similar to using the Ant Get task to retrieve a resource, but with the difference that it can
 * access resources that do not use the JRE's default trusted CAs.
 */
public class WaitForHttpResource {

    private static final Logger logger = Logging.getLogger(WaitForHttpResource.class);

    private final SslTrustResolver trustResolver;
    private final URL url;

    private Set<Integer> validResponseCodes = Collections.singleton(200);
    private String username;
    private String password;

    public WaitForHttpResource(String protocol, String host, int numberOfNodes) throws MalformedURLException {
        this(new URL(protocol + "://" + host + "/_cluster/health?wait_for_nodes=>=" + numberOfNodes + "&wait_for_status=yellow"));
    }

    public WaitForHttpResource(URL url) {
        this.url = url;
        this.trustResolver = new SslTrustResolver();
    }

    public void setValidResponseCodes(int... validResponseCodes) {
        this.validResponseCodes = new HashSet<>(validResponseCodes.length);
        for (int rc : validResponseCodes) {
            this.validResponseCodes.add(rc);
        }
    }

    public void setCertificateAuthorities(File... certificateAuthorities) {
        trustResolver.setCertificateAuthorities(certificateAuthorities);
    }

    public void setTrustStoreFile(File trustStoreFile) {
        trustResolver.setTrustStoreFile(trustStoreFile);
    }

    public void setTrustStorePassword(String trustStorePassword) {
        trustResolver.setTrustStorePassword(trustStorePassword);
    }

    public void setServerCertificate(File serverCertificate) {
        trustResolver.setServerCertificate(serverCertificate);
    }

    public void setServerKeystoreFile(File keyStoreFile) {
        trustResolver.setServerKeystoreFile(keyStoreFile);
    }

    public void setServerKeystorePassword(String keyStorePassword) {
        trustResolver.setServerKeystorePassword(keyStorePassword);
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public boolean wait(int durationInMs) throws GeneralSecurityException, InterruptedException, IOException {
        final long waitUntil = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(durationInMs);
        final long sleep = Long.max(durationInMs / 10, 100);

        final SSLContext ssl = trustResolver.getSslContext();
        IOException failure = null;
        while (true) {
            try {
                checkResource(ssl);
                return true;
            } catch (IOException e) {
                logger.debug("Failed to access resource [{}]", url, e);
                failure = e;
            }
            if (System.nanoTime() < waitUntil) {
                Thread.sleep(sleep);
            } else {
                throw failure;
            }
        }
    }

    protected void checkResource(SSLContext ssl) throws IOException {
        final HttpURLConnection connection = buildConnection(ssl);
        connection.connect();
        final Integer response = connection.getResponseCode();
        if (validResponseCodes.contains(response)) {
            logger.info("Got successful response [{}] from URL [{}]", response, url);
            return;
        } else {
            throw new IOException(response + " " + connection.getResponseMessage());
        }
    }

    HttpURLConnection buildConnection(SSLContext ssl) throws IOException {
        final HttpURLConnection connection = (HttpURLConnection) this.url.openConnection();
        configureSslContext(connection, ssl);
        configureBasicAuth(connection);
        connection.setRequestMethod("GET");
        return connection;
    }

    private void configureSslContext(HttpURLConnection connection, SSLContext ssl) {
        if (ssl != null) {
            if (connection instanceof HttpsURLConnection) {
                ((HttpsURLConnection) connection).setSSLSocketFactory(ssl.getSocketFactory());
            } else {
                throw new IllegalStateException("SSL trust has been configured, but [" + url + "] is not a 'https' URL");
            }
        }
    }

    private void configureBasicAuth(HttpURLConnection connection) {
        if (username != null) {
            if (password == null) {
                throw new IllegalStateException("Basic Auth user [" + username + "] has been set, but no password has been configured");
            }
            connection.setRequestProperty(
                "Authorization",
                "Basic " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8))
            );
        }
    }
}

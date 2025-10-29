/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ssl;

import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.elasticsearch.common.ssl.SslConfiguration;

import java.util.function.Consumer;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSocketFactory;

/**
 * An <em>SSL Profile</em> is a runtime implementation of a {@link SslConfiguration}.
 * It provides access to various SSL related objects that are automatically configured according to the associated {@link SslConfiguration}.
 */
public interface SslProfile {
    SslConfiguration configuration();

    SSLContext sslContext();

    /**
     * Create a new {@link SSLSocketFactory} based on the provided configuration.
     * The socket factory will also properly configure the ciphers and protocols on each socket that is created
     *
     * @return Never {@code null}.
     */
    SSLSocketFactory socketFactory();

    HostnameVerifier hostnameVerifier();

    SSLConnectionSocketFactory connectionSocketFactory();

    /**
     * @return An object that is useful for configuring Apache Http Client v4.x
     */
    SSLIOSessionStrategy ioSessionStrategy();

    /**
     * @return An object that is useful for configuring Apache Http Client v5.x
     */
    TlsStrategy clientTlsStrategy();

    SSLEngine engine(String host, int port);

    /**
     * Add a listener that is called when this profile is reloaded (for example, because one of the {@link #configuration() configuration's}
     * {@link SslConfiguration#getDependentFiles() dependent files} is modified.
     */
    void addReloadListener(Consumer<SslProfile> listener);
}

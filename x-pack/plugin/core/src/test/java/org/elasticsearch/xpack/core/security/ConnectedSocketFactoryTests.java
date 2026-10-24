/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.CommandLineHttpClient.ConnectedSocketFactory;

import java.net.SocketException;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class ConnectedSocketFactoryTests extends ESTestCase {

    /**
     * {@code HttpsURLConnection} only connects a plain socket and layers TLS over it with the URL's host name, which every JSSE provider
     * honours for endpoint identification, when the factory refuses to create an unconnected socket with exactly this exception.
     */
    public void testDeclinesUnconnectedSockets() throws Exception {
        final SSLSocketFactory delegate = SSLContext.getDefault().getSocketFactory();
        final ConnectedSocketFactory factory = new ConnectedSocketFactory(delegate);
        final SocketException e = expectThrows(SocketException.class, factory::createSocket);
        assertThat(e.getCause(), instanceOf(UnsupportedOperationException.class));
    }

    public void testDelegatesCipherSuites() throws Exception {
        final SSLSocketFactory delegate = SSLContext.getDefault().getSocketFactory();
        final ConnectedSocketFactory factory = new ConnectedSocketFactory(delegate);
        assertThat(factory.getDefaultCipherSuites(), equalTo(delegate.getDefaultCipherSuites()));
        assertThat(factory.getSupportedCipherSuites(), equalTo(delegate.getSupportedCipherSuites()));
    }
}

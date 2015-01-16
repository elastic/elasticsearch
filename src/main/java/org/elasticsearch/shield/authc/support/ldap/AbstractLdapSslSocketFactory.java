/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.support.ldap;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.shield.ssl.SSLService;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.InetAddress;

/**
 * Abstract class that wraps a SSLSocketFactory and uses it to create sockets for use with LDAP via JNDI
 */
public abstract class AbstractLdapSslSocketFactory extends SocketFactory {

    protected static SSLService sslService;

    private final SSLSocketFactory socketFactory;

    /**
     * This should only be invoked once to establish a static instance that will be used for each constructor.
     */
    @Inject
    public static void init(SSLService sslService) {
        AbstractLdapSslSocketFactory.sslService = sslService;
    }

    public AbstractLdapSslSocketFactory(SSLSocketFactory sslSocketFactory) {
        socketFactory = sslSocketFactory;
    }

    //The following methods are all wrappers around the instance of socketFactory

    @Override
    public SSLSocket createSocket() throws IOException {
        SSLSocket socket = (SSLSocket) socketFactory.createSocket();
        configureSSLSocket(socket);
        return socket;
    }

    @Override
    public SSLSocket createSocket(String host, int port) throws IOException {
        SSLSocket socket = (SSLSocket) socketFactory.createSocket(host, port);
        configureSSLSocket(socket);
        return socket;
    }

    @Override
    public SSLSocket createSocket(String host, int port, InetAddress localHost, int localPort) throws IOException {
        SSLSocket socket = (SSLSocket) socketFactory.createSocket(host, port, localHost, localPort);
        configureSSLSocket(socket);
        return socket;
    }

    @Override
    public SSLSocket createSocket(InetAddress host, int port) throws IOException {
        SSLSocket socket = (SSLSocket) socketFactory.createSocket(host, port);
        configureSSLSocket(socket);
        return socket;
    }

    @Override
    public SSLSocket createSocket(InetAddress host, int port, InetAddress localHost, int localPort) throws IOException {
        SSLSocket socket = (SSLSocket) socketFactory.createSocket(host, port, localHost, localPort);
        configureSSLSocket(socket);
        return socket;
    }

    /**
     * This method allows for performing additional configuration on each socket. All 'createSocket' methods will
     * call this method before returning the socket to the caller. The default implementation is a no-op
     * @param sslSocket
     */
    protected void configureSSLSocket(SSLSocket sslSocket) {
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.support.ldap;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.net.SocketFactory;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

/**
 * This factory is needed for JNDI configuration for LDAP connections with hostname verification. Each SSLSocket must
 * have the appropriate SSLParameters set to indicate that hostname verification is required
 */
public class HostnameVerifyingLdapSslSocketFactory extends AbstractLdapSslSocketFactory {

    private static final ESLogger logger = Loggers.getLogger(HostnameVerifyingLdapSslSocketFactory.class);

    private static HostnameVerifyingLdapSslSocketFactory instance;
    private final SSLParameters sslParameters;

    public HostnameVerifyingLdapSslSocketFactory(SSLSocketFactory socketFactory) {
        super(socketFactory);
        sslParameters = new SSLParameters();
        sslParameters.setEndpointIdentificationAlgorithm("LDAPS");
    }

    /**
     * This is invoked by JNDI and the returned SocketFactory must be an HostnameVerifyingLdapSslSocketFactory object
     *
     * @return a singleton instance of HostnameVerifyingLdapSslSocketFactory set by calling the init static method.
     */
    public static synchronized SocketFactory getDefault() {
        if (instance == null) {
            instance = new HostnameVerifyingLdapSslSocketFactory(clientSSLService.getSSLSocketFactory());
        }
        return instance;
    }

    /**
     * This clears the static factory.  There are threading issues with this.  But for
     * testing this is useful.
     *
     * WARNING: THIS METHOD SHOULD ONLY BE CALLED IN TESTS!!!!
     *
     * TODO: find a way to change the tests such that we can remove this method
     */
    public static void clear() {
        logger.error("clear should only be called by tests");
        instance = null;
    }

    /**
     * Configures the socket to require hostname verification using the LDAPS
     * @param sslSocket
     */
    @Override
    protected void configureSSLSocket(SSLSocket sslSocket) {
        super.configureSSLSocket(sslSocket);
        sslSocket.setSSLParameters(sslParameters);
    }
}

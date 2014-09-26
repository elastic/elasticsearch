/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.transport.ssl.SSLTrustConfig;

import javax.net.SocketFactory;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Locale;

/**
 * This factory is needed for JNDI configuration for LDAP connections.  It wraps a single instance of a static
 * factory that is initiated by the settings constructor.  JNDI uses reflection to call the getDefault() static method
 * then checks to make sure that the factory returned is an LdapSslSocketFactory.  Because of this we have to wrap
 * the socket factory
 *
 * http://docs.oracle.com/javase/tutorial/jndi/ldap/ssl.html
 */
public class LdapSslSocketFactory extends SocketFactory {

    private static ESLogger logger = ESLoggerFactory.getLogger(LdapSslSocketFactory.class.getName());
    private static LdapSslSocketFactory instance;

    /**
     * This should only be invoked once to establish a static instance that will be used for each constructor.
     */
    public static void init(Settings settings) {
        if (instance != null) {
            logger.error("LdapSslSocketFactory already configured, this change could lead to threading issues");
        }

        Settings componentSettings = settings.getComponentSettings(LdapSslSocketFactory.class);
        SSLTrustConfig sslConfig = new SSLTrustConfig(componentSettings, settings.getByPrefix("shield.ssl."));
        instance = new LdapSslSocketFactory(sslConfig.createSSLSocketFactory());
    }

    /**
     * This is invoked by JNDI and the returned SocketFactory must be an LdapSslSocketFactory object
     * @return
     */
    public static SocketFactory getDefault() {
        assert instance != null;
        return instance;
    }

    public static boolean initialized() {
        return instance != null;
    }

    final private SocketFactory socketFactory;

    private LdapSslSocketFactory(SocketFactory wrappedSocketFactory){
        socketFactory = wrappedSocketFactory;
    }

    //The following methods are all wrappers around the instance of socketFactory

    @Override
    public Socket createSocket(String s, int i) throws IOException {
        return socketFactory.createSocket(s, i);
    }

    @Override
    public Socket createSocket(String s, int i, InetAddress inetAddress, int i2) throws IOException {
        return socketFactory.createSocket(s, i, inetAddress, i2);
    }

    @Override
    public Socket createSocket(InetAddress inetAddress, int i) throws IOException {
        return socketFactory.createSocket(inetAddress, i);
    }

    @Override
    public Socket createSocket(InetAddress inetAddress, int i, InetAddress inetAddress2, int i2) throws IOException {
        return socketFactory.createSocket(inetAddress, i, inetAddress2, i2);
    }

    /**
     * If one of the ldapUrls are SSL this will set the LdapSslSocketFactory as a socket provider on the builder
     * @param ldapUrls
     * @param builder set of jndi properties, that will
     */
    public static void configureJndiSSL(String[] ldapUrls, ImmutableMap.Builder<String, Serializable> builder) {
        boolean needsSSL = false;
        for(String url: ldapUrls){
            if (url.toLowerCase(Locale.getDefault()).startsWith("ldaps://")) {
                needsSSL = true;
                break;
            }
        }
        if (needsSSL) {
            assert instance != null : "LdapSslSocketFactory not initialized and won't be used for LDAP connections";
            builder.put("java.naming.ldap.factory.socket", LdapSslSocketFactory.class.getName());
        } else {
            logger.debug("LdapSslSocketFactory not used for LDAP connections");
        }
    }
}

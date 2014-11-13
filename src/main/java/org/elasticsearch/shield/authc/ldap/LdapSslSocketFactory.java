/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.shield.ShieldSettingsException;
import org.elasticsearch.shield.ssl.SSLService;

import javax.net.SocketFactory;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.Socket;
import java.util.regex.Pattern;

import static java.util.Arrays.asList;
import static org.elasticsearch.common.base.Predicates.contains;
import static org.elasticsearch.common.collect.Iterables.all;

/**
 * This factory is needed for JNDI configuration for LDAP connections.  It wraps a single instance of a static
 * factory that is initiated by the settings constructor.  JNDI uses reflection to call the getDefault() static method
 * then checks to make sure that the factory returned is an LdapSslSocketFactory.  Because of this we have to wrap
 * the socket factory
 *
 * http://docs.oracle.com/javase/tutorial/jndi/ldap/ssl.html
 */
public class LdapSslSocketFactory extends SocketFactory {

    static final String JAVA_NAMING_LDAP_FACTORY_SOCKET = "java.naming.ldap.factory.socket";
    private static final Pattern STARTS_WITH_LDAPS = Pattern.compile("^ldaps:.*", Pattern.CASE_INSENSITIVE);
    private static final Pattern STARTS_WITH_LDAP = Pattern.compile("^ldap:.*", Pattern.CASE_INSENSITIVE);

    private static ESLogger logger = ESLoggerFactory.getLogger(LdapSslSocketFactory.class.getName());

    private static LdapSslSocketFactory instance;

    /**
     * This should only be invoked once to establish a static instance that will be used for each constructor.
     */
    @Inject
    public static void init(SSLService ssl) {
        if (instance != null) {
            logger.error("LdapSslSocketFactory already configured, this change could lead to threading issues");
        }
        if (ssl == null) {
            logger.warn("no keystore has been configured for LDAP");
        } else {
            instance = new LdapSslSocketFactory(ssl.getSSLSocketFactory());
        }
    }

    /**
     * This clears the static factory.  There are threading issues with this.  But for
     * testing this is useful.
     */
    @Deprecated
    static void clear() {
        logger.error("clear should only be called by tests");
        instance = null;
    }

    /**
     * This is invoked by JNDI and the returned SocketFactory must be an LdapSslSocketFactory object
     * @return a singleton instance of LdapSslSocketFactory set by calling the init static method.
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
     * @param ldapUrls array of ldap urls, either all SSL or none with SSL (no mixing)
     * @param builder set of jndi properties, that will
     * @throws org.elasticsearch.shield.ShieldSettingsException if URLs have mixed protocols.
     */
    public static void configureJndiSSL(String[] ldapUrls, ImmutableMap.Builder<String, Serializable> builder) {
        boolean secureProtocol = secureUrls(ldapUrls);
        if (secureProtocol && instance != null) {
            builder.put(JAVA_NAMING_LDAP_FACTORY_SOCKET, LdapSslSocketFactory.class.getName());
        } else {
            logger.warn("LdapSslSocketFactory not used for LDAP connections");
            if (logger.isDebugEnabled()) {
                logger.debug("LdapSslSocketFactory: secureProtocol = [{}], instance != null [{}]", secureProtocol, instance != null);
            }
        }
    }

    /**
     * @param ldapUrls URLS in the form of "ldap://..." or "ldaps://..."
     * @return true if all URLS are ldaps, also true it ldapUrls is empty.  False otherwise
     */
    public static boolean secureUrls(String[] ldapUrls) {
        if (ldapUrls.length == 0) {
            return true;
        }

        boolean allSecure = all(asList(ldapUrls), contains(STARTS_WITH_LDAPS));
        boolean allClear = all(asList(ldapUrls), contains(STARTS_WITH_LDAP));

        if (!allSecure && !allClear) {
            //No mixing is allowed because LdapSSLSocketFactory produces only SSL sockets and not clear text sockets
            throw new ShieldSettingsException("Configured ldap protocols are not all equal " +
                    "(ldaps://.. and ldap://..): [" + Strings.arrayToCommaDelimitedString(ldapUrls) + "]");
        }
        return allSecure;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.support.ldap;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.shield.ShieldSettingsException;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.support.SecuredString;

import java.io.Serializable;
import java.util.regex.Pattern;

import static java.util.Arrays.asList;
import static org.elasticsearch.common.base.Predicates.contains;
import static org.elasticsearch.common.collect.Iterables.all;

/**
 * This factory holds settings needed for authenticating to LDAP and creating LdapConnections.
 * Each created LdapConnection needs to be closed or else connections will pill up consuming resources.
 *
 * A standard looking usage pattern could look like this:
 <pre>
    ConnectionFactory factory = ...
    try (LdapConnection session = factory.open(...)) {
        ...do stuff with the session
    }
 </pre>
 */
public abstract class ConnectionFactory<Connection extends AbstractLdapConnection> {

    public static final String URLS_SETTING = "url";
    public static final String JNDI_LDAP_READ_TIMEOUT = "com.sun.jndi.ldap.read.timeout";
    public static final String JNDI_LDAP_CONNECT_TIMEOUT = "com.sun.jndi.ldap.connect.timeout";
    public static final String TIMEOUT_TCP_CONNECTION_SETTING = "timeout.tcp_connect";
    public static final String TIMEOUT_TCP_READ_SETTING = "timeout.tcp_read";
    public static final String TIMEOUT_LDAP_SETTING = "timeout.ldap_search";
    public static final String HOSTNAME_VERIFICATION_SETTING = "hostname_verification";
    public static final TimeValue TIMEOUT_DEFAULT = TimeValue.timeValueSeconds(5);
    static final String JAVA_NAMING_LDAP_FACTORY_SOCKET = "java.naming.ldap.factory.socket";
    private static final Pattern STARTS_WITH_LDAPS = Pattern.compile("^ldaps:.*", Pattern.CASE_INSENSITIVE);
    private static final Pattern STARTS_WITH_LDAP = Pattern.compile("^ldap:.*", Pattern.CASE_INSENSITIVE);

    protected final ESLogger logger;
    protected final ESLogger connectionLogger;
    protected final RealmConfig config;

    protected ConnectionFactory(Class<Connection> connectionClass, RealmConfig config) {
        this.config = config;
        this.logger = config.logger(getClass());
        this.connectionLogger = config.logger(connectionClass);
    }

    /**
     * Authenticates the given user and opens a new connection that bound to it (meaning, all operations
     * under the returned connection will be executed on behalf of the authenticated user.
     *
     * @param user      The name of the user to authenticate the connection with.
     * @param password  The password of the user
     */
    public abstract Connection open(String user, SecuredString password) ;

    /**
     * If one of the ldapUrls are SSL this will set the LdapSslSocketFactory as a socket provider on the builder
     *
     * @param ldapUrls array of ldap urls, either all SSL or none with SSL (no mixing)
     * @param builder  set of jndi properties, that will
     * @throws org.elasticsearch.shield.ShieldSettingsException if URLs have mixed protocols.
     */
    protected void configureJndiSSL(String[] ldapUrls, ImmutableMap.Builder<String, Serializable> builder) {
        boolean secureProtocol = secureUrls(ldapUrls);
        if (secureProtocol) {
            if (config.settings().getAsBoolean(HOSTNAME_VERIFICATION_SETTING, true)) {
                builder.put(JAVA_NAMING_LDAP_FACTORY_SOCKET, HostnameVerifyingLdapSslSocketFactory.class.getName());
                logger.debug("using encryption for LDAP connections with hostname verification");
            } else {
                builder.put(JAVA_NAMING_LDAP_FACTORY_SOCKET, LdapSslSocketFactory.class.getName());
                logger.debug("using encryption for LDAP connections without hostname verification");
            }
        } else {
            logger.warn("encryption not used for LDAP connections");
        }
    }

    /**
     * @param ldapUrls URLS in the form of "ldap://..." or "ldaps://..."
     * @return true if all URLS are ldaps, also true it ldapUrls is empty.  False otherwise
     */
    private boolean secureUrls(String[] ldapUrls) {
        if (ldapUrls.length == 0) {
            return true;
        }

        boolean allSecure = all(asList(ldapUrls), contains(STARTS_WITH_LDAPS));
        boolean allClear = all(asList(ldapUrls), contains(STARTS_WITH_LDAP));

        if (!allSecure && !allClear) {
            //No mixing is allowed because LdapSSLSocketFactory produces only SSL sockets and not clear text sockets
            throw new ShieldSettingsException("configured LDAP protocols are not all equal " +
                    "(ldaps://.. and ldap://..): [" + Strings.arrayToCommaDelimitedString(ldapUrls) + "]");
        }
        return allSecure;
    }
}

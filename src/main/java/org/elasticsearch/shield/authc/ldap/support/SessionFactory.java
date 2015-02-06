/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap.support;

import com.unboundid.ldap.sdk.LDAPConnectionOptions;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.LDAPURL;
import com.unboundid.ldap.sdk.ServerSet;
import com.unboundid.util.ssl.HostNameSSLSocketVerifier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.primitives.Ints;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.shield.ShieldSettingsException;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.ssl.ClientSSLService;

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
public abstract class SessionFactory {

    public static final String URLS_SETTING = "url";
    public static final String TIMEOUT_TCP_CONNECTION_SETTING = "timeout.tcp_connect";
    public static final String TIMEOUT_TCP_READ_SETTING = "timeout.tcp_read";
    public static final String TIMEOUT_LDAP_SETTING = "timeout.ldap_search";
    public static final String HOSTNAME_VERIFICATION_SETTING = "hostname_verification";
    public static final String FOLLOW_REFERRALS_SETTING = "follow_referrals";
    public static final TimeValue TIMEOUT_DEFAULT = TimeValue.timeValueSeconds(5);
    private static final Pattern STARTS_WITH_LDAPS = Pattern.compile("^ldaps:.*", Pattern.CASE_INSENSITIVE);
    private static final Pattern STARTS_WITH_LDAP = Pattern.compile("^ldap:.*", Pattern.CASE_INSENSITIVE);

    protected final ESLogger logger;
    protected final ESLogger connectionLogger;
    protected final RealmConfig config;
    protected final TimeValue timeout;

    protected SessionFactory(RealmConfig config) {
        this.config = config;
        this.logger = config.logger(getClass());
        this.connectionLogger = config.logger(getClass());
        TimeValue searchTimeout = config.settings().getAsTime(TIMEOUT_LDAP_SETTING, TIMEOUT_DEFAULT);
        if (searchTimeout.millis() < 1000L) {
            logger.warn("ldap_search timeout [{}] is less than the minimum supported search timeout of 1s. using 1s", searchTimeout.millis());
            searchTimeout = TimeValue.timeValueSeconds(1L);
        }
        this.timeout = searchTimeout;
    }

    /**
     * Authenticates the given user and opens a new connection that bound to it (meaning, all operations
     * under the returned connection will be executed on behalf of the authenticated user.
     *
     * @param user      The name of the user to authenticate the connection with.
     * @param password  The password of the user
     */
    public abstract LdapSession open(String user, SecuredString password);

    protected static LDAPConnectionOptions connectionOptions(Settings settings) {
        LDAPConnectionOptions options = new LDAPConnectionOptions();
        options.setConnectTimeoutMillis(Ints.checkedCast(settings.getAsTime(TIMEOUT_TCP_CONNECTION_SETTING, TIMEOUT_DEFAULT).millis()));
        options.setFollowReferrals(settings.getAsBoolean(FOLLOW_REFERRALS_SETTING, true));
        options.setResponseTimeoutMillis(settings.getAsTime(TIMEOUT_TCP_READ_SETTING, TIMEOUT_DEFAULT).millis());
        options.setAutoReconnect(true);
        options.setAllowConcurrentSocketFactoryUse(true);
        if (settings.getAsBoolean(HOSTNAME_VERIFICATION_SETTING, true)) {
            options.setSSLSocketVerifier(new HostNameSSLSocketVerifier(true));
        }
        return options;
    }

    public static class LDAPServers {

        private final String[] addresses;
        private final int[] ports;
        private final boolean ssl;

        public LDAPServers(String[] urls) {
            ssl = secureUrls(urls);
            addresses = new String[urls.length];
            ports = new int[urls.length];
            for (int i = 0; i < urls.length; i++) {
                try {
                    LDAPURL url = new LDAPURL(urls[i]);
                    addresses[i] = url.getHost();
                    ports[i] = url.getPort();
                } catch (LDAPException e) {
                    throw new ShieldSettingsException("unable to parse configured LDAP url [" + urls[i] +"]", e);
                }
            }
        }

        public String[] addresses() {
            return addresses;
        }

        public int[] ports() {
            return ports;
        }

        public boolean ssl() {
            return ssl;
        }

        /**
         * @param ldapUrls URLS in the form of "ldap://..." or "ldaps://..."
         */
        private boolean secureUrls(String[] ldapUrls) {
            if (ldapUrls.length == 0) {
                return true;
            }

            boolean allSecure = all(asList(ldapUrls), contains(STARTS_WITH_LDAPS));
            boolean allClear = all(asList(ldapUrls), contains(STARTS_WITH_LDAP));

            if (!allSecure && !allClear) {
                //No mixing is allowed because we use the same socketfactory
                throw new ShieldSettingsException("configured LDAP protocols are not all equal " +
                        "(ldaps://.. and ldap://..): [" + Strings.arrayToCommaDelimitedString(ldapUrls) + "]");
            }

            return allSecure;
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap.support;

import com.unboundid.ldap.sdk.LDAPConnectionOptions;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.LDAPURL;
import com.unboundid.ldap.sdk.ServerSet;
import com.unboundid.util.ssl.HostNameSSLSocketVerifier;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.support.SessionFactorySettings;
import org.elasticsearch.xpack.core.ssl.SSLConfiguration;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;

import javax.net.SocketFactory;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * This factory holds settings needed for authenticating to LDAP and creating LdapConnections.
 * Each created LdapConnection needs to be closed or else connections will pill up consuming
 * resources.
 * <p>
 * A standard looking usage pattern could look like this:
 * <pre>
 * ConnectionFactory factory = ...
 * try (LdapConnection session = factory.session(...)) {
 * ...do stuff with the session
 * }
 * </pre>
 */
public abstract class SessionFactory {

    private static final Pattern STARTS_WITH_LDAPS = Pattern.compile("^ldaps:.*",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern STARTS_WITH_LDAP = Pattern.compile("^ldap:.*",
            Pattern.CASE_INSENSITIVE);

    protected final Logger logger;
    protected final RealmConfig config;
    protected final TimeValue timeout;
    protected final SSLService sslService;
    protected final ThreadPool threadPool;

    protected final ServerSet serverSet;
    protected final boolean sslUsed;
    protected final boolean ignoreReferralErrors;

    protected final LdapMetadataResolver metadataResolver;

    protected SessionFactory(RealmConfig config, SSLService sslService, ThreadPool threadPool) {
        this.config = config;
        this.logger = LogManager.getLogger(getClass());
        TimeValue searchTimeout = config.getSetting(SessionFactorySettings.TIMEOUT_LDAP_SETTING,
                () -> SessionFactorySettings.TIMEOUT_DEFAULT);
        if (searchTimeout.millis() < 1000L) {
            logger.warn("ldap_search timeout [{}] is less than the minimum supported search " +
                            "timeout of 1s. using 1s",
                    searchTimeout.millis());
            searchTimeout = TimeValue.timeValueSeconds(1L);
        }
        this.timeout = searchTimeout;
        this.sslService = sslService;
        this.threadPool = threadPool;
        LDAPServers ldapServers = ldapServers(config);
        this.serverSet = serverSet(config, sslService, ldapServers);
        this.sslUsed = ldapServers.ssl;
        this.ignoreReferralErrors = config.getSetting(SessionFactorySettings.IGNORE_REFERRAL_ERRORS_SETTING);
        this.metadataResolver = new LdapMetadataResolver(config, ignoreReferralErrors);
    }

    /**
     * Authenticates the given user and opens a new connection that bound to it (meaning, all
     * operations under the returned connection will be executed on behalf of the authenticated
     * user.
     *
     * @param user     The name of the user to authenticate the connection with.
     * @param password The password of the user
     * @param listener the listener to call on a failure or result
     */
    public abstract void session(String user, SecureString password,
                                 ActionListener<LdapSession> listener);

    /**
     * Returns a flag to indicate if this session factory supports unauthenticated sessions.
     * This means that a session can be established without providing any credentials in a call to
     * {@link #unauthenticatedSession(String, ActionListener)}
     *
     * @return true if the factory supports unauthenticated sessions
     */
    public boolean supportsUnauthenticatedSession() {
        return false;
    }

    /**
     * Returns an {@link LdapSession} for the user identified by the String parameter
     *
     * @param username the identifier for the user
     * @param listener the listener to call on a failure or result
     */
    public void unauthenticatedSession(String username, ActionListener<LdapSession> listener) {
        throw new UnsupportedOperationException("unauthenticated sessions are not supported");
    }

    protected static LDAPConnectionOptions connectionOptions(RealmConfig config,
                                                             SSLService sslService, Logger logger) {
        LDAPConnectionOptions options = new LDAPConnectionOptions();
        options.setConnectTimeoutMillis(Math.toIntExact(config.getSetting(SessionFactorySettings.TIMEOUT_TCP_CONNECTION_SETTING).millis()));
        options.setFollowReferrals(config.getSetting(SessionFactorySettings.FOLLOW_REFERRALS_SETTING));
        final long responseTimeoutMillis;
        if (config.hasSetting(SessionFactorySettings.TIMEOUT_RESPONSE_SETTING)) {
            if (config.hasSetting(SessionFactorySettings.TIMEOUT_TCP_READ_SETTING)) {
                throw new IllegalArgumentException("[" + RealmSettings.getFullSettingKey(config,
                        SessionFactorySettings.TIMEOUT_TCP_READ_SETTING) + "] and [" + RealmSettings.getFullSettingKey(config,
                        SessionFactorySettings.TIMEOUT_RESPONSE_SETTING) + "] may not be used at the same time");
            }
            responseTimeoutMillis = config.getSetting(SessionFactorySettings.TIMEOUT_RESPONSE_SETTING).millis();
        } else {
            if (config.hasSetting(SessionFactorySettings.TIMEOUT_TCP_READ_SETTING)) {
                responseTimeoutMillis = config.getSetting(SessionFactorySettings.TIMEOUT_TCP_READ_SETTING).millis();
            } else {
                responseTimeoutMillis = config.getSetting(SessionFactorySettings.TIMEOUT_LDAP_SETTING).millis();
            }
        }
        options.setResponseTimeoutMillis(responseTimeoutMillis);
        options.setAllowConcurrentSocketFactoryUse(true);

        final boolean verificationModeExists = config.hasSetting(SSLConfigurationSettings.VERIFICATION_MODE_SETTING_REALM);
        final boolean hostnameVerificationExists = config.hasSetting(SessionFactorySettings.HOSTNAME_VERIFICATION_SETTING);

        if (verificationModeExists && hostnameVerificationExists) {
            throw new IllegalArgumentException("[" +
                    RealmSettings.getFullSettingKey(config, SessionFactorySettings.HOSTNAME_VERIFICATION_SETTING) + "] and [" +
                    RealmSettings.getFullSettingKey(config, SSLConfigurationSettings.VERIFICATION_MODE_SETTING_REALM) +
                    "] may not be used at the same time");
        } else if (verificationModeExists) {
            final String sslKey = RealmSettings.realmSslPrefix(config.identifier());
            final SSLConfiguration sslConfiguration = sslService.getSSLConfiguration(sslKey);
            if (sslConfiguration == null) {
                throw new IllegalStateException("cannot find SSL configuration for " + sslKey);
            }
            if (sslConfiguration.verificationMode().isHostnameVerificationEnabled()) {
                options.setSSLSocketVerifier(new HostNameSSLSocketVerifier(true));
            }
        } else if (hostnameVerificationExists) {
            final String fullSettingKey = RealmSettings.getFullSettingKey(config, SessionFactorySettings.HOSTNAME_VERIFICATION_SETTING);
            final String deprecationKey = "deprecated_setting_" + fullSettingKey.replace('.', '_');
            new DeprecationLogger(logger).deprecatedAndMaybeLog(
                deprecationKey,
                "the setting [{}] has been deprecated and " + "will be removed in a future version. use [{}] instead",
                fullSettingKey,
                RealmSettings.getFullSettingKey(config, SSLConfigurationSettings.VERIFICATION_MODE_SETTING_REALM)
            );
            if (config.getSetting(SessionFactorySettings.HOSTNAME_VERIFICATION_SETTING)) {
                options.setSSLSocketVerifier(new HostNameSSLSocketVerifier(true));
            }
        } else {
            options.setSSLSocketVerifier(new HostNameSSLSocketVerifier(true));
        }
        return options;
    }

    private LDAPServers ldapServers(RealmConfig config) {
        // Parse LDAP urls
        List<String> ldapUrls = config.getSetting(SessionFactorySettings.URLS_SETTING, () -> getDefaultLdapUrls(config));
        if (ldapUrls == null || ldapUrls.isEmpty()) {
            throw new IllegalArgumentException("missing required LDAP setting ["
                    + RealmSettings.getFullSettingKey(config, SessionFactorySettings.URLS_SETTING) + "]");
        }
        return new LDAPServers(ldapUrls.toArray(new String[ldapUrls.size()]));
    }

    protected List<String> getDefaultLdapUrls(RealmConfig config) {
        return null;
    }

    private ServerSet serverSet(RealmConfig realmConfig, SSLService clientSSLService,
                                LDAPServers ldapServers) {
        SocketFactory socketFactory = null;
        if (ldapServers.ssl()) {
            final String sslKey = RealmSettings.realmSslPrefix(config.identifier());
            final SSLConfiguration ssl = clientSSLService.getSSLConfiguration(sslKey);
            socketFactory = clientSSLService.sslSocketFactory(ssl);
            if (ssl.verificationMode().isHostnameVerificationEnabled()) {
                logger.debug("using encryption for LDAP connections with hostname verification");
            } else {
                logger.debug("using encryption for LDAP connections without hostname verification");
            }
        }
        return LdapLoadBalancing.serverSet(ldapServers.addresses(), ldapServers.ports(), realmConfig,
                socketFactory, connectionOptions(realmConfig, sslService, logger));
    }

    // package private to use for testing
    ServerSet getServerSet() {
        return serverSet;
    }

    public boolean isSslUsed() {
        return sslUsed;
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
                    throw new IllegalArgumentException("unable to parse configured LDAP url [" +
                            urls[i] + "]", e);
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

            final boolean allSecure = Arrays.stream(ldapUrls)
                    .allMatch(s -> STARTS_WITH_LDAPS.matcher(s).find());
            final boolean allClear = Arrays.stream(ldapUrls)
                    .allMatch(s -> STARTS_WITH_LDAP.matcher(s).find());

            if (!allSecure && !allClear) {
                //No mixing is allowed because we use the same socketfactory
                throw new IllegalArgumentException(
                        "configured LDAP protocols are not all equal (ldaps://.. and ldap://..): ["
                                + Strings.arrayToCommaDelimitedString(ldapUrls) + "]");
            }

            return allSecure;
        }
    }
}

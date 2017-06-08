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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.authc.RealmSettings;
import org.elasticsearch.xpack.ssl.SSLConfigurationSettings;
import org.elasticsearch.xpack.ssl.SSLService;
import org.elasticsearch.xpack.ssl.VerificationMode;

import javax.net.SocketFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
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

    public static final String URLS_SETTING = "url";
    public static final String TIMEOUT_TCP_CONNECTION_SETTING = "timeout.tcp_connect";
    public static final String TIMEOUT_TCP_READ_SETTING = "timeout.tcp_read";
    public static final String TIMEOUT_LDAP_SETTING = "timeout.ldap_search";
    public static final String HOSTNAME_VERIFICATION_SETTING = "hostname_verification";
    public static final String FOLLOW_REFERRALS_SETTING = "follow_referrals";
    public static final Setting<Boolean> IGNORE_REFERRAL_ERRORS_SETTING = Setting.boolSetting(
            "ignore_referral_errors", true, Setting.Property.NodeScope);

    public static final TimeValue TIMEOUT_DEFAULT = TimeValue.timeValueSeconds(5);
    private static final Pattern STARTS_WITH_LDAPS = Pattern.compile("^ldaps:.*",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern STARTS_WITH_LDAP = Pattern.compile("^ldap:.*",
            Pattern.CASE_INSENSITIVE);

    protected final Logger logger;
    protected final RealmConfig config;
    protected final TimeValue timeout;
    protected final SSLService sslService;

    protected final ServerSet serverSet;
    protected final boolean sslUsed;
    protected final boolean ignoreReferralErrors;

    protected SessionFactory(RealmConfig config, SSLService sslService) {
        this.config = config;
        this.logger = config.logger(getClass());
        final Settings settings = config.settings();
        TimeValue searchTimeout = settings.getAsTime(TIMEOUT_LDAP_SETTING, TIMEOUT_DEFAULT);
        if (searchTimeout.millis() < 1000L) {
            logger.warn("ldap_search timeout [{}] is less than the minimum supported search " +
                            "timeout of 1s. using 1s",
                    searchTimeout.millis());
            searchTimeout = TimeValue.timeValueSeconds(1L);
        }
        this.timeout = searchTimeout;
        this.sslService = sslService;
        LDAPServers ldapServers = ldapServers(settings);
        this.serverSet = serverSet(config, sslService, ldapServers);
        this.sslUsed = ldapServers.ssl;
        this.ignoreReferralErrors = IGNORE_REFERRAL_ERRORS_SETTING.get(settings);
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
        Settings realmSettings = config.settings();
        LDAPConnectionOptions options = new LDAPConnectionOptions();
        options.setConnectTimeoutMillis(Math.toIntExact(
                realmSettings.getAsTime(TIMEOUT_TCP_CONNECTION_SETTING, TIMEOUT_DEFAULT).millis()
        ));
        options.setFollowReferrals(realmSettings.getAsBoolean(FOLLOW_REFERRALS_SETTING, true));
        options.setResponseTimeoutMillis(
                realmSettings.getAsTime(TIMEOUT_TCP_READ_SETTING, TIMEOUT_DEFAULT).millis()
        );
        options.setAllowConcurrentSocketFactoryUse(true);

        final SSLConfigurationSettings sslConfigurationSettings =
                SSLConfigurationSettings.withoutPrefix();
        final Settings realmSSLSettings = realmSettings.getByPrefix("ssl.");
        final boolean verificationModeExists =
                sslConfigurationSettings.verificationMode.exists(realmSSLSettings);
        final boolean hostnameVerficationExists =
                realmSettings.get(HOSTNAME_VERIFICATION_SETTING, null) != null;

        if (verificationModeExists && hostnameVerficationExists) {
            throw new IllegalArgumentException("[" + HOSTNAME_VERIFICATION_SETTING + "] and [" +
                    sslConfigurationSettings.verificationMode.getKey() +
                    "] may not be used at the same time");
        } else if (verificationModeExists) {
            VerificationMode verificationMode = sslService.getVerificationMode(realmSSLSettings,
                    Settings.EMPTY);
            if (verificationMode == VerificationMode.FULL) {
                options.setSSLSocketVerifier(new HostNameSSLSocketVerifier(true));
            }
        } else if (hostnameVerficationExists) {
            new DeprecationLogger(logger).deprecated("the setting [{}] has been deprecated and " +
                            "will be removed in a future version. use [{}] instead",
                    RealmSettings.getFullSettingKey(config, HOSTNAME_VERIFICATION_SETTING),
                    RealmSettings.getFullSettingKey(config, "ssl." +
                            sslConfigurationSettings.verificationMode.getKey()));
            if (realmSettings.getAsBoolean(HOSTNAME_VERIFICATION_SETTING, true)) {
                options.setSSLSocketVerifier(new HostNameSSLSocketVerifier(true));
            }
        } else {
            options.setSSLSocketVerifier(new HostNameSSLSocketVerifier(true));
        }
        return options;
    }

    private LDAPServers ldapServers(Settings settings) {
        // Parse LDAP urls
        String[] ldapUrls = settings.getAsArray(URLS_SETTING, getDefaultLdapUrls(settings));
        if (ldapUrls == null || ldapUrls.length == 0) {
            throw new IllegalArgumentException("missing required LDAP setting [" + URLS_SETTING +
                    "]");
        }
        return new LDAPServers(ldapUrls);
    }

    protected String[] getDefaultLdapUrls(Settings settings) {
        return null;
    }

    private ServerSet serverSet(RealmConfig realmConfig, SSLService clientSSLService,
                                LDAPServers ldapServers) {
        Settings settings = realmConfig.settings();
        SocketFactory socketFactory = null;
        if (ldapServers.ssl()) {
            socketFactory = clientSSLService.sslSocketFactory(settings.getByPrefix("ssl."));
            if (settings.getAsBoolean(HOSTNAME_VERIFICATION_SETTING, true)) {
                logger.debug("using encryption for LDAP connections with hostname verification");
            } else {
                logger.debug("using encryption for LDAP connections without hostname verification");
            }
        }
        return LdapLoadBalancing.serverSet(ldapServers.addresses(), ldapServers.ports(), settings,
                socketFactory, connectionOptions(realmConfig, sslService, logger));
    }

    // package private to use for testing
    ServerSet getServerSet() {
        return serverSet;
    }

    public boolean isSslUsed() {
        return sslUsed;
    }

    protected static Set<Setting<?>> getSettings() {
        Set<Setting<?>> settings = new HashSet<>();
        settings.addAll(LdapLoadBalancing.getSettings());
        settings.add(Setting.listSetting(URLS_SETTING, Collections.emptyList(), Function.identity(),
                Setting.Property.NodeScope));
        settings.add(Setting.timeSetting(TIMEOUT_TCP_CONNECTION_SETTING, TIMEOUT_DEFAULT, Setting.Property.NodeScope));
        settings.add(Setting.timeSetting(TIMEOUT_TCP_READ_SETTING, TIMEOUT_DEFAULT, Setting.Property.NodeScope));
        settings.add(Setting.timeSetting(TIMEOUT_LDAP_SETTING, TIMEOUT_DEFAULT, Setting.Property.NodeScope));
        settings.add(Setting.boolSetting(HOSTNAME_VERIFICATION_SETTING, true, Setting.Property.NodeScope, Setting.Property.Filtered));
        settings.add(Setting.boolSetting(FOLLOW_REFERRALS_SETTING, true, Setting.Property.NodeScope));
        settings.add(IGNORE_REFERRAL_ERRORS_SETTING);
        settings.addAll(SSLConfigurationSettings.withPrefix("ssl.").getAllSettings());
        return settings;
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
                                +  Strings.arrayToCommaDelimitedString(ldapUrls) + "]");
            }

            return allSecure;
        }
    }
}

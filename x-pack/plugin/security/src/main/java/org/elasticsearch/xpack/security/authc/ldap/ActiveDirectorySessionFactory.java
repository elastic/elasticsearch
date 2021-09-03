/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import com.unboundid.ldap.sdk.Filter;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPConnectionPool;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.LDAPInterface;
import com.unboundid.ldap.sdk.SearchResultEntry;
import com.unboundid.ldap.sdk.ServerSet;
import com.unboundid.ldap.sdk.SimpleBindRequest;
import com.unboundid.ldap.sdk.controls.AuthorizationIdentityRequestControl;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.CharArrays;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.ActiveDirectorySessionFactorySettings;
import org.elasticsearch.xpack.core.security.authc.ldap.PoolingSessionFactorySettings;
import org.elasticsearch.xpack.core.security.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapMetadataResolver;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession.GroupsResolver;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils.attributesToSearchFor;
import static org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils.createFilter;
import static org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils.search;
import static org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils.searchForEntry;

/**
 * This Class creates LdapSessions authenticating via the custom Active Directory protocol.  (that being
 * authenticating with a principal name, "username@domain", then searching through the directory to find the
 * user entry in Active Directory that matches the user name).  This eliminates the need for user templates, and simplifies
 * the configuration for windows admins that may not be familiar with LDAP concepts.
 */
class ActiveDirectorySessionFactory extends PoolingSessionFactory {

    private static final String NETBIOS_NAME_FILTER_TEMPLATE = "(netbiosname={0})";

    final DefaultADAuthenticator defaultADAuthenticator;
    final DownLevelADAuthenticator downLevelADAuthenticator;
    final UpnADAuthenticator upnADAuthenticator;

    ActiveDirectorySessionFactory(RealmConfig config, SSLService sslService, ThreadPool threadPool) throws LDAPException {
        super(config, sslService, new ActiveDirectoryGroupsResolver(config),
                ActiveDirectorySessionFactorySettings.POOL_ENABLED,
                config.hasSetting(PoolingSessionFactorySettings.BIND_DN) ? getBindDN(config) : null,
                () -> {
                    if (config.hasSetting(PoolingSessionFactorySettings.BIND_DN)) {
                        final String healthCheckDn = config.getSetting(PoolingSessionFactorySettings.BIND_DN);
                        if (healthCheckDn.isEmpty() && healthCheckDn.indexOf('=') > 0) {
                            return healthCheckDn;
                        }
                    }
                    return config.getSetting(ActiveDirectorySessionFactorySettings.AD_USER_SEARCH_BASEDN_SETTING,
                        () -> config.getSetting(ActiveDirectorySessionFactorySettings.AD_DOMAIN_NAME_SETTING));
                }, threadPool);
        String domainName = config.getSetting(ActiveDirectorySessionFactorySettings.AD_DOMAIN_NAME_SETTING);
        String domainDN = buildDnFromDomain(domainName);
        final int ldapPort = config.getSetting(ActiveDirectorySessionFactorySettings.AD_LDAP_PORT_SETTING);
        final int ldapsPort = config.getSetting(ActiveDirectorySessionFactorySettings.AD_LDAPS_PORT_SETTING);
        final int gcLdapPort = config.getSetting(ActiveDirectorySessionFactorySettings.AD_GC_LDAP_PORT_SETTING);
        final int gcLdapsPort = config.getSetting(ActiveDirectorySessionFactorySettings.AD_GC_LDAPS_PORT_SETTING);

        defaultADAuthenticator = new DefaultADAuthenticator(config, timeout, ignoreReferralErrors, logger, groupResolver,
                metadataResolver, domainDN, threadPool);
        downLevelADAuthenticator = new DownLevelADAuthenticator(config, timeout, ignoreReferralErrors, logger, groupResolver,
                metadataResolver, domainDN, sslService, threadPool, ldapPort, ldapsPort, gcLdapPort, gcLdapsPort);
        upnADAuthenticator = new UpnADAuthenticator(config, timeout, ignoreReferralErrors, logger, groupResolver,
                metadataResolver, domainDN, threadPool);

    }

    @Override
    protected List<String> getDefaultLdapUrls(RealmConfig config) {
        return Collections.singletonList("ldap://" + config.getSetting(ActiveDirectorySessionFactorySettings.AD_DOMAIN_NAME_SETTING) +
                ":" + config.getSetting(ActiveDirectorySessionFactorySettings.AD_LDAP_PORT_SETTING));
    }

    @Override
    public boolean supportsUnauthenticatedSession() {
        // Strictly, we only support unauthenticated sessions if there is a bind_dn or a connection pool, but the
        // getUnauthenticatedSession... methods handle the situations correctly, so it's OK to always return true here.
        return true;
    }

    @Override
    void getSessionWithPool(LDAPConnectionPool connectionPool, String user, SecureString password, ActionListener<LdapSession> listener) {
        getADAuthenticator(user).authenticate(connectionPool, user, password, threadPool, listener);
    }

    @Override
    void getSessionWithoutPool(String username, SecureString password, ActionListener<LdapSession> listener) {
        try {
            final LDAPConnection connection = LdapUtils.privilegedConnect(serverSet::getConnection);
            getADAuthenticator(username).authenticate(connection, username, password, ActionListener.wrap(listener::onResponse, e -> {
                IOUtils.closeWhileHandlingException(connection);
                listener.onFailure(e);
            }));
        } catch (LDAPException e) {
            listener.onFailure(e);
        }
    }

    @Override
    void getUnauthenticatedSessionWithPool(LDAPConnectionPool connectionPool, String user, ActionListener<LdapSession> listener) {
        getADAuthenticator(user).searchForDN(connectionPool, user, null, Math.toIntExact(timeout.seconds()), ActionListener.wrap(entry -> {
            if (entry == null) {
                listener.onResponse(null);
            } else {
                final String dn = entry.getDN();
                listener.onResponse(new LdapSession(logger, config, connectionPool, dn, groupResolver, metadataResolver, timeout, null));
            }
        }, listener::onFailure));
    }

    @Override
    void getUnauthenticatedSessionWithoutPool(String user, ActionListener<LdapSession> listener) {
        if (config.hasSetting(PoolingSessionFactorySettings.BIND_DN) == false) {
            listener.onResponse(null);
            return;
        }
        try {
            final LDAPConnection connection = LdapUtils.privilegedConnect(serverSet::getConnection);
            LdapUtils.maybeForkThenBind(connection, bindCredentials, true, threadPool, new AbstractRunnable() {

                @Override
                public void onFailure(Exception e) {
                    IOUtils.closeWhileHandlingException(connection);
                    listener.onFailure(e);
                }

                @Override
                protected void doRun() throws Exception {
                    getADAuthenticator(user).searchForDN(connection, user, null, Math.toIntExact(timeout.getSeconds()),
                            ActionListener.wrap(entry -> {
                                if (entry == null) {
                                    IOUtils.close(connection);
                                    listener.onResponse(null);
                                } else {
                                    listener.onResponse(new LdapSession(logger, config, connection, entry.getDN(), groupResolver,
                                            metadataResolver, timeout, null));
                                }
                            }, e -> {
                                IOUtils.closeWhileHandlingException(connection);
                                listener.onFailure(e);
                            }));

                }
            });
        } catch (LDAPException e) {
            listener.onFailure(e);
        }
    }

    /**
     * @param domain active directory domain name
     * @return LDAP DN, distinguished name, of the root of the domain
     */
    static String buildDnFromDomain(String domain) {
        return "DC=" + domain.replace(".", ",DC=");
    }

    static String getBindDN(RealmConfig config) {
        String bindDN = config.getSetting(PoolingSessionFactorySettings.BIND_DN);
        if (bindDN.isEmpty() == false && bindDN.indexOf('\\') < 0 && bindDN.indexOf('@') < 0 && bindDN.indexOf('=') < 0) {
            bindDN = bindDN + "@" + config.getSetting(ActiveDirectorySessionFactorySettings.AD_DOMAIN_NAME_SETTING);
        }
        return bindDN;
    }

    // Exposed for testing
    ServerSet getServerSet() {
        return super.serverSet;
    }

    ADAuthenticator getADAuthenticator(String username) {
        if (username.indexOf('\\') > 0) {
            return downLevelADAuthenticator;
        } else if (username.indexOf("@") > 0) {
            return upnADAuthenticator;
        }
        return defaultADAuthenticator;
    }

    abstract static class ADAuthenticator {

        private final RealmConfig realm;
        final TimeValue timeout;
        final boolean ignoreReferralErrors;
        final Logger logger;
        final GroupsResolver groupsResolver;
        final LdapMetadataResolver metadataResolver;
        final String userSearchDN;
        final LdapSearchScope userSearchScope;
        final String userSearchFilter;
        final String bindDN;
        final SecureString bindPassword;
        final ThreadPool threadPool;

        ADAuthenticator(RealmConfig realm, TimeValue timeout, boolean ignoreReferralErrors, Logger logger, GroupsResolver groupsResolver,
                        LdapMetadataResolver metadataResolver, String domainDN, Setting.AffixSetting<String> userSearchFilterSetting,
                        String defaultUserSearchFilter, ThreadPool threadPool) {
            this.realm = realm;
            this.timeout = timeout;
            this.ignoreReferralErrors = ignoreReferralErrors;
            this.logger = logger;
            this.groupsResolver = groupsResolver;
            this.metadataResolver = metadataResolver;
            this.bindDN = getBindDN(realm);
            this.bindPassword = realm.getSetting(PoolingSessionFactorySettings.SECURE_BIND_PASSWORD,
                    () -> realm.getSetting(PoolingSessionFactorySettings.LEGACY_BIND_PASSWORD));
            this.threadPool = threadPool;
            userSearchDN = realm.getSetting(ActiveDirectorySessionFactorySettings.AD_USER_SEARCH_BASEDN_SETTING, () -> domainDN);
            userSearchScope = LdapSearchScope.resolve(realm.getSetting(ActiveDirectorySessionFactorySettings.AD_USER_SEARCH_SCOPE_SETTING),
                    LdapSearchScope.SUB_TREE);
            userSearchFilter = realm.getSetting(userSearchFilterSetting, () -> defaultUserSearchFilter);
        }

        final void authenticate(LDAPConnection connection, String username, SecureString password, ActionListener<LdapSession> listener) {
            final byte[] passwordBytes = CharArrays.toUtf8Bytes(password.getChars());
            final SimpleBindRequest userBind = new SimpleBindRequest(bindUsername(username), passwordBytes,
                    new AuthorizationIdentityRequestControl());
            LdapUtils.maybeForkThenBind(connection, userBind, false, threadPool, new ActionRunnable<LdapSession>(listener) {
                @Override
                protected void doRun() throws Exception {
                    final ActionRunnable<LdapSession> searchRunnable = new ActionRunnable<LdapSession>(listener) {
                        @Override
                        protected void doRun() throws Exception {
                            searchForDN(connection, username, password, Math.toIntExact(timeout.seconds()), ActionListener.wrap((entry) -> {
                                if (entry == null) {
                                    // we did not find the user, cannot authenticate in this realm
                                    listener.onFailure(new ElasticsearchSecurityException(
                                            "search for user [" + username + "] by principal name yielded no results"));
                                } else {
                                    listener.onResponse(new LdapSession(logger, realm, connection, entry.getDN(), groupsResolver,
                                            metadataResolver, timeout, null));
                                }
                            }, e -> {
                                listener.onFailure(e);
                            }));
                        }
                    };
                    if (bindDN.isEmpty()) {
                        searchRunnable.run();
                    } else {
                        final SimpleBindRequest bind = new SimpleBindRequest(bindDN, CharArrays.toUtf8Bytes(bindPassword.getChars()));
                        LdapUtils.maybeForkThenBind(connection, bind, true, threadPool, searchRunnable);
                    }
                }
            });
        }

        final void authenticate(LDAPConnectionPool pool, String username, SecureString password, ThreadPool threadPool,
                                ActionListener<LdapSession> listener) {
            final byte[] passwordBytes = CharArrays.toUtf8Bytes(password.getChars());
            final SimpleBindRequest bind = new SimpleBindRequest(bindUsername(username), passwordBytes);
            LdapUtils.maybeForkThenBindAndRevert(pool, bind, threadPool, new ActionRunnable<LdapSession>(listener) {
                @Override
                protected void doRun() throws Exception {
                    searchForDN(pool, username, password, Math.toIntExact(timeout.seconds()), ActionListener.wrap((entry) -> {
                        if (entry == null) {
                            // we did not find the user, cannot authenticate in this realm
                            listener.onFailure(new ElasticsearchSecurityException(
                                    "search for user [" + username + "] by principal name yielded no results"));
                        } else {
                            listener.onResponse(
                                    new LdapSession(logger, realm, pool, entry.getDN(), groupsResolver, metadataResolver, timeout, null));
                        }
                    }, e -> {
                        listener.onFailure(e);
                    }));
                }
            });
        }

        String bindUsername(String username) {
            return username;
        }

        // pkg-private for testing
        final String getUserSearchFilter() {
            return userSearchFilter;
        }

        abstract void searchForDN(LDAPInterface connection, String username, SecureString password, int timeLimitSeconds,
                                  ActionListener<SearchResultEntry> listener);
    }

    /**
     * This authenticator is used for usernames that do not contain an `@` or `/`. It attempts a bind with the provided username combined
     * with the domain name specified in settings. On AD DS this will work for both upn@domain and samaccountname@domain; AD LDS will only
     * support the upn format
     */
    static class DefaultADAuthenticator extends ADAuthenticator {

        final String domainName;

        DefaultADAuthenticator(RealmConfig realm, TimeValue timeout, boolean ignoreReferralErrors, Logger logger,
                               GroupsResolver groupsResolver, LdapMetadataResolver metadataResolver, String domainDN,
                               ThreadPool threadPool) {
            super(realm, timeout, ignoreReferralErrors, logger, groupsResolver, metadataResolver, domainDN,
                    ActiveDirectorySessionFactorySettings.AD_USER_SEARCH_FILTER_SETTING,
                    "(&(objectClass=user)(|(sAMAccountName={0})(userPrincipalName={0}@" + domainName(realm) + ")))", threadPool);
            domainName = domainName(realm);
        }

        private static String domainName(RealmConfig realm) {
            return realm.getSetting(ActiveDirectorySessionFactorySettings.AD_DOMAIN_NAME_SETTING);
        }

        @Override
        void searchForDN(LDAPInterface connection, String username, SecureString password,
                         int timeLimitSeconds, ActionListener<SearchResultEntry> listener) {
            try {
                searchForEntry(connection, userSearchDN, userSearchScope.scope(),
                        createFilter(userSearchFilter, username), timeLimitSeconds,
                        ignoreReferralErrors, listener,
                        attributesToSearchFor(groupsResolver.attributes()));
            } catch (LDAPException e) {
                listener.onFailure(e);
            }
        }

        @Override
        String bindUsername(String username) {
            return username + "@" + domainName;
        }
    }

    /**
     * Active Directory calls the format <code>DOMAIN\\username</code> down-level credentials and
     * this class contains the logic necessary to authenticate this form of a username
     */
    static class DownLevelADAuthenticator extends ADAuthenticator {
        static final String DOWN_LEVEL_FILTER = "(&(objectClass=user)(sAMAccountName={0}))";
        Cache<String, String> domainNameCache = CacheBuilder.<String, String>builder().setMaximumWeight(100).build();

        final String domainDN;
        final SSLService sslService;
        final RealmConfig config;
        private final int ldapPort;
        private final int ldapsPort;
        private final int gcLdapPort;
        private final int gcLdapsPort;

        DownLevelADAuthenticator(RealmConfig config, TimeValue timeout, boolean ignoreReferralErrors, Logger logger,
                                 GroupsResolver groupsResolver, LdapMetadataResolver metadataResolver, String domainDN,
                                 SSLService sslService, ThreadPool threadPool,
                                 int ldapPort, int ldapsPort, int gcLdapPort, int gcLdapsPort) {
            super(config, timeout, ignoreReferralErrors, logger, groupsResolver, metadataResolver, domainDN,
                    ActiveDirectorySessionFactorySettings.AD_DOWN_LEVEL_USER_SEARCH_FILTER_SETTING, DOWN_LEVEL_FILTER, threadPool);
            this.domainDN = domainDN;
            this.sslService = sslService;
            this.config = config;
            this.ldapPort = ldapPort;
            this.ldapsPort = ldapsPort;
            this.gcLdapPort = gcLdapPort;
            this.gcLdapsPort = gcLdapsPort;
        }

        @Override
        void searchForDN(LDAPInterface connection, String username, SecureString password, int timeLimitSeconds,
                         ActionListener<SearchResultEntry> listener) {
            String[] parts = username.split("\\\\");
            assert parts.length == 2;
            final String netBiosDomainName = parts[0];
            final String accountName = parts[1];

            netBiosDomainNameToDn(connection, netBiosDomainName, username, password, timeLimitSeconds, ActionListener.wrap((domainDN) -> {
                if (domainDN == null) {
                    listener.onResponse(null);
                } else {
                    searchForEntry(connection, domainDN, LdapSearchScope.SUB_TREE.scope(), createFilter(userSearchFilter, accountName),
                            timeLimitSeconds, ignoreReferralErrors, listener, attributesToSearchFor(groupsResolver.attributes()));
                }
            }, listener::onFailure));
        }

        void netBiosDomainNameToDn(LDAPInterface ldapInterface, String netBiosDomainName, String username, SecureString password,
                                   int timeLimitSeconds, ActionListener<String> listener) {
            LDAPConnection ldapConnection = null;
            try {
                final Filter filter = createFilter(NETBIOS_NAME_FILTER_TEMPLATE, netBiosDomainName);
                final String cachedName = domainNameCache.get(netBiosDomainName);
                if (cachedName != null) {
                    listener.onResponse(cachedName);
                } else if (usingGlobalCatalog(ldapInterface) == false) {
                    search(ldapInterface, "CN=Configuration," + domainDN, LdapSearchScope.SUB_TREE.scope(), filter, timeLimitSeconds,
                            ignoreReferralErrors,
                            ActionListener.wrap((results) -> handleSearchResults(results, netBiosDomainName, domainNameCache, listener),
                                    listener::onFailure),
                            "ncname");
                } else {
                    // the global catalog does not replicate the necessary information to map a
                    // netbios dns name to a DN so we need to instead connect to the normal ports.
                    // This code uses the standard ports to avoid adding even more settings and is
                    // probably ok as most AD users do not use non-standard ports
                    if (ldapInterface instanceof LDAPConnection) {
                        ldapConnection = (LDAPConnection) ldapInterface;
                    } else {
                        ldapConnection = LdapUtils.privilegedConnect(((LDAPConnectionPool) ldapInterface)::getConnection);
                    }
                    final LDAPConnection finalLdapConnection = ldapConnection;
                    final LDAPConnection searchConnection = LdapUtils.privilegedConnect(
                            () -> new LDAPConnection(finalLdapConnection.getSocketFactory(), connectionOptions(config, sslService, logger),
                                    finalLdapConnection.getConnectedAddress(),
                                    finalLdapConnection.getSSLSession() != null ? ldapsPort : ldapPort));
                    final byte[] passwordBytes = CharArrays.toUtf8Bytes(password.getChars());
                    final boolean bindAsAuthenticatingUser = this.bindDN.isEmpty();
                    final SimpleBindRequest bind = bindAsAuthenticatingUser
                            ? new SimpleBindRequest(username, passwordBytes)
                            : new SimpleBindRequest(bindDN, CharArrays.toUtf8Bytes(bindPassword.getChars()));
                    ActionRunnable<String> body = new ActionRunnable<>(listener) {
                        @Override
                        protected void doRun() throws Exception {
                            search(
                                searchConnection,
                                "CN=Configuration," + domainDN,
                                LdapSearchScope.SUB_TREE.scope(),
                                filter,
                                timeLimitSeconds,
                                ignoreReferralErrors,
                                ActionListener.wrap(results -> {
                                    IOUtils.close(searchConnection);
                                    handleSearchResults(results, netBiosDomainName, domainNameCache, listener);
                                }, e -> {
                                    IOUtils.closeWhileHandlingException(searchConnection);
                                    listener.onFailure(e);
                                }),
                                "ncname"
                            );
                        }

                        @Override
                        public void onFailure(Exception e) {
                            IOUtils.closeWhileHandlingException(searchConnection);
                            listener.onFailure(e);
                        }
                    };
                    LdapUtils.maybeForkThenBind(searchConnection, bind, bindAsAuthenticatingUser == false, threadPool, body);
                }
            } catch (LDAPException e) {
                listener.onFailure(e);
            } finally {
                if (ldapInterface instanceof LDAPConnectionPool && ldapConnection != null) {
                    ((LDAPConnectionPool) ldapInterface).releaseConnection(ldapConnection);
                }
            }
        }

        static void handleSearchResults(List<SearchResultEntry> results, String netBiosDomainName,
                                        Cache<String, String> domainNameCache,
                                        ActionListener<String> listener) {
            Optional<SearchResultEntry> entry = results.stream()
                    .filter((r) -> r.hasAttribute("ncname"))
                    .findFirst();
            if (entry.isPresent()) {
                final String value = entry.get().getAttributeValue("ncname");
                try {
                    domainNameCache.computeIfAbsent(netBiosDomainName, (s) -> value);
                } catch (ExecutionException e) {
                    throw new AssertionError("failed to load constant non-null value", e);
                }
                listener.onResponse(value);
            } else {
                listener.onResponse(null);
            }
        }

        boolean usingGlobalCatalog(LDAPInterface ldap) throws LDAPException {
            if (ldap instanceof LDAPConnection) {
                return usingGlobalCatalog((LDAPConnection) ldap);
            } else {
                LDAPConnectionPool pool = (LDAPConnectionPool) ldap;
                LDAPConnection connection = null;
                try {
                    connection = LdapUtils.privilegedConnect(pool::getConnection);
                    return usingGlobalCatalog(connection);
                } finally {
                    if (connection != null) {
                        pool.releaseConnection(connection);
                    }
                }
            }
        }

        private boolean usingGlobalCatalog(LDAPConnection ldapConnection) {
            return ldapConnection.getConnectedPort() == gcLdapPort || ldapConnection.getConnectedPort() == gcLdapsPort;
        }
    }

    /**
     * Authenticates user principal names provided by the user (eq user@domain). Note this authenticator does not currently support
     * UPN suffixes that are different than the actual domain name.
     */
    static class UpnADAuthenticator extends ADAuthenticator {
        static final String UPN_USER_FILTER = "(&(objectClass=user)(userPrincipalName={1}))";
        private final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(logger.getName());

        UpnADAuthenticator(RealmConfig config, TimeValue timeout, boolean ignoreReferralErrors, Logger logger,
                           GroupsResolver groupsResolver, LdapMetadataResolver metadataResolver, String domainDN, ThreadPool threadPool) {
            super(config, timeout, ignoreReferralErrors, logger, groupsResolver, metadataResolver, domainDN,
                    ActiveDirectorySessionFactorySettings.AD_UPN_USER_SEARCH_FILTER_SETTING, UPN_USER_FILTER, threadPool);
            if (userSearchFilter.contains("{0}")) {
                deprecationLogger.deprecate(DeprecationCategory.SECURITY, "ldap_settings",
                    "The use of the account name variable {0} in the setting ["
                    + RealmSettings.getFullSettingKey(config, ActiveDirectorySessionFactorySettings.AD_UPN_USER_SEARCH_FILTER_SETTING)
                    + "] has been deprecated and will be removed in a future version!");
            }
        }

        @Override
        void searchForDN(LDAPInterface connection, String username, SecureString password, int timeLimitSeconds,
                         ActionListener<SearchResultEntry> listener) {
            String[] parts = username.split("@");
            assert parts.length == 2 : "there should have only been two values for " + username + " after splitting on '@'";
            final String accountName = parts[0];
            try {
                Filter filter = createFilter(userSearchFilter, accountName, username);
                searchForEntry(connection, userSearchDN, LdapSearchScope.SUB_TREE.scope(), filter,
                        timeLimitSeconds, ignoreReferralErrors, listener,
                        attributesToSearchFor(groupsResolver.attributes()));
            } catch (LDAPException e) {
                listener.onFailure(e);
            }
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import com.unboundid.ldap.sdk.Filter;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPConnectionOptions;
import com.unboundid.ldap.sdk.LDAPConnectionPool;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.LDAPInterface;
import com.unboundid.ldap.sdk.SearchResultEntry;
import com.unboundid.ldap.sdk.SimpleBindRequest;
import com.unboundid.ldap.sdk.controls.AuthorizationIdentityRequestControl;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.authc.RealmSettings;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapMetaDataResolver;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession.GroupsResolver;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils;
import org.elasticsearch.xpack.security.authc.ldap.support.SessionFactory;
import org.elasticsearch.xpack.security.authc.support.CharArrays;
import org.elasticsearch.xpack.ssl.SSLService;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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

    static final String AD_DOMAIN_NAME_SETTING = "domain_name";

    static final String AD_GROUP_SEARCH_BASEDN_SETTING = "group_search.base_dn";
    static final String AD_GROUP_SEARCH_SCOPE_SETTING = "group_search.scope";
    static final String AD_USER_SEARCH_BASEDN_SETTING = "user_search.base_dn";
    static final String AD_USER_SEARCH_FILTER_SETTING = "user_search.filter";
    static final String AD_UPN_USER_SEARCH_FILTER_SETTING = "user_search.upn_filter";
    static final String AD_DOWN_LEVEL_USER_SEARCH_FILTER_SETTING = "user_search.down_level_filter";
    static final String AD_USER_SEARCH_SCOPE_SETTING = "user_search.scope";
    private static final String NETBIOS_NAME_FILTER_TEMPLATE = "(netbiosname={0})";
    private static final Setting<Boolean> POOL_ENABLED = Setting.boolSetting("user_search.pool.enabled",
            settings -> Boolean.toString(PoolingSessionFactory.BIND_DN.exists(settings)), Setting.Property.NodeScope);

    final DefaultADAuthenticator defaultADAuthenticator;
    final DownLevelADAuthenticator downLevelADAuthenticator;
    final UpnADAuthenticator upnADAuthenticator;

    ActiveDirectorySessionFactory(RealmConfig config, SSLService sslService) throws LDAPException {
        super(config, sslService, new ActiveDirectoryGroupsResolver(config.settings()), POOL_ENABLED, () -> {
            if (BIND_DN.exists(config.settings())) {
                return new SimpleBindRequest(getBindDN(config.settings()), BIND_PASSWORD.get(config.settings()));
            } else {
                return new SimpleBindRequest();
            }
        }, () -> {
            if (BIND_DN.exists(config.settings())) {
                final String healthCheckDn = BIND_DN.get(config.settings());
                if (healthCheckDn.isEmpty() && healthCheckDn.indexOf('=') > 0) {
                    return healthCheckDn;
                }
            }
            return config.settings().get(AD_USER_SEARCH_BASEDN_SETTING, config.settings().get(AD_DOMAIN_NAME_SETTING));
        });
        Settings settings = config.settings();
        String domainName = settings.get(AD_DOMAIN_NAME_SETTING);
        if (domainName == null) {
            throw new IllegalArgumentException("missing [" + AD_DOMAIN_NAME_SETTING + "] setting for active directory");
        }
        String domainDN = buildDnFromDomain(domainName);
        defaultADAuthenticator = new DefaultADAuthenticator(config, timeout, ignoreReferralErrors, logger, groupResolver,
                metaDataResolver, domainDN);
        downLevelADAuthenticator = new DownLevelADAuthenticator(config, timeout, ignoreReferralErrors, logger, groupResolver, 
                metaDataResolver, domainDN, sslService);
        upnADAuthenticator = new UpnADAuthenticator(config, timeout, ignoreReferralErrors, logger, groupResolver, 
                metaDataResolver, domainDN);

    }

    @Override
    protected String[] getDefaultLdapUrls(Settings settings) {
        return new String[] {"ldap://" + settings.get(AD_DOMAIN_NAME_SETTING) + ":389"};
    }

    @Override
    void getSessionWithPool(LDAPConnectionPool connectionPool, String user, SecureString password, ActionListener<LdapSession> listener) {
        getADAuthenticator(user).authenticate(connectionPool, user, password, listener);
    }

    @Override
    void getSessionWithoutPool(String username, SecureString password, ActionListener<LdapSession> listener) {
        // the runnable action here allows us make the control/flow logic simpler to understand. If we got a connection then lets
        // authenticate. If there was a failure pass it back using the listener
        Runnable runnable;
        try {
            final LDAPConnection connection = LdapUtils.privilegedConnect(serverSet::getConnection);
            runnable = () -> getADAuthenticator(username).authenticate(connection, username, password,
                    ActionListener.wrap(listener::onResponse,
                            (e) -> {
                                IOUtils.closeWhileHandlingException(connection);
                                listener.onFailure(e);
                            }));
        } catch (LDAPException e) {
            runnable = () -> listener.onFailure(e);
        }
        runnable.run();
    }

    @Override
    void getUnauthenticatedSessionWithPool(LDAPConnectionPool connectionPool, String user, ActionListener<LdapSession> listener) {
        getADAuthenticator(user).searchForDN(connectionPool, user, null, Math.toIntExact(timeout.seconds()), ActionListener.wrap(entry -> {
            if (entry == null) {
                listener.onResponse(null);
            } else {
                final String dn = entry.getDN();
                listener.onResponse(new LdapSession(logger, config, connectionPool, dn, groupResolver, metaDataResolver, timeout, null));
            }
        }, listener::onFailure));
    }

    @Override
    void getUnauthenticatedSessionWithoutPool(String user, ActionListener<LdapSession> listener) {
        if (BIND_DN.exists(config.settings())) {
            LDAPConnection connection = null;
            boolean startedSearching = false;
            try {
                connection = LdapUtils.privilegedConnect(serverSet::getConnection);
                connection.bind(new SimpleBindRequest(getBindDN(config.settings()), BIND_PASSWORD.get(config.settings())));
                final LDAPConnection finalConnection = connection;
                getADAuthenticator(user).searchForDN(finalConnection, user, null, Math.toIntExact(timeout.getSeconds()),
                        ActionListener.wrap(entry -> {
                            if (entry == null) {
                                IOUtils.closeWhileHandlingException(finalConnection);
                                listener.onResponse(null);
                            } else {
                                final String dn = entry.getDN();
                                listener.onResponse(new LdapSession(logger, config, finalConnection, dn, groupResolver, metaDataResolver,
                                        timeout, null));
                            }
                        }, e -> {
                            IOUtils.closeWhileHandlingException(finalConnection);
                            listener.onFailure(e);
                        }));
                startedSearching = true;
            } catch (LDAPException e) {
                listener.onFailure(e);
            } finally {
                if (connection != null && startedSearching == false) {
                    IOUtils.closeWhileHandlingException(connection);
                }
            }
        } else {
            listener.onResponse(null);
        }
    }

    /**
     * @param domain active directory domain name
     * @return LDAP DN, distinguished name, of the root of the domain
     */
    static String buildDnFromDomain(String domain) {
        return "DC=" + domain.replace(".", ",DC=");
    }

    static String getBindDN(Settings settings) {
        String bindDN = BIND_DN.get(settings);
        if (bindDN.isEmpty() == false && bindDN.indexOf('\\') < 0 && bindDN.indexOf('@') < 0 && bindDN.indexOf('=') < 0) {
            bindDN = bindDN + "@" + settings.get(AD_DOMAIN_NAME_SETTING);
        }
        return bindDN;
    }

    public static Set<Setting<?>> getSettings() {
        Set<Setting<?>> settings = new HashSet<>();
        settings.addAll(SessionFactory.getSettings());
        settings.add(Setting.simpleString(AD_DOMAIN_NAME_SETTING, Setting.Property.NodeScope));
        settings.add(Setting.simpleString(AD_GROUP_SEARCH_BASEDN_SETTING, Setting.Property.NodeScope));
        settings.add(Setting.simpleString(AD_GROUP_SEARCH_SCOPE_SETTING, Setting.Property.NodeScope));
        settings.add(Setting.simpleString(AD_USER_SEARCH_BASEDN_SETTING, Setting.Property.NodeScope));
        settings.add(Setting.simpleString(AD_USER_SEARCH_FILTER_SETTING, Setting.Property.NodeScope));
        settings.add(Setting.simpleString(AD_UPN_USER_SEARCH_FILTER_SETTING, Setting.Property.NodeScope));
        settings.add(Setting.simpleString(AD_DOWN_LEVEL_USER_SEARCH_FILTER_SETTING, Setting.Property.NodeScope));
        settings.add(Setting.simpleString(AD_USER_SEARCH_SCOPE_SETTING, Setting.Property.NodeScope));
        settings.addAll(PoolingSessionFactory.getSettings());
        return settings;
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
        final LdapMetaDataResolver metaDataResolver;
        final String userSearchDN;
        final LdapSearchScope userSearchScope;
        final String userSearchFilter;
        final String bindDN;
        final String bindPassword; // TODO this needs to be a setting in the secure settings store!

        ADAuthenticator(RealmConfig realm, TimeValue timeout, boolean ignoreReferralErrors, Logger logger,
                        GroupsResolver groupsResolver, LdapMetaDataResolver metaDataResolver, String domainDN,
                        String userSearchFilterSetting, String defaultUserSearchFilter) {
            this.realm = realm;
            this.timeout = timeout;
            this.ignoreReferralErrors = ignoreReferralErrors;
            this.logger = logger;
            this.groupsResolver = groupsResolver;
            this.metaDataResolver = metaDataResolver;
            final Settings settings = realm.settings();
            this.bindDN = getBindDN(settings);
            this.bindPassword = BIND_PASSWORD.get(settings);
            userSearchDN = settings.get(AD_USER_SEARCH_BASEDN_SETTING, domainDN);
            userSearchScope = LdapSearchScope.resolve(settings.get(AD_USER_SEARCH_SCOPE_SETTING), LdapSearchScope.SUB_TREE);
            userSearchFilter = settings.get(userSearchFilterSetting, defaultUserSearchFilter);
        }

        final void authenticate(LDAPConnection connection, String username, SecureString password,
                          ActionListener<LdapSession> listener) {
            boolean success = false;
            try {
                connection.bind(new SimpleBindRequest(bindUsername(username), CharArrays.toUtf8Bytes(password.getChars()),
                        new AuthorizationIdentityRequestControl()));
                if (bindDN.isEmpty() == false) {
                    connection.bind(new SimpleBindRequest(bindDN, bindPassword));
                }
                searchForDN(connection, username, password, Math.toIntExact(timeout.seconds()), ActionListener.wrap((entry) -> {
                    if (entry == null) {
                        IOUtils.close(connection);
                        // we did not find the user, cannot authenticate in this realm
                        listener.onFailure(new ElasticsearchSecurityException("search for user [" + username
                                + "] by principle name yielded no results"));
                    } else {
                        final String dn = entry.getDN();
                        listener.onResponse(new LdapSession(logger, realm, connection, dn, groupsResolver, metaDataResolver,
                                timeout, null));
                    }
                }, (e) -> {
                    IOUtils.closeWhileHandlingException(connection);
                    listener.onFailure(e);
                }));
                success = true;
            } catch (LDAPException e) {
                listener.onFailure(e);
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(connection);
                }
            }
        }

        final void authenticate(LDAPConnectionPool pool, String username, SecureString password,
                                ActionListener<LdapSession> listener) {
            try {
                LdapUtils.privilegedConnect(() -> {
                    SimpleBindRequest request = new SimpleBindRequest(bindUsername(username), CharArrays.toUtf8Bytes(password.getChars()));
                    return pool.bindAndRevertAuthentication(request);
                });
                searchForDN(pool, username, password, Math.toIntExact(timeout.seconds()), ActionListener.wrap((entry) -> {
                    if (entry == null) {
                        // we did not find the user, cannot authenticate in this realm
                        listener.onFailure(new ElasticsearchSecurityException("search for user [" + username
                                + "] by principle name yielded no results"));
                    } else {
                        final String dn = entry.getDN();
                        listener.onResponse(new LdapSession(logger, realm, pool, dn, groupsResolver, metaDataResolver, timeout, null));
                    }
                }, listener::onFailure));
            } catch (LDAPException e) {
                listener.onFailure(e);
            }
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
        DefaultADAuthenticator(RealmConfig realm, TimeValue timeout, boolean ignoreReferralErrors,
                               Logger logger, GroupsResolver groupsResolver, LdapMetaDataResolver metaDataResolver, String domainDN) {
            super(realm, timeout, ignoreReferralErrors, logger, groupsResolver, metaDataResolver, domainDN, AD_USER_SEARCH_FILTER_SETTING,
                    "(&(objectClass=user)(|(sAMAccountName={0})(userPrincipalName={0}@" + domainName(realm) + ")))");
            domainName = domainName(realm);
        }

        private static String domainName(RealmConfig realm) {
            return realm.settings().get(AD_DOMAIN_NAME_SETTING);
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
        final Settings settings;
        final SSLService sslService;
        final RealmConfig config;

        DownLevelADAuthenticator(RealmConfig config, TimeValue timeout, boolean ignoreReferralErrors, Logger logger,
                                 GroupsResolver groupsResolver, LdapMetaDataResolver metaDataResolver, String domainDN,
                                 SSLService sslService) {
            super(config, timeout, ignoreReferralErrors, logger, groupsResolver, metaDataResolver, domainDN,
                    AD_DOWN_LEVEL_USER_SEARCH_FILTER_SETTING, DOWN_LEVEL_FILTER);
            this.domainDN = domainDN;
            this.settings = config.settings();
            this.sslService = sslService;
            this.config = config;
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
                    try {
                        searchForEntry(connection, domainDN, LdapSearchScope.SUB_TREE.scope(),
                                createFilter(userSearchFilter,
                                        accountName), timeLimitSeconds, ignoreReferralErrors,
                                listener, attributesToSearchFor(groupsResolver.attributes()));
                    } catch (LDAPException e) {
                        listener.onFailure(e);
                    }
                }
            }, listener::onFailure));
        }

        void netBiosDomainNameToDn(LDAPInterface ldapInterface, String netBiosDomainName, String username, SecureString password,
                                   int timeLimitSeconds, ActionListener<String> listener) {
            final String cachedName = domainNameCache.get(netBiosDomainName);
            try {
                if (cachedName != null) {
                    listener.onResponse(cachedName);
                } else if (usingGlobalCatalog(ldapInterface)) {
                    // the global catalog does not replicate the necessary information to map a netbios
                    // dns name to a DN so we need to instead connect to the normal ports. This code
                    // uses the standard ports to avoid adding even more settings and is probably ok as
                    // most AD users do not use non-standard ports
                    final LDAPConnectionOptions options = connectionOptions(config, sslService, logger);
                    boolean startedSearching = false;
                    LDAPConnection searchConnection = null;
                    LDAPConnection ldapConnection = null;
                    try {
                        Filter filter = createFilter(NETBIOS_NAME_FILTER_TEMPLATE, netBiosDomainName);
                        if (ldapInterface instanceof LDAPConnection) {
                            ldapConnection = (LDAPConnection) ldapInterface;
                        } else {
                            ldapConnection = LdapUtils.privilegedConnect(((LDAPConnectionPool) ldapInterface)::getConnection);
                        }
                        final LDAPConnection finalLdapConnection = ldapConnection;
                        searchConnection = LdapUtils.privilegedConnect(
                                () -> new LDAPConnection(finalLdapConnection.getSocketFactory(), options,
                                        finalLdapConnection.getConnectedAddress(),
                                        finalLdapConnection.getSSLSession() != null ? 636 : 389));

                        final SimpleBindRequest bindRequest =
                                bindDN.isEmpty() ? new SimpleBindRequest(username, CharArrays.toUtf8Bytes(password.getChars())) :
                                        new SimpleBindRequest(bindDN, bindPassword);
                        searchConnection.bind(bindRequest);
                        final LDAPConnection finalConnection = searchConnection;
                        search(finalConnection, domainDN, LdapSearchScope.SUB_TREE.scope(), filter,
                                timeLimitSeconds, ignoreReferralErrors, ActionListener.wrap(
                                        (results) -> {
                                            IOUtils.close(finalConnection);
                                            handleSearchResults(results, netBiosDomainName, domainNameCache, listener);
                                        }, (e) -> {
                                            IOUtils.closeWhileHandlingException(finalConnection);
                                            listener.onFailure(e);
                                        }),
                                "ncname");
                        startedSearching = true;
                    } finally {
                        if (startedSearching == false) {
                            IOUtils.closeWhileHandlingException(searchConnection);
                        }
                        if (ldapInterface instanceof LDAPConnectionPool && ldapConnection != null) {
                            ((LDAPConnectionPool) ldapInterface).releaseConnection(ldapConnection);
                        }
                    }
                } else {
                    Filter filter = createFilter(NETBIOS_NAME_FILTER_TEMPLATE, netBiosDomainName);
                    search(ldapInterface, domainDN, LdapSearchScope.SUB_TREE.scope(), filter,
                            timeLimitSeconds, ignoreReferralErrors, ActionListener.wrap(
                                    (results) -> handleSearchResults(results, netBiosDomainName,
                                            domainNameCache, listener),
                                    listener::onFailure),
                            "ncname");
                }
            } catch (LDAPException e) {
                listener.onFailure(e);
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

        static boolean usingGlobalCatalog(LDAPInterface ldap) throws LDAPException {
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

        private static boolean usingGlobalCatalog(LDAPConnection ldapConnection) {
            return ldapConnection.getConnectedPort() == 3268 || ldapConnection.getConnectedPort() == 3269;
        }
    }

    /**
     * Authenticates user principal names provided by the user (eq user@domain). Note this authenticator does not currently support
     * UPN suffixes that are different than the actual domain name.
     */
    static class UpnADAuthenticator extends ADAuthenticator {

        static final String UPN_USER_FILTER = "(&(objectClass=user)(userPrincipalName={1}))";

        UpnADAuthenticator(RealmConfig config, TimeValue timeout, boolean ignoreReferralErrors, Logger logger,
                           GroupsResolver groupsResolver, LdapMetaDataResolver metaDataResolver, String domainDN) {
            super(config, timeout, ignoreReferralErrors, logger, groupsResolver, metaDataResolver, domainDN,
                    AD_UPN_USER_SEARCH_FILTER_SETTING, UPN_USER_FILTER);
            if (userSearchFilter.contains("{0}")) {
                new DeprecationLogger(logger).deprecated("The use of the account name variable {0} in the setting ["
                        + RealmSettings.getFullSettingKey(config, AD_UPN_USER_SEARCH_FILTER_SETTING) +
                        "] has been deprecated and will be removed in a future version!");
            }
        }

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

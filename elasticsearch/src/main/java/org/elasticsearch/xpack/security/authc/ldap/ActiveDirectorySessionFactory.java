/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import com.unboundid.ldap.sdk.Filter;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPConnectionOptions;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.SearchResultEntry;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession.GroupsResolver;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils;
import org.elasticsearch.xpack.security.authc.ldap.support.SessionFactory;
import org.elasticsearch.xpack.security.authc.support.SecuredString;
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
class ActiveDirectorySessionFactory extends SessionFactory {

    static final String AD_DOMAIN_NAME_SETTING = "domain_name";

    static final String AD_GROUP_SEARCH_BASEDN_SETTING = "group_search.base_dn";
    static final String AD_GROUP_SEARCH_SCOPE_SETTING = "group_search.scope";
    static final String AD_USER_SEARCH_BASEDN_SETTING = "user_search.base_dn";
    static final String AD_USER_SEARCH_FILTER_SETTING = "user_search.filter";
    static final String AD_USER_SEARCH_SCOPE_SETTING = "user_search.scope";
    private static final String NETBIOS_NAME_FILTER_TEMPLATE = "(netbiosname={0})";

    private final DefaultADAuthenticator defaultADAuthenticator;
    private final DownLevelADAuthenticator downLevelADAuthenticator;
    private final UpnADAuthenticator upnADAuthenticator;

    ActiveDirectorySessionFactory(RealmConfig config, SSLService sslService) {
        super(config, sslService);
        Settings settings = config.settings();
        String domainName = settings.get(AD_DOMAIN_NAME_SETTING);
        if (domainName == null) {
            throw new IllegalArgumentException("missing [" + AD_DOMAIN_NAME_SETTING + "] setting for active directory");
        }
        String domainDN = buildDnFromDomain(domainName);
        GroupsResolver groupResolver = new ActiveDirectoryGroupsResolver(settings.getAsSettings("group_search"), domainDN);
        defaultADAuthenticator = new DefaultADAuthenticator(settings, timeout, logger, groupResolver, domainDN);
        downLevelADAuthenticator = new DownLevelADAuthenticator(config, timeout, logger, groupResolver, domainDN, sslService);
        upnADAuthenticator = new UpnADAuthenticator(settings, timeout, logger, groupResolver, domainDN);
    }

    @Override
    protected String[] getDefaultLdapUrls(Settings settings) {
        return new String[] {"ldap://" + settings.get(AD_DOMAIN_NAME_SETTING) + ":389"};
    }

    /**
     * This is an active directory bind that looks up the user DN after binding with a windows principal.
     *
     * @param username name of the windows user without the domain
     */
    @Override
    public void session(String username, SecuredString password, ActionListener<LdapSession> listener) {
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

    /**
     * @param domain active directory domain name
     * @return LDAP DN, distinguished name, of the root of the domain
     */
    static String buildDnFromDomain(String domain) {
        return "DC=" + domain.replace(".", ",DC=");
    }

    public static Set<Setting<?>> getSettings() {
        Set<Setting<?>> settings = new HashSet<>();
        settings.addAll(SessionFactory.getSettings());
        settings.add(Setting.simpleString(AD_DOMAIN_NAME_SETTING, Setting.Property.NodeScope));
        settings.add(Setting.simpleString(AD_GROUP_SEARCH_BASEDN_SETTING, Setting.Property.NodeScope));
        settings.add(Setting.simpleString(AD_GROUP_SEARCH_SCOPE_SETTING, Setting.Property.NodeScope));
        settings.add(Setting.simpleString(AD_USER_SEARCH_BASEDN_SETTING, Setting.Property.NodeScope));
        settings.add(Setting.simpleString(AD_USER_SEARCH_FILTER_SETTING, Setting.Property.NodeScope));
        settings.add(Setting.simpleString(AD_USER_SEARCH_SCOPE_SETTING, Setting.Property.NodeScope));
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

        final TimeValue timeout;
        final Logger logger;
        final GroupsResolver groupsResolver;
        final String userSearchDN;
        final LdapSearchScope userSearchScope;

        ADAuthenticator(Settings settings, TimeValue timeout, Logger logger, GroupsResolver groupsResolver, String domainDN) {
            this.timeout = timeout;
            this.logger = logger;
            this.groupsResolver = groupsResolver;
            userSearchDN = settings.get(AD_USER_SEARCH_BASEDN_SETTING, domainDN);
            userSearchScope = LdapSearchScope.resolve(settings.get(AD_USER_SEARCH_SCOPE_SETTING), LdapSearchScope.SUB_TREE);
        }

        final void authenticate(LDAPConnection connection, String username, SecuredString password,
                          ActionListener<LdapSession> listener) {
            boolean success = false;
            try {
                connection.bind(bindUsername(username), new String(password.internalChars()));
                searchForDN(connection, username, password, Math.toIntExact(timeout.seconds()), ActionListener.wrap((entry) -> {
                    if (entry == null) {
                        IOUtils.close(connection);
                        // we did not find the user, cannot authenticate in this realm
                        listener.onFailure(new ElasticsearchSecurityException("search for user [" + username
                                + "] by principle name yielded no results"));
                    } else {
                        final String dn = entry.getDN();
                        listener.onResponse(new LdapSession(logger, connection, dn, groupsResolver, timeout, null));
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

        String bindUsername(String username) {
            return username;
        }

        abstract void searchForDN(LDAPConnection connection, String username, SecuredString password, int timeLimitSeconds,
                                  ActionListener<SearchResultEntry> listener);
    }

    /**
     * This authenticator is used for usernames that do not contain an `@` or `/`. It attempts a bind with the provided username combined
     * with the domain name specified in settings. On AD DS this will work for both upn@domain and samaccountname@domain; AD LDS will only
     * support the upn format
     */
    static class DefaultADAuthenticator extends ADAuthenticator {

        final String userSearchFilter;

        final String domainName;
        DefaultADAuthenticator(Settings settings, TimeValue timeout, Logger logger, GroupsResolver groupsResolver, String domainDN) {
            super(settings, timeout, logger, groupsResolver, domainDN);
            domainName = settings.get(AD_DOMAIN_NAME_SETTING);
            userSearchFilter = settings.get(AD_USER_SEARCH_FILTER_SETTING, "(&(objectClass=user)(|(sAMAccountName={0})" +
                    "(userPrincipalName={0}@" + domainName + ")))");
        }

        @Override
        void searchForDN(LDAPConnection connection, String username, SecuredString password, int timeLimitSeconds,
                         ActionListener<SearchResultEntry> listener) {
            try {
                searchForEntry(connection, userSearchDN, userSearchScope.scope(), createFilter(userSearchFilter, username),
                        timeLimitSeconds, listener, attributesToSearchFor(groupsResolver.attributes()));
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
     * Active Directory calls the format <code>DOMAIN\\username</code> down-level credentials and this class contains the logic necessary
     * to authenticate this form of a username
     */
    static class DownLevelADAuthenticator extends ADAuthenticator {
        Cache<String, String> domainNameCache = CacheBuilder.<String, String>builder().setMaximumWeight(100).build();

        final String domainDN;
        final Settings settings;
        final SSLService sslService;
        final RealmConfig config;

        DownLevelADAuthenticator(RealmConfig config, TimeValue timeout, Logger logger, GroupsResolver groupsResolver, String domainDN,
                                 SSLService sslService) {
            super(config.settings(), timeout, logger, groupsResolver, domainDN);
            this.domainDN = domainDN;
            this.settings = config.settings();
            this.sslService = sslService;
            this.config = config;
        }

        @Override
        void searchForDN(LDAPConnection connection, String username, SecuredString password, int timeLimitSeconds,
                         ActionListener<SearchResultEntry> listener) {
            String[] parts = username.split("\\\\");
            assert parts.length == 2;
            final String netBiosDomainName = parts[0];
            final String accountName = parts[1];

            netBiosDomainNameToDn(connection, netBiosDomainName, username, password, timeLimitSeconds, ActionListener.wrap((domainDN) -> {
                if (domainDN == null) {
                    IOUtils.close(connection);
                    listener.onResponse(null);
                } else {
                    try {
                        searchForEntry(connection, domainDN, LdapSearchScope.SUB_TREE.scope(),
                                createFilter("(&(objectClass=user)(sAMAccountName={0}))", accountName), timeLimitSeconds, listener,
                                attributesToSearchFor(groupsResolver.attributes()));
                    } catch (LDAPException e) {
                        IOUtils.closeWhileHandlingException(connection);
                        listener.onFailure(e);
                    }
                }
            }, (e) -> {
                IOUtils.closeWhileHandlingException(connection);
                listener.onFailure(e);
            }));
        }

        void netBiosDomainNameToDn(LDAPConnection connection, String netBiosDomainName, String username, SecuredString password,
                                   int timeLimitSeconds, ActionListener<String> listener) {
            final String cachedName = domainNameCache.get(netBiosDomainName);
            if (cachedName != null) {
                listener.onResponse(cachedName);
            } else if (usingGlobalCatalog(settings, connection)) {
                // the global catalog does not replicate the necessary information to map a netbios dns name to a DN so we need to instead
                // connect to the normal ports. This code uses the standard ports to avoid adding even more settings and is probably ok as
                // most AD users do not use non-standard ports
                final LDAPConnectionOptions options = connectionOptions(config, sslService, logger);
                boolean startedSearching = false;
                LDAPConnection searchConnection = null;
                try {
                    Filter filter = createFilter(NETBIOS_NAME_FILTER_TEMPLATE, netBiosDomainName);
                    if (connection.getSSLSession() != null) {
                        searchConnection = LdapUtils.privilegedConnect(() -> new LDAPConnection(connection.getSocketFactory(), options,
                                connection.getConnectedAddress(), 636));
                    } else {
                        searchConnection = LdapUtils.privilegedConnect(() ->
                                new LDAPConnection(options, connection.getConnectedAddress(), 389));
                    }
                    searchConnection.bind(username, new String(password.internalChars()));
                    final LDAPConnection finalConnection = searchConnection;
                    search(finalConnection, domainDN, LdapSearchScope.SUB_TREE.scope(), filter, timeLimitSeconds,
                            ActionListener.wrap((results) -> {
                                IOUtils.close(finalConnection);
                                handleSearchResults(results, netBiosDomainName, domainNameCache, listener);
                            }, (e) -> {
                                IOUtils.closeWhileHandlingException(connection);
                                listener.onFailure(e);
                            }),
                            "ncname");
                    startedSearching = true;
                } catch (LDAPException e) {
                    listener.onFailure(e);
                } finally {
                    if (startedSearching == false) {
                        IOUtils.closeWhileHandlingException(searchConnection);
                    }
                }
            } else {
                try {
                    Filter filter = createFilter(NETBIOS_NAME_FILTER_TEMPLATE, netBiosDomainName);
                    search(connection, domainDN, LdapSearchScope.SUB_TREE.scope(), filter, timeLimitSeconds,
                            ActionListener.wrap((results) -> handleSearchResults(results, netBiosDomainName, domainNameCache, listener),
                                    (e) -> {
                                        IOUtils.closeWhileHandlingException(connection);
                                        listener.onFailure(e);
                                    }),
                            "ncname");
                } catch (LDAPException e) {
                    listener.onFailure(e);
                }
            }
        }

        static void handleSearchResults(List<SearchResultEntry> results, String netBiosDomainName, Cache<String, String> domainNameCache,
                                        ActionListener<String> listener) {
            Optional<SearchResultEntry> entry = results.stream().filter((r) -> r.hasAttribute("ncname")).findFirst();
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

        static boolean usingGlobalCatalog(Settings settings, LDAPConnection ldapConnection) {
            Boolean usingGlobalCatalog = settings.getAsBoolean("global_catalog", null);
            if (usingGlobalCatalog != null) {
                return usingGlobalCatalog;
            }
            return ldapConnection.getConnectedPort() == 3268 || ldapConnection.getConnectedPort() == 3269;
        }
    }

    static class UpnADAuthenticator extends ADAuthenticator {

        private static final String UPN_USER_FILTER = "(&(objectClass=user)(|(sAMAccountName={0})(userPrincipalName={1})))";

        UpnADAuthenticator(Settings settings, TimeValue timeout, Logger logger, GroupsResolver groupsResolver, String domainDN) {
            super(settings, timeout, logger, groupsResolver, domainDN);
        }

        void searchForDN(LDAPConnection connection, String username, SecuredString password, int timeLimitSeconds,
                         ActionListener<SearchResultEntry> listener) {
            String[] parts = username.split("@");
            assert parts.length == 2;
            final String accountName = parts[0];
            final String domainName = parts[1];
            final String domainDN = buildDnFromDomain(domainName);
            try {
                Filter filter = createFilter(UPN_USER_FILTER, accountName, username);
                searchForEntry(connection, domainDN, LdapSearchScope.SUB_TREE.scope(), filter, timeLimitSeconds, listener,
                        attributesToSearchFor(groupsResolver.attributes()));
            } catch (LDAPException e) {
                listener.onFailure(e);
            }
        }
    }
}

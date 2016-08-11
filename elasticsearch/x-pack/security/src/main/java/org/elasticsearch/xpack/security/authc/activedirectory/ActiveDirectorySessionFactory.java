/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.activedirectory;

import com.unboundid.ldap.sdk.Attribute;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPConnectionOptions;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.SearchRequest;
import com.unboundid.ldap.sdk.SearchResult;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession.GroupsResolver;
import org.elasticsearch.xpack.security.authc.ldap.support.SessionFactory;
import org.elasticsearch.xpack.security.authc.support.SecuredString;
import org.elasticsearch.xpack.security.ssl.SSLService;

import java.util.concurrent.ExecutionException;

import static org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils.attributesToSearchFor;
import static org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils.createFilter;
import static org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils.search;

/**
 * This Class creates LdapSessions authenticating via the custom Active Directory protocol.  (that being
 * authenticating with a principal name, "username@domain", then searching through the directory to find the
 * user entry in Active Directory that matches the user name).  This eliminates the need for user templates, and simplifies
 * the configuration for windows admins that may not be familiar with LDAP concepts.
 */
public class ActiveDirectorySessionFactory extends SessionFactory {

    public static final String AD_DOMAIN_NAME_SETTING = "domain_name";

    public static final String AD_GROUP_SEARCH_BASEDN_SETTING = "group_search.base_dn";
    public static final String AD_GROUP_SEARCH_SCOPE_SETTING = "group_search.scope";
    public static final String AD_USER_SEARCH_BASEDN_SETTING = "user_search.base_dn";
    public static final String AD_USER_SEARCH_FILTER_SETTING = "user_search.filter";
    public static final String AD_USER_SEARCH_SCOPE_SETTING = "user_search.scope";
    private static final String NETBIOS_NAME_FILTER_TEMPLATE = "(netbiosname={0})";

    private final String domainName;
    private final GroupsResolver groupResolver;
    private final DefaultADAuthenticator defaultADAuthenticator;
    private final DownLevelADAuthenticator downLevelADAuthenticator;
    private final UpnADAuthenticator upnADAuthenticator;

    public ActiveDirectorySessionFactory(RealmConfig config, SSLService sslService) {
        super(config, sslService);
        Settings settings = config.settings();
        domainName = settings.get(AD_DOMAIN_NAME_SETTING);
        if (domainName == null) {
            throw new IllegalArgumentException("missing [" + AD_DOMAIN_NAME_SETTING + "] setting for active directory");
        }
        String domainDN = buildDnFromDomain(domainName);
        groupResolver = new ActiveDirectoryGroupsResolver(settings.getAsSettings("group_search"), domainDN);
        defaultADAuthenticator = new DefaultADAuthenticator(settings, timeout, logger, groupResolver, domainDN);
        downLevelADAuthenticator = new DownLevelADAuthenticator(settings, timeout, logger, groupResolver, domainDN);
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
     * @return An authenticated LdapSession
     */
    @Override
    protected LdapSession getSession(String username, SecuredString password) throws Exception {
        LDAPConnection connection = serverSet.getConnection();
        ADAuthenticator authenticator = getADAuthenticator(username);
        return authenticator.authenticate(connection, username, password);
    }

    /**
     * @param domain active directory domain name
     * @return LDAP DN, distinguished name, of the root of the domain
     */
    static String buildDnFromDomain(String domain) {
        return "DC=" + domain.replace(".", ",DC=");
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
        final ESLogger logger;
        final GroupsResolver groupsResolver;
        final String userSearchDN;
        final LdapSearchScope userSearchScope;

        ADAuthenticator(Settings settings, TimeValue timeout, ESLogger logger, GroupsResolver groupsResolver, String domainDN) {
            this.timeout = timeout;
            this.logger = logger;
            this.groupsResolver = groupsResolver;
            userSearchDN = settings.get(AD_USER_SEARCH_BASEDN_SETTING, domainDN);
            userSearchScope = LdapSearchScope.resolve(settings.get(AD_USER_SEARCH_SCOPE_SETTING), LdapSearchScope.SUB_TREE);
        }

        LdapSession authenticate(LDAPConnection connection, String username, SecuredString password) throws LDAPException {
            boolean success = false;
            try {
                connection.bind(bindUsername(username), new String(password.internalChars()));
                SearchRequest searchRequest = getSearchRequest(connection, username, password);
                searchRequest.setTimeLimitSeconds(Math.toIntExact(timeout.seconds()));
                SearchResult results = search(connection, searchRequest, logger);
                int numResults = results.getEntryCount();
                if (numResults > 1) {
                    throw new IllegalStateException("search for user [" + username + "] by principle name yielded multiple results");
                } else if (numResults < 1) {
                    throw new IllegalStateException("search for user [" + username + "] by principle name yielded no results");
                }

                String dn = results.getSearchEntries().get(0).getDN();
                LdapSession session = new LdapSession(logger, connection, dn, groupsResolver, timeout, null);
                success = true;
                return session;
            } finally {
                if (success == false) {
                    connection.close();
                }
            }
        }

        String bindUsername(String username) {
            return username;
        }

        abstract SearchRequest getSearchRequest(LDAPConnection connection, String username, SecuredString password) throws LDAPException;
    }

    /**
     * This authenticator is used for usernames that do not contain an `@` or `/`. It attempts a bind with the provided username combined
     * with the domain name specified in settings. On AD DS this will work for both upn@domain and samaccountname@domain; AD LDS will only
     * support the upn format
     */
    static class DefaultADAuthenticator extends ADAuthenticator {

        final String userSearchFilter;

        final String domainName;
        DefaultADAuthenticator(Settings settings, TimeValue timeout, ESLogger logger, GroupsResolver groupsResolver, String domainDN) {
            super(settings, timeout, logger, groupsResolver, domainDN);
            domainName = settings.get(AD_DOMAIN_NAME_SETTING);
            userSearchFilter = settings.get(AD_USER_SEARCH_FILTER_SETTING, "(&(objectClass=user)(|(sAMAccountName={0})" +
                    "(userPrincipalName={0}@" + domainName + ")))");
        }

        @Override
        SearchRequest getSearchRequest(LDAPConnection connection, String username, SecuredString password) throws LDAPException {
            return new SearchRequest(userSearchDN, userSearchScope.scope(),
                    createFilter(userSearchFilter, username), attributesToSearchFor(groupsResolver.attributes()));
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

        DownLevelADAuthenticator(Settings settings, TimeValue timeout, ESLogger logger, GroupsResolver groupsResolver, String domainDN) {
            super(settings, timeout, logger, groupsResolver, domainDN);
            this.domainDN = domainDN;
            this.settings = settings;
        }

        SearchRequest getSearchRequest(LDAPConnection connection, String username, SecuredString password) throws LDAPException {
            String[] parts = username.split("\\\\");
            assert parts.length == 2;
            final String netBiosDomainName = parts[0];
            final String accountName = parts[1];

            final String domainDn = netBiosDomainNameToDn(connection, netBiosDomainName, username, password);

            return new SearchRequest(domainDn, LdapSearchScope.SUB_TREE.scope(),
                    createFilter("(&(objectClass=user)(sAMAccountName={0}))", accountName),
                    attributesToSearchFor(groupsResolver.attributes()));
        }

        String netBiosDomainNameToDn(LDAPConnection connection, String netBiosDomainName, String username, SecuredString password)
                throws LDAPException {
            try {
                return domainNameCache.computeIfAbsent(netBiosDomainName, (key) -> {
                    LDAPConnection searchConnection = connection;
                    boolean openedConnection = false;
                    try {
                        // global catalog does not replicate the necessary information by default
                        // TODO add settings for ports and maybe cache connectionOptions
                        if (usingGlobalCatalog(settings, connection)) {
                            LDAPConnectionOptions options = connectionOptions(settings);
                            if (connection.getSSLSession() != null) {
                                searchConnection = new LDAPConnection(connection.getSocketFactory(), options,
                                        connection.getConnectedAddress(), 636);
                            } else {
                                searchConnection = new LDAPConnection(options, connection.getConnectedAddress(), 389);
                            }
                            openedConnection = true;
                            searchConnection.bind(username, new String(password.internalChars()));
                        }

                        SearchRequest searchRequest = new SearchRequest(domainDN, LdapSearchScope.SUB_TREE.scope(),
                                createFilter(NETBIOS_NAME_FILTER_TEMPLATE, netBiosDomainName), "ncname");
                        SearchResult results = search(searchConnection, searchRequest, logger);
                        if (results.getEntryCount() > 0) {
                            Attribute attribute = results.getSearchEntries().get(0).getAttribute("ncname");
                            if (attribute != null) {
                                return attribute.getValue();
                            }
                        }
                        logger.debug("failed to find domain name DN from netbios name [{}]", netBiosDomainName);
                        return null;
                    } finally {
                        if (openedConnection) {
                            searchConnection.close();
                        }
                    }
                });
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof LDAPException) {
                    throw (LDAPException) cause;
                } else {
                    connection.close();
                    throw new ElasticsearchException("error occurred while mapping [{}] to domain DN", cause, netBiosDomainName);
                }
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

        UpnADAuthenticator(Settings settings, TimeValue timeout, ESLogger logger, GroupsResolver groupsResolver, String domainDN) {
            super(settings, timeout, logger, groupsResolver, domainDN);
        }

        SearchRequest getSearchRequest(LDAPConnection connection, String username, SecuredString password) throws LDAPException {
            String[] parts = username.split("@");
            assert parts.length == 2;
            final String accountName = parts[0];
            final String domainName = parts[1];
            final String domainDN = buildDnFromDomain(domainName);
            return new SearchRequest(domainDN, LdapSearchScope.SUB_TREE.scope(),
                    createFilter(UPN_USER_FILTER, accountName, username), attributesToSearchFor(groupsResolver.attributes()));
        }
    }
}

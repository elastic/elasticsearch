/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.activedirectory;

import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.SearchRequest;
import com.unboundid.ldap.sdk.SearchResult;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.shield.authc.ldap.support.LdapSession;
import org.elasticsearch.shield.authc.ldap.support.LdapSession.GroupsResolver;
import org.elasticsearch.shield.authc.ldap.support.SessionFactory;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.ssl.ClientSSLService;

import java.io.IOException;

import static org.elasticsearch.shield.authc.ldap.support.LdapUtils.createFilter;
import static org.elasticsearch.shield.authc.ldap.support.LdapUtils.search;
import static org.elasticsearch.shield.support.Exceptions.authenticationError;

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

    private final String userSearchDN;
    private final String domainName;
    private final String userSearchFilter;
    private final LdapSearchScope userSearchScope;
    private final GroupsResolver groupResolver;

    public ActiveDirectorySessionFactory(RealmConfig config, ClientSSLService sslService) {
        super(config, sslService);
        Settings settings = config.settings();
        domainName = settings.get(AD_DOMAIN_NAME_SETTING);
        if (domainName == null) {
            throw new IllegalArgumentException("missing [" + AD_DOMAIN_NAME_SETTING + "] setting for active directory");
        }
        String domainDN = buildDnFromDomain(domainName);
        userSearchDN = settings.get(AD_USER_SEARCH_BASEDN_SETTING, domainDN);
        userSearchScope = LdapSearchScope.resolve(settings.get(AD_USER_SEARCH_SCOPE_SETTING), LdapSearchScope.SUB_TREE);
        userSearchFilter = settings.get(AD_USER_SEARCH_FILTER_SETTING, "(&(objectClass=user)(|(sAMAccountName={0})" +
                "(userPrincipalName={0}@" + domainName + ")))");
        groupResolver = new ActiveDirectoryGroupsResolver(settings.getAsSettings("group_search"), domainDN);
    }


    @Override
    protected LDAPServers ldapServers(Settings settings) {
        String[] ldapUrls = settings.getAsArray(URLS_SETTING, new String[]{"ldap://" + domainName + ":389"});
        return new LDAPServers(ldapUrls);
    }

    /**
     * This is an active directory bind that looks up the user DN after binding with a windows principal.
     *
     * @param userName name of the windows user without the domain
     * @return An authenticated
     */
    @Override
    protected LdapSession getSession(String userName, SecuredString password) throws Exception {
        LDAPConnection connection;

        try {
            connection = serverSet.getConnection();
        } catch (LDAPException e) {
            throw new IOException("failed to connect to any active directory servers", e);
        }

        String userPrincipal = userName + "@" + domainName;
        try {
            connection.bind(userPrincipal, new String(password.internalChars()));
            SearchRequest searchRequest = new SearchRequest(userSearchDN, userSearchScope.scope(),
                    createFilter(userSearchFilter, userName), SearchRequest.NO_ATTRIBUTES);
            searchRequest.setTimeLimitSeconds(Math.toIntExact(timeout.seconds()));
            SearchResult results = search(connection, searchRequest, logger);
            int numResults = results.getEntryCount();
            if (numResults > 1) {
                throw new IllegalStateException("search for user [" + userName + "] by principle name yielded multiple results");
            } else if (numResults < 1) {
                throw new IllegalStateException("search for user [" + userName + "] by principle name yielded no results");
            }
            String dn = results.getSearchEntries().get(0).getDN();
            return new LdapSession(connectionLogger, connection, dn, groupResolver, timeout);
        } catch (LDAPException e) {
            connection.close();
            throw authenticationError("unable to authenticate user [{}] to active directory domain [{}]", e, userName, domainName);
        }
    }

    /**
     * @param domain active directory domain name
     * @return LDAP DN, distinguished name, of the root of the domain
     */
    String buildDnFromDomain(String domain) {
        return "DC=" + domain.replace(".", ",DC=");
    }

}

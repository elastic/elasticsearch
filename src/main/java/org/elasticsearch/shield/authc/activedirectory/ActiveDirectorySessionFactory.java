/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.activedirectory;

import com.unboundid.ldap.sdk.*;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.primitives.Ints;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.ShieldSettingsException;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.ldap.support.LdapSession;
import org.elasticsearch.shield.authc.ldap.support.LdapSession.GroupsResolver;
import org.elasticsearch.shield.authc.ldap.support.SessionFactory;
import org.elasticsearch.shield.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.shield.ssl.ClientSSLService;

import javax.net.SocketFactory;

import static org.elasticsearch.shield.authc.ldap.support.LdapUtils.*;

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
    private final ServerSet ldapServerSet;

    public ActiveDirectorySessionFactory(RealmConfig config, ClientSSLService sslService) {
        super(config);
        Settings settings = config.settings();
        domainName = settings.get(AD_DOMAIN_NAME_SETTING);
        if (domainName == null) {
            throw new ShieldSettingsException("missing [" + AD_DOMAIN_NAME_SETTING + "] setting for active directory");
        }
        String domainDN = buildDnFromDomain(domainName);
        userSearchDN = settings.get(AD_USER_SEARCH_BASEDN_SETTING, domainDN);
        userSearchScope = LdapSearchScope.resolve(settings.get(AD_USER_SEARCH_SCOPE_SETTING), LdapSearchScope.SUB_TREE);
        userSearchFilter = settings.get(AD_USER_SEARCH_FILTER_SETTING, "(&(objectClass=user)(|(sAMAccountName={0})(userPrincipalName={0}@" + domainName + ")))");
        ldapServerSet = serverSet(config.settings(), sslService);
        groupResolver = new ActiveDirectoryGroupsResolver(settings.getAsSettings("group_search"), domainDN);
    }

    ServerSet serverSet(Settings settings, ClientSSLService clientSSLService) {
        String[] ldapUrls = settings.getAsArray(URLS_SETTING, new String[] { "ldap://" + domainName + ":389" });
        LDAPServers servers = new LDAPServers(ldapUrls);
        LDAPConnectionOptions options = connectionOptions(settings);
        SocketFactory socketFactory;
        if (servers.ssl()) {
            socketFactory = clientSSLService.sslSocketFactory();
            if (settings.getAsBoolean(HOSTNAME_VERIFICATION_SETTING, true)) {
                logger.debug("using encryption for LDAP connections with hostname verification");
            } else {
                logger.debug("using encryption for LDAP connections without hostname verification");
            }
        } else {
            socketFactory = null;
        }
        FailoverServerSet serverSet = new FailoverServerSet(servers.addresses(), servers.ports(), socketFactory, options);
        serverSet.setReOrderOnFailover(true);
        return serverSet;
    }

    /**
     * This is an active directory bind that looks up the user DN after binding with a windows principal.
     *
     * @param userName name of the windows user without the domain
     * @return An authenticated
     */
    @Override
    public LdapSession open(String userName, SecuredString password) {
        LDAPConnection connection;

        try {
            connection = ldapServerSet.getConnection();
        } catch (LDAPException e) {
            throw new ActiveDirectoryException("failed to connect to any active directory servers");
        }

        String userPrincipal = userName + "@" + domainName;
        try {
            connection.bind(userPrincipal, new String(password.internalChars()));
            SearchRequest searchRequest = new SearchRequest(userSearchDN, userSearchScope.scope(), createFilter(userSearchFilter, userName), Strings.EMPTY_ARRAY);
            searchRequest.setTimeLimitSeconds(Ints.checkedCast(timeout.seconds()));
            SearchResult results = search(connection, searchRequest, logger);
            int numResults = results.getEntryCount();
            if (numResults > 1) {
                throw new ActiveDirectoryException("search for user [" + userName + "] by principle name yielded multiple results");
            } else if (numResults < 1) {
                throw new ActiveDirectoryException("search for user [" + userName + "] by principle name yielded no results");
            }
            String dn = results.getSearchEntries().get(0).getDN();
            return new LdapSession(connectionLogger, connection, dn, groupResolver, timeout);
        } catch (LDAPException e) {
            connection.close();
            throw new ActiveDirectoryException("unable to authenticate user [" + userName + "] to active directory domain [" + domainName + "]", e);
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

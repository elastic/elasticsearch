/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import java.io.Serializable;
import java.util.Hashtable;

/**
 * This Class creates LdapConnections authenticating via the custom Active Directory protocol.  (that being
 * authenticating with a principal name, "username@domain", then searching through the directory to find the
 * user entry in LDAP that matches the user name).  This eliminates the need for user templates, and simplifies
 * the configuration for windows admins that may not be familiar with LDAP concepts.
 */
public class ActiveDirectoryConnectionFactory extends AbstractComponent implements LdapConnectionFactory {

    public static final String AD_DOMAIN_NAME_SETTING = "domain_name";
    public static final String AD_PORT = "default_port";
    public static final String AD_USER_SEARCH_BASEDN_SETTING = "user_search_dn";

    private final ImmutableMap<String, Serializable> sharedLdapEnv;
    private final String userSearchDN;
    private final String domainName;

    @Inject
    public ActiveDirectoryConnectionFactory(Settings settings){
        super(settings);
        domainName = componentSettings.get(AD_DOMAIN_NAME_SETTING);
        if (domainName == null) {
            throw new org.elasticsearch.shield.SecurityException("Missing [" + AD_DOMAIN_NAME_SETTING + "] setting for active directory");
        }
        userSearchDN = componentSettings.get(AD_USER_SEARCH_BASEDN_SETTING, buildDnFromDomain(domainName));
        int port = componentSettings.getAsInt(AD_PORT, 389);
        String[] ldapUrls = componentSettings.getAsArray(URLS_SETTING,  new String[] { "ldap://" + domainName + ":" + port });


        sharedLdapEnv = ImmutableMap.<String, Serializable>builder()
                .put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory")
                .put(Context.PROVIDER_URL, Strings.arrayToCommaDelimitedString(ldapUrls))
                .put(Context.REFERRAL, "follow")
                .build();
    }

    /**
     * This is an active directory bind that looks up the user DN after binding with a windows principal.
     * @param userName name of the windows user without the domain
     * @return An authenticated
     */
    @Override
    public LdapConnection bind(String userName, char[] password) {
        String userPrincipal = userName + "@" + this.domainName;

        Hashtable<String, java.io.Serializable> ldapEnv = new Hashtable<>(this.sharedLdapEnv);
        ldapEnv.put(Context.SECURITY_AUTHENTICATION, "simple");
        ldapEnv.put(Context.SECURITY_PRINCIPAL, userPrincipal);
        ldapEnv.put(Context.SECURITY_CREDENTIALS, password);

        try {
            DirContext ctx = new InitialDirContext(ldapEnv);
            SearchControls searchCtls = new SearchControls();
            searchCtls.setSearchScope(SearchControls.SUBTREE_SCOPE);
            searchCtls.setReturningAttributes( new String[0] );

            String searchFilter = "(&(objectClass=user)(userPrincipalName={0}))";
            NamingEnumeration<SearchResult> results = ctx.search(userSearchDN, searchFilter, new Object[]{ userPrincipal }, searchCtls);

            if (results.hasMore()){
                SearchResult entry = results.next();
                String name = entry.getNameInNamespace();

                if (!results.hasMore()) {
                    //searchByAttribute=true, group subtree search=false, groupSubtreeDN=null
                    return new LdapConnection(ctx, name, true, false, null);
                }
                throw new LdapException("Search for user [" + userName + "] by principle name yielded multiple results");
            }

            throw new LdapException("Search for user [" + userName + "] yielded no results");

        } catch (NamingException e) {
            throw new LdapException("Unable to authenticate user [" + userName + "] to active directory domain ["+ domainName +"]", e);
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

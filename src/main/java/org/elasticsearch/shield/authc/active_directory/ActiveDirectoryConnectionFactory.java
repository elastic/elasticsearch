/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.active_directory;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.ShieldSettingsException;
import org.elasticsearch.shield.authc.ldap.LdapException;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.ldap.ClosableNamingEnumeration;
import org.elasticsearch.shield.authc.support.ldap.ConnectionFactory;

import javax.naming.Context;
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
 * user entry in Active Directory that matches the user name).  This eliminates the need for user templates, and simplifies
 * the configuration for windows admins that may not be familiar with LDAP concepts.
 */
public class ActiveDirectoryConnectionFactory extends ConnectionFactory {

    public static final String AD_DOMAIN_NAME_SETTING = "domain_name";
    public static final String AD_USER_SEARCH_BASEDN_SETTING = "user_search_dn";

    private final ImmutableMap<String, Serializable> sharedLdapEnv;
    private final String userSearchDN;
    private final String domainName;
    private final int timeoutMilliseconds;

    @Inject
    public ActiveDirectoryConnectionFactory(Settings settings) {
        super(settings);
        domainName = settings.get(AD_DOMAIN_NAME_SETTING);
        if (domainName == null) {
            throw new ShieldSettingsException("Missing [" + AD_DOMAIN_NAME_SETTING + "] setting for active directory");
        }
        userSearchDN = settings.get(AD_USER_SEARCH_BASEDN_SETTING, buildDnFromDomain(domainName));
        timeoutMilliseconds = (int) settings.getAsTime(TIMEOUT_LDAP_SETTING, TIMEOUT_DEFAULT).millis();
        String[] ldapUrls = settings.getAsArray(URLS_SETTING, new String[] { "ldaps://" + domainName + ":636" });

        ImmutableMap.Builder<String, Serializable> builder = ImmutableMap.<String, Serializable>builder()
                .put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory")
                .put(Context.PROVIDER_URL, Strings.arrayToCommaDelimitedString(ldapUrls))
                .put(JNDI_LDAP_CONNECT_TIMEOUT, Long.toString(settings.getAsTime(TIMEOUT_TCP_CONNECTION_SETTING, TIMEOUT_DEFAULT).millis()))
                .put(JNDI_LDAP_READ_TIMEOUT, Long.toString(settings.getAsTime(TIMEOUT_TCP_READ_SETTING, TIMEOUT_DEFAULT).millis()))
                .put("java.naming.ldap.attributes.binary", "tokenGroups")
                .put(Context.REFERRAL, "follow");

        configureJndiSSL(ldapUrls, builder);

        sharedLdapEnv = builder.build();
    }

    /**
     * This is an active directory bind that looks up the user DN after binding with a windows principal.
     *
     * @param userName name of the windows user without the domain
     * @return An authenticated
     */
    @Override
    public ActiveDirectoryConnection open(String userName, SecuredString password) {
        String userPrincipal = userName + "@" + this.domainName;
        Hashtable<String, Serializable> ldapEnv = new Hashtable<>(this.sharedLdapEnv);
        ldapEnv.put(Context.SECURITY_AUTHENTICATION, "simple");
        ldapEnv.put(Context.SECURITY_PRINCIPAL, userPrincipal);
        ldapEnv.put(Context.SECURITY_CREDENTIALS, password.internalChars());

        DirContext ctx = null;
        try {
            ctx = new InitialDirContext(ldapEnv);
            SearchControls searchCtls = new SearchControls();
            searchCtls.setSearchScope(SearchControls.SUBTREE_SCOPE);
            searchCtls.setReturningAttributes(Strings.EMPTY_ARRAY);
            searchCtls.setTimeLimit(timeoutMilliseconds);
            String searchFilter = "(&(objectClass=user)(|(sAMAccountName={0})(userPrincipalName={1})))";
            try (ClosableNamingEnumeration<SearchResult> results = new ClosableNamingEnumeration(
                ctx.search(userSearchDN, searchFilter, new Object[] { userName, userPrincipal }, searchCtls))) {

                if(results.hasMore()){
                    SearchResult entry = results.next();
                    String name = entry.getNameInNamespace();

                    if (!results.hasMore()) {
                        return new ActiveDirectoryConnection(ctx, name, userSearchDN, timeoutMilliseconds);
                    }
                    throw new ActiveDirectoryException("Search for user [" + userName + "] by principle name yielded multiple results");
                }

                ctx.close();
                throw new ActiveDirectoryException("Search for user [" + userName + "] by principle name yielded multiple results");
            }
        } catch (NamingException | LdapException e) {
            if (ctx != null) {
                try {
                    ctx.close();
                } catch (NamingException closeException) {
                    logger.error("An unexpected error occurred closing an LDAP exception", closeException);
                }
            }
            throw new ActiveDirectoryException("Unable to authenticate user [" + userName + "] to active directory domain [" + domainName + "]", e);
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

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
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.ldap.Rdn;
import java.io.Serializable;
import java.text.MessageFormat;
import java.util.Hashtable;

/**
 * This factory creates LDAP connections via iterating through user templates.
 *
 * Note that even though there is a separate factory for Active Directory, this factory would work against AD.  A template
 * for each user context would need to be supplied.
 */
public class StandardLdapConnectionFactory extends AbstractComponent implements LdapConnectionFactory {

    public static final String USER_DN_TEMPLATES_SETTING = "user_dn_templates";
    public static final String GROUP_SEARCH_SUBTREE_SETTING = "group_search.subtree_search";
    public static final String GROUP_SEARCH_BASEDN_SETTING = "group_search.group_search_dn";

    private final ImmutableMap<String, Serializable> sharedLdapEnv;
    private final String[] userDnTemplates;
    protected final String groupSearchDN;
    protected final boolean groupSubTreeSearch;
    protected final boolean findGroupsByAttribute;

    @Inject
    public StandardLdapConnectionFactory(Settings settings) {
        super(settings);
        userDnTemplates = componentSettings.getAsArray(USER_DN_TEMPLATES_SETTING);
        if (userDnTemplates == null) {
            throw new org.elasticsearch.shield.SecurityException("Missing required ldap setting [" + USER_DN_TEMPLATES_SETTING + "]");
        }
        String[] ldapUrls = componentSettings.getAsArray(URLS_SETTING);
        if (ldapUrls == null) {
            throw new org.elasticsearch.shield.SecurityException("Missing required ldap setting [" + URLS_SETTING + "]");
        }
        sharedLdapEnv = ImmutableMap.<String, Serializable>builder()
                .put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory")
                .put(Context.PROVIDER_URL, Strings.arrayToCommaDelimitedString(ldapUrls))
                .put(Context.REFERRAL, "follow")
                .build();

        groupSearchDN = componentSettings.get(GROUP_SEARCH_BASEDN_SETTING);
        findGroupsByAttribute = groupSearchDN == null;
        groupSubTreeSearch = componentSettings.getAsBoolean(GROUP_SEARCH_SUBTREE_SETTING, false);
    }

    /**
     * This iterates through the configured user templates attempting to connect.  If all attempts fail, all exceptions
     * are combined into one Exception as nested exceptions.
     * @param username a relative name, Not a distinguished name, that will be inserted into the template.
     * @return authenticated exception
     */
    @Override
    public LdapConnection bind(String username, char[] password) {
        //SASL, MD5, etc. all options here stink, we really need to go over ssl + simple authentication
        Hashtable<String, java.io.Serializable> ldapEnv = new Hashtable<>(this.sharedLdapEnv);
        ldapEnv.put(Context.SECURITY_AUTHENTICATION, "simple");
        ldapEnv.put(Context.SECURITY_CREDENTIALS, password);

        for (String template : userDnTemplates) {
            String dn = buildDnFromTemplate(username, template);
            ldapEnv.put(Context.SECURITY_PRINCIPAL, dn);
            try {
                DirContext ctx = new InitialDirContext(ldapEnv);

                //return the first good connection
                return new LdapConnection(ctx, dn, findGroupsByAttribute, groupSubTreeSearch, groupSearchDN);

            } catch (NamingException e) {
                logger.warn("Failed ldap authentication with user template [{}], dn [{}]", template, dn);
            }
        }
        throw new LdapException("Failed ldap authentication");
    }

    /**
     * Securely escapes the username and inserts it into the template using MessageFormat
     * @param username username to insert into the DN template.  Any commas, equals or plus will be escaped.
     * @return DN (distinquished name) build from the template.
     */
    String buildDnFromTemplate(String username, String template) {
        //this value must be escaped to avoid manipulation of the template DN.
        String escapedUsername = Rdn.escapeValue(username);
        return MessageFormat.format(template, escapedUsername);
    }
}

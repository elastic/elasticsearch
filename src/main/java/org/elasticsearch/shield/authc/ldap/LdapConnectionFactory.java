/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.ShieldSettingsException;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.ldap.ConnectionFactory;

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
public class LdapConnectionFactory extends ConnectionFactory<LdapConnection> {

    public static final String USER_DN_TEMPLATES_SETTING = "user_dn_templates";
    public static final String GROUP_SEARCH_SUBTREE_SETTING = "group_search.subtree";
    public static final String GROUP_SEARCH_BASEDN_SETTING = "group_search.base_dn";
    public static final String GROUP_SEARCH_FILTER_SETTING = "group_search.filter";
    public static final String GROUP_SEARCH_USER_ATTRIBUTE_SETTING = "group_search.user_attribute";

    private static final String GROUP_SEARCH_DEFAULT_FILTER = "(&" +
            "(|(objectclass=groupOfNames)(objectclass=groupOfUniqueNames)(objectclass=group))" +
            "(|(uniqueMember={0})(member={0})))";

    private final ImmutableMap<String, Serializable> sharedLdapEnv;
    private final String[] userDnTemplates;
    protected final String groupSearchDN;
    protected final boolean groupSubTreeSearch;
    protected final boolean findGroupsByAttribute;
    private final int timeoutMilliseconds;
    private final String groupFilter;
    private final String groupSearchUserAttribute;

    @Inject()
    public LdapConnectionFactory(RealmConfig config) {
        super(LdapConnection.class, config);
        Settings settings = config.settings();
        userDnTemplates = settings.getAsArray(USER_DN_TEMPLATES_SETTING);
        if (userDnTemplates == null) {
            throw new ShieldSettingsException("missing required LDAP setting [" + USER_DN_TEMPLATES_SETTING + "]");
        }
        String[] ldapUrls = settings.getAsArray(URLS_SETTING);
        if (ldapUrls == null) {
            throw new ShieldSettingsException("missing required LDAP setting [" + URLS_SETTING + "]");
        }

        timeoutMilliseconds = (int) settings.getAsTime(TIMEOUT_LDAP_SETTING, TIMEOUT_DEFAULT).millis();

        ImmutableMap.Builder<String, Serializable> builder = ImmutableMap.<String, Serializable>builder()
                .put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory")
                .put(Context.PROVIDER_URL, Strings.arrayToCommaDelimitedString(ldapUrls))
                .put(JNDI_LDAP_READ_TIMEOUT, Long.toString(settings.getAsTime(TIMEOUT_TCP_READ_SETTING, TIMEOUT_DEFAULT).millis()))
                .put(JNDI_LDAP_CONNECT_TIMEOUT, Long.toString(settings.getAsTime(TIMEOUT_TCP_CONNECTION_SETTING, TIMEOUT_DEFAULT).millis()))
                .put(Context.REFERRAL, "follow");

        configureJndiSSL(ldapUrls, builder);

        sharedLdapEnv = builder.build();
        groupSearchDN = settings.get(GROUP_SEARCH_BASEDN_SETTING);
        findGroupsByAttribute = groupSearchDN == null;
        groupSubTreeSearch = settings.getAsBoolean(GROUP_SEARCH_SUBTREE_SETTING, true);
        groupFilter = settings.get(GROUP_SEARCH_FILTER_SETTING, GROUP_SEARCH_DEFAULT_FILTER);
        groupSearchUserAttribute = settings.get(GROUP_SEARCH_FILTER_SETTING) == null ?
                null : settings.get(GROUP_SEARCH_USER_ATTRIBUTE_SETTING);  //if filter isn't set we don't want to change from using the DN
    }

    /**
     * This iterates through the configured user templates attempting to open.  If all attempts fail, all exceptions
     * are combined into one Exception as nested exceptions.
     *
     * @param username a relative name, Not a distinguished name, that will be inserted into the template.
     * @return authenticated exception
     */
    @Override
    public LdapConnection open(String username, SecuredString password) {
        //SASL, MD5, etc. all options here stink, we really need to go over ssl + simple authentication
        Hashtable<String, Serializable> ldapEnv = new Hashtable<>(this.sharedLdapEnv);
        ldapEnv.put(Context.SECURITY_AUTHENTICATION, "simple");
        ldapEnv.put(Context.SECURITY_CREDENTIALS, password.internalChars());

        for (String template : userDnTemplates) {
            String dn = buildDnFromTemplate(username, template);
            ldapEnv.put(Context.SECURITY_PRINCIPAL, dn);
            try {
                DirContext ctx = new InitialDirContext(ldapEnv);

                //return the first good connection
                return new LdapConnection(connectionLogger, ctx, dn, timeoutMilliseconds, findGroupsByAttribute, groupSubTreeSearch,
                        groupFilter, groupSearchDN, groupSearchUserAttribute);

            } catch (NamingException e) {
                logger.warn("failed LDAP authentication with user template [{}] and DN [{}]", e, template, dn);
            }
        }

        throw new LdapException("failed LDAP authentication");
    }

    /**
     * Securely escapes the username and inserts it into the template using MessageFormat
     *
     * @param username username to insert into the DN template.  Any commas, equals or plus will be escaped.
     * @return DN (distinquished name) build from the template.
     */
    String buildDnFromTemplate(String username, String template) {
        //this value must be escaped to avoid manipulation of the template DN.
        String escapedUsername = Rdn.escapeValue(username);
        return MessageFormat.format(template, escapedUsername);
    }
}

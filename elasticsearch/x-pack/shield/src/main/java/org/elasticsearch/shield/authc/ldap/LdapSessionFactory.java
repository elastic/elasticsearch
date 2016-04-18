/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.ldap.support.LdapSession;
import org.elasticsearch.shield.authc.ldap.support.LdapSession.GroupsResolver;
import org.elasticsearch.shield.authc.ldap.support.SessionFactory;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.ssl.ClientSSLService;
import org.elasticsearch.shield.support.Exceptions;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Locale;

import static org.elasticsearch.shield.authc.ldap.support.LdapUtils.escapedRDNValue;

/**
 * This factory creates LDAP connections via iterating through user templates.
 *
 * Note that even though there is a separate factory for Active Directory, this factory would work against AD.  A template
 * for each user context would need to be supplied.
 */
public class LdapSessionFactory extends SessionFactory {

    public static final String USER_DN_TEMPLATES_SETTING = "user_dn_templates";

    private final String[] userDnTemplates;
    private final GroupsResolver groupResolver;

    public LdapSessionFactory(RealmConfig config, ClientSSLService sslService) {
        super(config, sslService);
        Settings settings = config.settings();
        userDnTemplates = settings.getAsArray(USER_DN_TEMPLATES_SETTING);
        if (userDnTemplates == null) {
            throw new IllegalArgumentException("missing required LDAP setting [" + USER_DN_TEMPLATES_SETTING + "]");
        }
        groupResolver = groupResolver(settings);
    }

    /**
     * This iterates through the configured user templates attempting to open.  If all attempts fail, the last exception
     * is kept as the cause of the thrown exception
     *
     * @param username a relative name, Not a distinguished name, that will be inserted into the template.
     * @return authenticated exception
     */
    @Override
    protected LdapSession getSession(String username, SecuredString password) throws Exception {
        LDAPConnection connection;

        try {
            connection = serverSet.getConnection();
        } catch (LDAPException e) {
            throw new IOException("failed to connect to any LDAP servers", e);
        }

        LDAPException lastException = null;
        String passwordString = new String(password.internalChars());
        for (String template : userDnTemplates) {
            String dn = buildDnFromTemplate(username, template);
            try {
                connection.bind(dn, passwordString);
                return new LdapSession(connectionLogger, connection, dn, groupResolver, timeout);
            } catch (LDAPException e) {
                if (logger.isDebugEnabled()) {
                    logger.debug("failed LDAP authentication with user template [{}] and DN [{}]", e, template, dn);
                } else {
                    logger.warn("failed LDAP authentication with user template [{}] and DN [{}]: {}", template, dn, e.getMessage());
                }

                lastException = e;
            }
        }

        connection.close();
        throw Exceptions.authenticationError("failed LDAP authentication", lastException);
    }

    /**
     * Securely escapes the username and inserts it into the template using MessageFormat
     *
     * @param username username to insert into the DN template.  Any commas, equals or plus will be escaped.
     * @return DN (distinquished name) build from the template.
     */
    String buildDnFromTemplate(String username, String template) {
        //this value must be escaped to avoid manipulation of the template DN.
        String escapedUsername = escapedRDNValue(username);
        return new MessageFormat(template, Locale.ROOT).format(new Object[] { escapedUsername }, new StringBuffer(), null).toString();
    }

    static GroupsResolver groupResolver(Settings settings) {
        Settings searchSettings = settings.getAsSettings("group_search");
        if (!searchSettings.names().isEmpty()) {
            return new SearchGroupsResolver(searchSettings);
        }
        return new UserAttributeGroupsResolver(settings);
    }
}

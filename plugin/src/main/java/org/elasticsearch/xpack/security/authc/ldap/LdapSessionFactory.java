/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.SimpleBindRequest;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.authc.RealmSettings;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapMetaDataResolver;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession.GroupsResolver;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils;
import org.elasticsearch.xpack.security.authc.ldap.support.SessionFactory;
import org.elasticsearch.xpack.security.authc.support.CharArrays;
import org.elasticsearch.xpack.ssl.SSLService;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils.escapedRDNValue;

/**
 * This factory creates LDAP connections via iterating through user templates.
 *
 * Note that even though there is a separate factory for Active Directory, this factory would work against AD.  A template
 * for each user context would need to be supplied.
 */
public class LdapSessionFactory extends SessionFactory {

    public static final Setting<List<String>> USER_DN_TEMPLATES_SETTING = Setting.listSetting("user_dn_templates",
            Collections.emptyList(), Function.identity(), Setting.Property.NodeScope);

    private final String[] userDnTemplates;
    private final GroupsResolver groupResolver;
    private final LdapMetaDataResolver metaDataResolver;

    public LdapSessionFactory(RealmConfig config, SSLService sslService) {
        super(config, sslService);
        Settings settings = config.settings();
        userDnTemplates = USER_DN_TEMPLATES_SETTING.get(settings).toArray(Strings.EMPTY_ARRAY);
        if (userDnTemplates.length == 0) {
            throw new IllegalArgumentException("missing required LDAP setting ["
                    + RealmSettings.getFullSettingKey(config, USER_DN_TEMPLATES_SETTING) + "]");
        }
        logger.info("Realm [{}] is in user-dn-template mode: [{}]", config.name(), userDnTemplates);
        groupResolver = groupResolver(settings);
        metaDataResolver = new LdapMetaDataResolver(settings, ignoreReferralErrors);
    }

    /**
     * This iterates through the configured user templates attempting to open.  If all attempts fail, the last exception
     * is kept as the cause of the thrown exception
     *
     * @param username a relative name, Not a distinguished name, that will be inserted into the template.
     */
    @Override
    public void session(String username, SecureString password, ActionListener<LdapSession> listener) {
        LDAPException lastException = null;
        LDAPConnection connection = null;
        LdapSession ldapSession = null;
        final byte[] passwordBytes = CharArrays.toUtf8Bytes(password.getChars());
        boolean success = false;
        try {
            connection = LdapUtils.privilegedConnect(serverSet::getConnection);
            for (String template : userDnTemplates) {
                String dn = buildDnFromTemplate(username, template);
                try {
                    connection.bind(new SimpleBindRequest(dn, passwordBytes));
                    ldapSession = new LdapSession(logger, config, connection, dn, groupResolver, metaDataResolver, timeout, null);
                    success = true;
                    break;
                } catch (LDAPException e) {
                    // we catch the ldapException here since we expect it can happen and we shouldn't be logging this all the time otherwise
                    // it is just noise
                    logger.trace((Supplier<?>) () -> new ParameterizedMessage(
                            "failed LDAP authentication with user template [{}] and DN [{}]", template, dn), e);
                    if (lastException == null) {
                        lastException = e;
                    } else {
                        lastException.addSuppressed(e);
                    }
                }
            }
        } catch (LDAPException e) {
            assert lastException == null : "if we catch a LDAPException here, we should have never seen another exception";
            assert ldapSession == null : "LDAPSession should not have been established due to a connection failure";
            lastException = e;
        } finally {
            Arrays.fill(passwordBytes, (byte) 0);
            if (success == false) {
                IOUtils.closeWhileHandlingException(connection);
            }
        }

        if (ldapSession != null) {
            listener.onResponse(ldapSession);
        } else {
            assert lastException != null : "if there is not LDAPSession, then we must have a exception";
            listener.onFailure(lastException);
        }
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
        if (SearchGroupsResolver.BASE_DN.exists(settings)) {
            return new SearchGroupsResolver(settings);
        }
        return new UserAttributeGroupsResolver(settings);
    }

    public static Set<Setting<?>> getSettings() {
        Set<Setting<?>> settings = new HashSet<>();
        settings.addAll(SessionFactory.getSettings());
        settings.add(USER_DN_TEMPLATES_SETTING);
        return settings;
    }
}

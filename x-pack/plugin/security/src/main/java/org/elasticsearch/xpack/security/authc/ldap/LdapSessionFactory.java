/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.SimpleBindRequest;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.CharArrays;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapSessionFactorySettings;
import org.elasticsearch.xpack.core.security.authc.ldap.SearchGroupsResolverSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession.GroupsResolver;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils;
import org.elasticsearch.xpack.security.authc.ldap.support.SessionFactory;

import java.text.MessageFormat;
import java.util.Locale;

import static org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils.escapedRDNValue;

/**
 * This factory creates LDAP connections via iterating through user templates.
 *
 * Note that even though there is a separate factory for Active Directory, this factory would work against AD.  A template
 * for each user context would need to be supplied.
 */
public class LdapSessionFactory extends SessionFactory {

    private final String[] userDnTemplates;
    private final GroupsResolver groupResolver;

    public LdapSessionFactory(RealmConfig config, SSLService sslService, ThreadPool threadPool) {
        super(config, sslService, threadPool);
        userDnTemplates = config.getSetting(LdapSessionFactorySettings.USER_DN_TEMPLATES_SETTING).toArray(Strings.EMPTY_ARRAY);
        if (userDnTemplates.length == 0) {
            throw new IllegalArgumentException("missing required LDAP setting ["
                    + RealmSettings.getFullSettingKey(config, LdapSessionFactorySettings.USER_DN_TEMPLATES_SETTING) + "]");
        }
        logger.info("Realm [{}] is in user-dn-template mode: [{}]", config.name(), userDnTemplates);
        groupResolver = groupResolver(config);
    }

    /**
     * This iterates through the configured user templates attempting to open.  If all attempts fail, the last exception
     * is kept as the cause of the thrown exception
     *
     * @param username a relative name, Not a distinguished name, that will be inserted into the template.
     */
    @Override
    public void session(String username, SecureString password, ActionListener<LdapSession> listener) {
        try {
            new AbstractRunnable() {
                final LDAPConnection connection = LdapUtils.privilegedConnect(serverSet::getConnection);
                final byte[] passwordBytes = CharArrays.toUtf8Bytes(password.getChars());
                Exception containerException = null;
                int loopIndex = 0;

                @Override
                protected void doRun() throws Exception {
                    listener.onResponse(
                            (new LdapSession(logger, config, connection, ((SimpleBindRequest) connection.getLastBindRequest()).getBindDN(),
                                    groupResolver, metadataResolver, timeout, null)));
                }

                @Override
                public void onFailure(Exception e) {
                    // record failure
                    if (containerException == null) {
                        containerException = e;
                    } else {
                        containerException.addSuppressed(e);
                    }

                    if (loopIndex > userDnTemplates.length) {
                        listener.onFailure(new IllegalStateException("User DN template iteration index out of bounds."));
                    } else if (loopIndex == userDnTemplates.length) {
                        // loop break
                        IOUtils.closeWhileHandlingException(connection);
                        listener.onFailure(containerException);
                    } else {
                        loop();
                    }
                }

                // loop body
                void loop() {
                    final String template = userDnTemplates[loopIndex++];
                    final SimpleBindRequest bind = new SimpleBindRequest(buildDnFromTemplate(username, template), passwordBytes);
                    LdapUtils.maybeForkThenBind(connection, bind, threadPool, this);
                }
            }.loop();
        } catch (LDAPException e) {
            listener.onFailure(e);
        }
    }

    /**
     * Securely escapes the username and inserts it into the template using MessageFormat
     *
     * @param username username to insert into the DN template.  Any commas, equals or plus will be escaped.
     * @return DN (distinguished name) build from the template.
     */
    String buildDnFromTemplate(String username, String template) {
        //this value must be escaped to avoid manipulation of the template DN.
        String escapedUsername = escapedRDNValue(username);
        return new MessageFormat(template, Locale.ROOT).format(new Object[] { escapedUsername }, new StringBuffer(), null).toString();
    }

    static GroupsResolver groupResolver(RealmConfig realmConfig) {
        if (realmConfig.hasSetting(SearchGroupsResolverSettings.BASE_DN)) {
            return new SearchGroupsResolver(realmConfig);
        }
        return new UserAttributeGroupsResolver(realmConfig);
    }

}

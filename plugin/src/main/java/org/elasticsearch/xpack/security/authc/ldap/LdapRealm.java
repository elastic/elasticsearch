/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import com.unboundid.ldap.sdk.LDAPException;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapLoadBalancing;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession;
import org.elasticsearch.xpack.security.authc.RealmSettings;
import org.elasticsearch.xpack.security.authc.ldap.support.SessionFactory;
import org.elasticsearch.xpack.security.authc.support.CachingUsernamePasswordRealm;
import org.elasticsearch.xpack.security.authc.support.DnRoleMapper;
import org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.user.User;
import org.elasticsearch.xpack.ssl.SSLService;


/**
 * Authenticates username/password tokens against ldap, locates groups and maps them to roles.
 */
public final class LdapRealm extends CachingUsernamePasswordRealm {

    public static final String LDAP_TYPE = "ldap";
    public static final String AD_TYPE = "active_directory";

    private final SessionFactory sessionFactory;
    private final DnRoleMapper roleMapper;
    private final ThreadPool threadPool;

    public LdapRealm(String type, RealmConfig config, ResourceWatcherService watcherService, SSLService sslService,
                     ThreadPool threadPool) throws LDAPException {
        this(type, config, sessionFactory(config, sslService, type), new DnRoleMapper(type, config, watcherService), threadPool);
    }

    // pkg private for testing
    LdapRealm(String type, RealmConfig config, SessionFactory sessionFactory, DnRoleMapper roleMapper, ThreadPool threadPool) {
        super(type, config);
        this.sessionFactory = sessionFactory;
        this.roleMapper = roleMapper;
        this.threadPool = threadPool;
        roleMapper.addListener(this::expireAll);
    }

    static SessionFactory sessionFactory(RealmConfig config, SSLService sslService, String type) throws LDAPException {
        final SessionFactory sessionFactory;
        if (AD_TYPE.equals(type)) {
            sessionFactory = new ActiveDirectorySessionFactory(config, sslService);
        } else {
            assert LDAP_TYPE.equals(type) : "type [" + type + "] is unknown. expected one of [" + AD_TYPE + ", " + LDAP_TYPE + "]";
            final boolean hasSearchSettings = LdapUserSearchSessionFactory.hasUserSearchSettings(config);
            final boolean hasTemplates = LdapSessionFactory.USER_DN_TEMPLATES_SETTING.exists(config.settings());
            if (hasSearchSettings == false) {
                if(hasTemplates == false) {
                    throw new IllegalArgumentException("settings were not found for either user search [" +
                            RealmSettings.getFullSettingKey(config, LdapUserSearchSessionFactory.SEARCH_PREFIX) +
                            "] or user template [" +
                            RealmSettings.getFullSettingKey(config, LdapSessionFactory.USER_DN_TEMPLATES_SETTING) +
                            "] modes of operation. " +
                            "Please provide the settings for the mode you wish to use. For more details refer to the ldap " +
                            "authentication section of the X-Pack guide.");
                }
                sessionFactory = new LdapSessionFactory(config, sslService);
            } else if (hasTemplates) {
                throw new IllegalArgumentException("settings were found for both user search [" +
                        RealmSettings.getFullSettingKey(config, LdapUserSearchSessionFactory.SEARCH_PREFIX) +
                        "] and user template [" +
                        RealmSettings.getFullSettingKey(config, LdapSessionFactory.USER_DN_TEMPLATES_SETTING) +
                        "] modes of operation. " +
                        "Please remove the settings for the mode you do not wish to use. For more details refer to the ldap " +
                        "authentication section of the X-Pack guide.");
            } else {
                sessionFactory = new LdapUserSearchSessionFactory(config, sslService);
            }
        }
        return sessionFactory;
    }

    /**
     * @return The {@link Setting setting configuration} for this realm type
     * @param type Either {@link #AD_TYPE} or {@link #LDAP_TYPE}
     */
    public static Set<Setting<?>> getSettings(String type) {
        Set<Setting<?>> settings = new HashSet<>();
        settings.addAll(CachingUsernamePasswordRealm.getCachingSettings());
        DnRoleMapper.getSettings(settings);
        if (AD_TYPE.equals(type)) {
            settings.addAll(ActiveDirectorySessionFactory.getSettings());
        } else {
            assert LDAP_TYPE.equals(type) : "type [" + type + "] is unknown. expected one of [" + AD_TYPE + ", " + LDAP_TYPE + "]";
            settings.addAll(LdapSessionFactory.getSettings());
            settings.addAll(LdapUserSearchSessionFactory.getSettings());
        }
        return settings;
    }

    /**
     * Given a username and password, open a connection to ldap, bind to authenticate, retrieve groups, map to roles and build the user.
     * This user will then be passed to the listener
     */
    @Override
    protected void doAuthenticate(UsernamePasswordToken token, ActionListener<User> listener) {
        // we submit to the threadpool because authentication using LDAP will execute blocking I/O for a bind request and we don't want
        // network threads stuck waiting for a socket to connect. After the bind, then all interaction with LDAP should be async
        threadPool.generic().execute(() -> sessionFactory.session(token.principal(), token.credentials(),
                    new LdapSessionActionListener("authenticate", token.principal(), listener, roleMapper, logger)));
    }

    @Override
    protected void doLookupUser(String username, ActionListener<User> listener) {
        if (sessionFactory.supportsUnauthenticatedSession()) {
            // we submit to the threadpool because authentication using LDAP will execute blocking I/O for a bind request and we don't want
            // network threads stuck waiting for a socket to connect. After the bind, then all interaction with LDAP should be async
            threadPool.generic().execute(() ->
                sessionFactory.unauthenticatedSession(username,
                        new LdapSessionActionListener("lookup", username, listener, roleMapper, logger)));
        } else {
            listener.onResponse(null);
        }
    }

    @Override
    public Map<String, Object> usageStats() {
        Map<String, Object> usage = super.usageStats();
        usage.put("load_balance_type", LdapLoadBalancing.resolve(config.settings()).toString());
        usage.put("ssl", sessionFactory.isSslUsed());
        usage.put("user_search", LdapUserSearchSessionFactory.hasUserSearchSettings(config));
        return usage;
    }

    private static void lookupGroups(LdapSession session, String username, ActionListener<User> listener, DnRoleMapper roleMapper) {
        if (session == null) {
            listener.onResponse(null);
        } else {
            boolean loadingGroups = false;
            try {
                session.groups(ActionListener.wrap((groups) -> {
                            Set<String> roles = roleMapper.resolveRoles(session.userDn(), groups);
                            IOUtils.close(session);
                            listener.onResponse(new User(username, roles.toArray(Strings.EMPTY_ARRAY)));
                        },
                        (e) -> {
                            IOUtils.closeWhileHandlingException(session);
                            listener.onFailure(e);
                        }));
                loadingGroups = true;
            } finally {
                if (loadingGroups == false) {
                    session.close();
                }
            }
        }
    }


    /**
     * A special {@link ActionListener} that encapsulates the handling of a LdapSession, which is used to return a user. This class handles
     * cases where the session is null or where an exception may be caught after a session has been established, which requires the
     * closing of the session.
     */
    private static class LdapSessionActionListener implements ActionListener<LdapSession> {

        private final AtomicReference<LdapSession> ldapSessionAtomicReference = new AtomicReference<>();
        private String action;
        private Logger logger;
        private final String username;
        private final ActionListener<User> userActionListener;
        private final DnRoleMapper roleMapper;

        LdapSessionActionListener(String action, String username, ActionListener<User> userActionListener,
                                  DnRoleMapper roleMapper, Logger logger) {
            this.action = action;
            this.username = username;
            this.userActionListener = userActionListener;
            this.roleMapper = roleMapper;
            this.logger = logger;
        }

        @Override
        public void onResponse(LdapSession session) {
            if (session == null) {
                userActionListener.onResponse(null);
            } else {
                ldapSessionAtomicReference.set(session);
                lookupGroups(session, username, userActionListener, roleMapper);
            }
        }

        @Override
        public void onFailure(Exception e) {
            if (ldapSessionAtomicReference.get() != null) {
                IOUtils.closeWhileHandlingException(ldapSessionAtomicReference.get());
            }
            logger.info("{} failed for user [{}]: {}", action, username, e.getMessage());
            if (logger.isDebugEnabled()) {
                logger.debug(new ParameterizedMessage("{} failed", action), e);
            }
            userActionListener.onResponse(null);
        }

    }
}

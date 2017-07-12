/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import com.unboundid.ldap.sdk.Filter;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPConnectionPool;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.LDAPInterface;
import com.unboundid.ldap.sdk.SearchResultEntry;
import com.unboundid.ldap.sdk.SimpleBindRequest;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.authc.RealmSettings;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession.GroupsResolver;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils;
import org.elasticsearch.xpack.security.authc.ldap.support.SessionFactory;
import org.elasticsearch.xpack.security.authc.support.CharArrays;
import org.elasticsearch.xpack.ssl.SSLService;

import java.util.Arrays;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils.attributesToSearchFor;
import static org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils.createFilter;
import static org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils.searchForEntry;

class LdapUserSearchSessionFactory extends PoolingSessionFactory {

    private static final String DEFAULT_USERNAME_ATTRIBUTE = "uid";

    static final String SEARCH_PREFIX = "user_search.";
    static final Setting<String> SEARCH_ATTRIBUTE = new Setting<>("user_search.attribute", DEFAULT_USERNAME_ATTRIBUTE,
            Function.identity(), Setting.Property.NodeScope, Setting.Property.Deprecated);

    private static final Setting<String> SEARCH_BASE_DN = Setting.simpleString("user_search.base_dn", Setting.Property.NodeScope);
    private static final Setting<String> SEARCH_FILTER = Setting.simpleString("user_search.filter", Setting.Property.NodeScope);
    private static final Setting<LdapSearchScope> SEARCH_SCOPE = new Setting<>("user_search.scope", (String) null,
            s -> LdapSearchScope.resolve(s, LdapSearchScope.SUB_TREE), Setting.Property.NodeScope);
    private static final Setting<Boolean> POOL_ENABLED = Setting.boolSetting("user_search.pool.enabled", true, Setting.Property.NodeScope);

    private final String userSearchBaseDn;
    private final LdapSearchScope scope;
    private final String searchFilter;

    LdapUserSearchSessionFactory(RealmConfig config, SSLService sslService) throws LDAPException {
        super(config, sslService, groupResolver(config.settings()), POOL_ENABLED,
                () -> LdapUserSearchSessionFactory.bindRequest(config.settings()),
                () -> {
                    if (BIND_DN.exists(config.settings())) {
                        return BIND_DN.get(config.settings());
                    } else {
                        return SEARCH_BASE_DN.get(config.settings());
                    }
                });
        Settings settings = config.settings();
        if (SEARCH_BASE_DN.exists(settings)) {
            userSearchBaseDn = SEARCH_BASE_DN.get(settings);
        } else {
            throw new IllegalArgumentException("[" + RealmSettings.getFullSettingKey(config, SEARCH_BASE_DN) + "] must be specified");
        }
        scope = SEARCH_SCOPE.get(settings);
        searchFilter = getSearchFilter(config);
        logger.info("Realm [{}] is in user-search mode - base_dn=[{}], search filter=[{}]",
                config.name(), userSearchBaseDn, searchFilter);
    }

    static SimpleBindRequest bindRequest(Settings settings) {
        if (BIND_DN.exists(settings)) {
            return new SimpleBindRequest(BIND_DN.get(settings), BIND_PASSWORD.get(settings));
        } else {
            return new SimpleBindRequest();
        }
    }

    static boolean hasUserSearchSettings(RealmConfig config) {
        return config.settings().getByPrefix("user_search.").isEmpty() == false;
    }

    /**
     * Sets up a LDAPSession using the connection pool that potentially holds existing connections to the server
     */
    @Override
    void getSessionWithPool(LDAPConnectionPool connectionPool, String user, SecureString password, ActionListener<LdapSession> listener) {
        findUser(user, connectionPool, ActionListener.wrap((entry) -> {
            if (entry == null) {
                listener.onResponse(null);
            } else {
                final String dn = entry.getDN();
                final byte[] passwordBytes = CharArrays.toUtf8Bytes(password.getChars());
                try {
                    LdapUtils.privilegedConnect(() -> connectionPool.bindAndRevertAuthentication(new SimpleBindRequest(dn, passwordBytes)));
                    listener.onResponse(new LdapSession(logger, config, connectionPool, dn, groupResolver, metaDataResolver, timeout,
                            entry.getAttributes()));
                } catch (LDAPException e) {
                    listener.onFailure(e);
                } finally {
                    Arrays.fill(passwordBytes, (byte) 0);
                }
            }
        }, listener::onFailure));
    }

    /**
     * Sets up a LDAPSession using the following process:
     * <ol>
     *     <li>Opening a new connection to the LDAP server</li>
     *     <li>Executes a bind request using the bind user</li>
     *     <li>Executes a search to find the DN of the user</li>
     *     <li>Closes the opened connection</li>
     *     <li>Opens a new connection to the LDAP server</li>
     *     <li>Executes a bind request using the found DN and provided password</li>
     *     <li>Creates a new LDAPSession with the bound connection</li>
     * </ol>
     */
    @Override
    void getSessionWithoutPool(String user, SecureString password, ActionListener<LdapSession> listener) {
        boolean success = false;
        LDAPConnection connection = null;
        try {
            connection = LdapUtils.privilegedConnect(serverSet::getConnection);
            final SimpleBindRequest bind = bindRequest(config.settings());
            connection.bind(bind);
            final LDAPConnection finalConnection = connection;
            findUser(user, connection, ActionListener.wrap((entry) -> {
                        // close the existing connection since we are executing in this handler of the previous request and cannot bind here
                        // so we need to open a new connection to bind on and use for the session
                        IOUtils.close(finalConnection);
                        if (entry == null) {
                            listener.onResponse(null);
                        } else {
                            final String dn = entry.getDN();
                            boolean sessionCreated = false;
                            LDAPConnection userConnection = null;
                            final byte[] passwordBytes = CharArrays.toUtf8Bytes(password.getChars());
                            try {
                                userConnection = LdapUtils.privilegedConnect(serverSet::getConnection);
                                userConnection.bind(new SimpleBindRequest(dn, passwordBytes));
                                LdapSession session = new LdapSession(logger, config, userConnection, dn, groupResolver,
                                        metaDataResolver, timeout, entry.getAttributes());
                                sessionCreated = true;
                                listener.onResponse(session);
                            } catch (Exception e) {
                                listener.onFailure(e);
                            } finally {
                                Arrays.fill(passwordBytes, (byte) 0);
                                if (sessionCreated == false) {
                                    IOUtils.close(userConnection);
                                }
                            }
                        }
                    },
                    (e) -> {
                        IOUtils.closeWhileHandlingException(finalConnection);
                        listener.onFailure(e);
                    }));
            success = true;
        } catch (LDAPException e) {
            listener.onFailure(e);
        } finally {
            // need the success flag since the search is async and we don't want to close it if it is in progress
            if (success == false) {
                IOUtils.closeWhileHandlingException(connection);
            }
        }
    }

    @Override
    public boolean supportsUnauthenticatedSession() {
        return true;
    }

    @Override
    void getUnauthenticatedSessionWithPool(LDAPConnectionPool connectionPool, String user, ActionListener<LdapSession> listener) {
        findUser(user, connectionPool, ActionListener.wrap((entry) -> {
            if (entry == null) {
                listener.onResponse(null);
            } else {
                final String dn = entry.getDN();
                LdapSession session = new LdapSession(logger, config, connectionPool, dn, groupResolver, metaDataResolver, timeout,
                        entry.getAttributes());
                listener.onResponse(session);
            }
        }, listener::onFailure));
    }

    @Override
    void getUnauthenticatedSessionWithoutPool(String user, ActionListener<LdapSession> listener) {
        LDAPConnection connection = null;
        boolean success = false;
        try {
            connection = LdapUtils.privilegedConnect(serverSet::getConnection);
            connection.bind(bindRequest(config.settings()));
            final LDAPConnection finalConnection = connection;

            findUser(user, finalConnection, ActionListener.wrap((entry) -> {
                if (entry == null) {
                    listener.onResponse(null);
                } else {
                    boolean sessionCreated = false;
                    try {
                        final String dn = entry.getDN();
                        LdapSession session = new LdapSession(logger, config, finalConnection, dn, groupResolver, metaDataResolver, timeout,
                                entry.getAttributes());
                        sessionCreated = true;
                        listener.onResponse(session);
                    } finally {
                        if (sessionCreated == false) {
                            IOUtils.close(finalConnection);
                        }
                    }
                }
            }, listener::onFailure));
            success = true;
        } catch (LDAPException e) {
            listener.onFailure(e);
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(connection);
            }
        }
    }

    private void findUser(String user, LDAPInterface ldapInterface, ActionListener<SearchResultEntry> listener) {
        final Filter filter;
        try {
            filter = createFilter(searchFilter, user);
        } catch (LDAPException e) {
            listener.onFailure(e);
            return;
        }

        searchForEntry(ldapInterface, userSearchBaseDn, scope.scope(),
                filter, Math.toIntExact(timeout.seconds()), ignoreReferralErrors, listener,
                attributesToSearchFor(groupResolver.attributes(), metaDataResolver.attributeNames()));
    }

    private static GroupsResolver groupResolver(Settings settings) {
        if (SearchGroupsResolver.BASE_DN.exists(settings)) {
            return new SearchGroupsResolver(settings);
        }
        return new UserAttributeGroupsResolver(settings);
    }

    static String getSearchFilter(RealmConfig config) {
        final Settings settings = config.settings();
        final boolean hasAttribute = SEARCH_ATTRIBUTE.exists(settings);
        final boolean hasFilter = SEARCH_FILTER.exists(settings);
        if (hasAttribute && hasFilter) {
            throw new IllegalArgumentException("search attribute setting [" +
                    RealmSettings.getFullSettingKey(config, SEARCH_ATTRIBUTE) + "] and filter setting [" +
                    RealmSettings.getFullSettingKey(config, SEARCH_FILTER) + "] cannot be combined!");
        } else if (hasFilter) {
            return SEARCH_FILTER.get(settings);
        } else if (hasAttribute) {
            return "(" + SEARCH_ATTRIBUTE.get(settings) + "={0})";
        } else {
            return "(uid={0})";
        }
    }

    public static Set<Setting<?>> getSettings() {
        Set<Setting<?>> settings = new HashSet<>();
        settings.addAll(SessionFactory.getSettings());
        settings.addAll(PoolingSessionFactory.getSettings());
        settings.add(SEARCH_BASE_DN);
        settings.add(SEARCH_SCOPE);
        settings.add(SEARCH_ATTRIBUTE);
        settings.add(POOL_ENABLED);
        settings.add(SEARCH_FILTER);

        settings.addAll(SearchGroupsResolver.getSettings());
        settings.addAll(UserAttributeGroupsResolver.getSettings());

        return settings;
    }
}

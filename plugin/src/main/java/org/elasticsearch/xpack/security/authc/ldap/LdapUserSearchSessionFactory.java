/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import com.unboundid.ldap.sdk.GetEntryLDAPConnectionPoolHealthCheck;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPConnectionPool;
import com.unboundid.ldap.sdk.LDAPConnectionPoolHealthCheck;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.LDAPInterface;
import com.unboundid.ldap.sdk.SearchResultEntry;
import com.unboundid.ldap.sdk.ServerSet;
import com.unboundid.ldap.sdk.SimpleBindRequest;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.authc.RealmSettings;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession.GroupsResolver;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils;
import org.elasticsearch.xpack.security.authc.ldap.support.SessionFactory;
import org.elasticsearch.xpack.security.authc.support.CharArrays;
import org.elasticsearch.xpack.security.authc.support.SecuredString;
import org.elasticsearch.xpack.ssl.SSLService;

import java.util.Arrays;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.unboundid.ldap.sdk.Filter.createEqualityFilter;
import static com.unboundid.ldap.sdk.Filter.encodeValue;
import static org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils.attributesToSearchFor;
import static org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils.searchForEntry;

class LdapUserSearchSessionFactory extends SessionFactory {

    static final int DEFAULT_CONNECTION_POOL_SIZE = 20;
    static final int DEFAULT_CONNECTION_POOL_INITIAL_SIZE = 0;
    static final String DEFAULT_USERNAME_ATTRIBUTE = "uid";
    static final TimeValue DEFAULT_HEALTH_CHECK_INTERVAL = TimeValue.timeValueSeconds(60L);

    static final String SEARCH_PREFIX = "user_search.";

    private static final Setting<String> SEARCH_BASE_DN = Setting.simpleString("user_search.base_dn", Setting.Property.NodeScope);
    private static final Setting<String> SEARCH_ATTRIBUTE = new Setting<>("user_search.attribute", DEFAULT_USERNAME_ATTRIBUTE,
            Function.identity(), Setting.Property.NodeScope);
    private static final Setting<LdapSearchScope> SEARCH_SCOPE = new Setting<>("user_search.scope", (String) null,
            s -> LdapSearchScope.resolve(s, LdapSearchScope.SUB_TREE), Setting.Property.NodeScope);

    private static final Setting<Boolean> POOL_ENABLED = Setting.boolSetting("user_search.pool.enabled",
            true, Setting.Property.NodeScope);
    private static final Setting<Integer> POOL_INITIAL_SIZE = Setting.intSetting("user_search.pool.initial_size",
            DEFAULT_CONNECTION_POOL_INITIAL_SIZE, 0, Setting.Property.NodeScope);
    private static final Setting<Integer> POOL_SIZE = Setting.intSetting("user_search.pool.size",
            DEFAULT_CONNECTION_POOL_SIZE, 1, Setting.Property.NodeScope);
    private static final Setting<TimeValue> HEALTH_CHECK_INTERVAL = Setting.timeSetting("user_search.pool.health_check.interval",
            DEFAULT_HEALTH_CHECK_INTERVAL, Setting.Property.NodeScope);
    private static final Setting<Boolean> HEALTH_CHECK_ENABLED = Setting.boolSetting("user_search.pool.health_check.enabled",
            true, Setting.Property.NodeScope);
    private static final Setting<Optional<String>> HEALTH_CHECK_DN = new Setting<>("user_search.pool.health_check.dn", (String) null,
            Optional::ofNullable, Setting.Property.NodeScope);

    private static final Setting<String> BIND_DN = Setting.simpleString("bind_dn", Setting.Property.NodeScope);
    private static final Setting<String> BIND_PASSWORD = Setting.simpleString("bind_password", Setting.Property.NodeScope);

    private final String userSearchBaseDn;
    private final LdapSearchScope scope;
    private final String userAttribute;
    private final GroupsResolver groupResolver;
    private final boolean useConnectionPool;

    private final LDAPConnectionPool connectionPool;

    LdapUserSearchSessionFactory(RealmConfig config, SSLService sslService) throws LDAPException {
        super(config, sslService);
        Settings settings = config.settings();
        if (SEARCH_BASE_DN.exists(settings)) {
            userSearchBaseDn = SEARCH_BASE_DN.get(settings);
        } else {
            throw new IllegalArgumentException("[" + RealmSettings.getFullSettingKey(config, SEARCH_BASE_DN) + "] must be specified");
        }
        scope = SEARCH_SCOPE.get(settings);
        userAttribute = SEARCH_ATTRIBUTE.get(settings);
        groupResolver = groupResolver(settings);
        useConnectionPool = POOL_ENABLED.get(settings);
        if (useConnectionPool) {
            connectionPool = createConnectionPool(config, serverSet, timeout, logger);
        } else {
            connectionPool = null;
        }
    }

    static LDAPConnectionPool createConnectionPool(RealmConfig config, ServerSet serverSet, TimeValue timeout, Logger logger)
                                                    throws LDAPException {
        Settings settings = config.settings();
        SimpleBindRequest bindRequest = bindRequest(settings);
        final int initialSize = POOL_INITIAL_SIZE.get(settings);
        final int size = POOL_SIZE.get(settings);
        LDAPConnectionPool pool = null;
        boolean success = false;
        try {
            pool = LdapUtils.privilegedConnect(() -> new LDAPConnectionPool(serverSet, bindRequest, initialSize, size));
            pool.setRetryFailedOperationsDueToInvalidConnections(true);
            if (HEALTH_CHECK_ENABLED.get(settings)) {
                String entryDn = HEALTH_CHECK_DN.get(settings).orElseGet(() -> bindRequest == null ? null : bindRequest.getBindDN());
                final long healthCheckInterval = HEALTH_CHECK_INTERVAL.get(settings).millis();
                if (entryDn != null) {
                    // Checks the status of the LDAP connection at a specified interval in the background. We do not check on
                    // on create as the LDAP server may require authentication to get an entry and a bind request has not been executed
                    // yet so we could end up never getting a connection. We do not check on checkout as we always set retry operations
                    // and the pool will handle a bad connection without the added latency on every operation
                    LDAPConnectionPoolHealthCheck healthCheck = new GetEntryLDAPConnectionPoolHealthCheck(entryDn, timeout.millis(),
                            false, false, false, true, false);
                    pool.setHealthCheck(healthCheck);
                    pool.setHealthCheckIntervalMillis(healthCheckInterval);
                } else {
                    logger.warn("[" + RealmSettings.getFullSettingKey(config, BIND_DN) + "] and [" +
                            RealmSettings.getFullSettingKey(config, HEALTH_CHECK_DN) + "] have not been specified so no " +
                            "ldap query will be run as a health check");
                }
            }

            success = true;
            return pool;
        } finally {
            if (success == false && pool != null) {
                pool.close();
            }
        }
    }

    static SimpleBindRequest bindRequest(Settings settings) {
        SimpleBindRequest request = null;
        if (BIND_DN.exists(settings)) {
            request = new SimpleBindRequest(BIND_DN.get(settings), BIND_PASSWORD.get(settings));
        }
        return request;
    }

    public static boolean hasUserSearchSettings(RealmConfig config) {
        return config.settings().getByPrefix("user_search.").isEmpty() == false;
    }

    @Override
    public void session(String user, SecuredString password, ActionListener<LdapSession> listener) {
        if (useConnectionPool) {
            getSessionWithPool(user, password, listener);
        } else {
            getSessionWithoutPool(user, password, listener);
        }
    }

    /**
     * Sets up a LDAPSession using the connection pool that potentially holds existing connections to the server
     */
    private void getSessionWithPool(String user, SecuredString password, ActionListener<LdapSession> listener) {
        findUser(user, connectionPool, ActionListener.wrap((entry) -> {
            if (entry == null) {
                listener.onResponse(null);
            } else {
                final String dn = entry.getDN();
                final byte[] passwordBytes = CharArrays.toUtf8Bytes(password.internalChars());
                try {
                    LdapUtils.privilegedConnect(() -> connectionPool.bindAndRevertAuthentication(new SimpleBindRequest(dn, passwordBytes)));
                    listener.onResponse(new LdapSession(logger, connectionPool, dn, groupResolver, timeout, entry.getAttributes()));
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
    private void getSessionWithoutPool(String user, SecuredString password, ActionListener<LdapSession> listener) {
        boolean success = false;
        LDAPConnection connection = null;
        try {
            connection = LdapUtils.privilegedConnect(serverSet::getConnection);
            connection.bind(bindRequest(config.settings()));
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
                            final byte[] passwordBytes = CharArrays.toUtf8Bytes(password.internalChars());
                            try {
                                userConnection = LdapUtils.privilegedConnect(serverSet::getConnection);
                                userConnection.bind(new SimpleBindRequest(dn, passwordBytes));
                                LdapSession session = new LdapSession(logger, userConnection, dn, groupResolver, timeout,
                                        entry.getAttributes());
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
    public void unauthenticatedSession(String user, ActionListener<LdapSession> listener) {
        LDAPConnection connection = null;
        boolean success = false;
        try {
            final LDAPInterface ldapInterface;
            if (useConnectionPool) {
                ldapInterface = connectionPool;
            } else {
                connection = LdapUtils.privilegedConnect(serverSet::getConnection);
                connection.bind(bindRequest(config.settings()));
                ldapInterface = connection;
            }

            findUser(user, ldapInterface, ActionListener.wrap((entry) -> {
                if (entry == null) {
                    listener.onResponse(null);
                } else {
                    boolean sessionCreated = false;
                    try {
                        final String dn = entry.getDN();
                        LdapSession session = new LdapSession(logger, ldapInterface, dn, groupResolver, timeout, entry.getAttributes());
                        sessionCreated = true;
                        listener.onResponse(session);
                    } finally {
                        if (sessionCreated == false && useConnectionPool == false) {
                            IOUtils.close((LDAPConnection) ldapInterface);
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
        searchForEntry(ldapInterface, userSearchBaseDn, scope.scope(),
                createEqualityFilter(userAttribute, encodeValue(user)),
                Math.toIntExact(timeout.seconds()), ignoreReferralErrors, listener,
                attributesToSearchFor(groupResolver.attributes()));
    }

    /*
     * This method is used to cleanup the connections
     */
    void shutdown() {
        if (connectionPool != null) {
            connectionPool.close();
        }
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
        settings.add(SEARCH_BASE_DN);
        settings.add(SEARCH_SCOPE);
        settings.add(SEARCH_ATTRIBUTE);
        settings.add(POOL_ENABLED);
        settings.add(POOL_INITIAL_SIZE);
        settings.add(POOL_SIZE);
        settings.add(HEALTH_CHECK_ENABLED);
        settings.add(HEALTH_CHECK_DN);
        settings.add(HEALTH_CHECK_INTERVAL);
        settings.add(BIND_DN);
        settings.add(BIND_PASSWORD);

        settings.addAll(SearchGroupsResolver.getSettings());
        settings.addAll(UserAttributeGroupsResolver.getSettings());

        return settings;
    }
}

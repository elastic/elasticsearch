/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import com.unboundid.ldap.sdk.GetEntryLDAPConnectionPoolHealthCheck;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPConnectionPool;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.SearchRequest;
import com.unboundid.ldap.sdk.SearchResultEntry;
import com.unboundid.ldap.sdk.ServerSet;
import com.unboundid.ldap.sdk.SimpleBindRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.shield.authc.ldap.support.LdapSession;
import org.elasticsearch.shield.authc.ldap.support.LdapSession.GroupsResolver;
import org.elasticsearch.shield.authc.ldap.support.SessionFactory;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.ssl.ClientSSLService;
import org.elasticsearch.shield.support.Exceptions;

import java.io.IOException;
import java.util.Locale;

import static com.unboundid.ldap.sdk.Filter.createEqualityFilter;
import static com.unboundid.ldap.sdk.Filter.encodeValue;
import static org.elasticsearch.shield.authc.ldap.support.LdapUtils.searchForEntry;

public class LdapUserSearchSessionFactory extends SessionFactory {

    static final int DEFAULT_CONNECTION_POOL_SIZE = 20;
    static final int DEFAULT_CONNECTION_POOL_INITIAL_SIZE = 5;
    static final String DEFAULT_USERNAME_ATTRIBUTE = "uid";
    static final TimeValue DEFAULT_HEALTH_CHECK_INTERVAL = TimeValue.timeValueSeconds(60L);

    private final String userSearchBaseDn;
    private final LdapSearchScope scope;
    private final String userAttribute;

    private LDAPConnectionPool connectionPool;
    private GroupsResolver groupResolver;

    public LdapUserSearchSessionFactory(RealmConfig config, ClientSSLService sslService) {
        super(config, sslService);
        Settings settings = config.settings();
        userSearchBaseDn = settings.get("user_search.base_dn");
        if (userSearchBaseDn == null) {
            throw new IllegalArgumentException("user_search base_dn must be specified");
        }
        scope = LdapSearchScope.resolve(settings.get("user_search.scope"), LdapSearchScope.SUB_TREE);
        userAttribute = settings.get("user_search.attribute", DEFAULT_USERNAME_ATTRIBUTE);
    }

    @Override
    public LdapUserSearchSessionFactory init() {
        super.init();
        connectionPool = createConnectionPool(config, serverSet, timeout, logger);
        groupResolver = groupResolver(config.settings());
        return this;
    }

    private synchronized LDAPConnectionPool connectionPool() throws IOException {
        if (connectionPool == null) {
            connectionPool = createConnectionPool(config, serverSet, timeout, logger);
            // if it is still null throw an exception
            if (connectionPool == null) {
                String msg = "failed to create a connection pool for realm [" + config.name() + "] as no LDAP servers are available";
                throw new IOException(msg);
            }
        }

        return connectionPool;
    }

    static LDAPConnectionPool createConnectionPool(RealmConfig config, ServerSet serverSet, TimeValue timeout, ESLogger logger) {
        Settings settings = config.settings();
        SimpleBindRequest bindRequest = bindRequest(settings);
        int initialSize = settings.getAsInt("user_search.pool.initial_size", DEFAULT_CONNECTION_POOL_INITIAL_SIZE);
        int size = settings.getAsInt("user_search.pool.size", DEFAULT_CONNECTION_POOL_SIZE);
        try {
            LDAPConnectionPool pool = new LDAPConnectionPool(serverSet, bindRequest, initialSize, size);
            pool.setRetryFailedOperationsDueToInvalidConnections(true);
            if (settings.getAsBoolean("user_search.pool.health_check.enabled", true)) {
                String entryDn = settings.get("user_search.pool.health_check.dn", (bindRequest == null) ? null : bindRequest.getBindDN());
                if (entryDn == null) {
                    pool.close();
                    throw new IllegalArgumentException("[bind_dn] has not been specified so a value must be specified for [user_search" +
                            ".pool.health_check.dn] or [user_search.pool.health_check.enabled] must be set to false");
                }
                long healthCheckInterval = settings.getAsTime("user_search.pool.health_check.interval", DEFAULT_HEALTH_CHECK_INTERVAL)
                        .millis();
                // Checks the status of the LDAP connection at a specified interval in the background. We do not check on
                // on create as the LDAP server may require authentication to get an entry. We do not check on checkout
                // as we always set retry operations and the pool will handle a bad connection without the added latency on every operation
                GetEntryLDAPConnectionPoolHealthCheck healthCheck = new GetEntryLDAPConnectionPoolHealthCheck(entryDn, timeout.millis(),
                        false, false, false, true, false);
                pool.setHealthCheck(healthCheck);
                pool.setHealthCheckIntervalMillis(healthCheckInterval);
            }
            return pool;
        } catch (LDAPException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("unable to create connection pool for realm [{}]", e, config.name());
            } else {
                logger.error("unable to create connection pool for realm [{}]: {}", config.name(), e.getMessage());
            }
        }
        return null;
    }

    static SimpleBindRequest bindRequest(Settings settings) {
        SimpleBindRequest request = null;
        String bindDn = settings.get("bind_dn");
        if (bindDn != null) {
            request = new SimpleBindRequest(bindDn, settings.get("bind_password"));
        }
        return request;
    }

    @Override
    protected LdapSession getSession(String user, SecuredString password) throws Exception {
        try {
            String dn = findUserDN(user);
            tryBind(dn, password);
            return new LdapSession(logger, connectionPool, dn, groupResolver, timeout);
        } catch (LDAPException e) {
            throw Exceptions.authenticationError("failed to authenticate user [{}]", e, user);
        }
    }

    @Override
    public boolean supportsUnauthenticatedSession() {
        return true;
    }

    @Override
    public LdapSession unauthenticatedSession(String user) throws Exception {
        try {
            String dn = findUserDN(user);
            return new LdapSession(logger, connectionPool, dn, groupResolver, timeout);
        } catch (LDAPException e) {
            throw Exceptions.authenticationError("failed to lookup user [{}]", e, user);
        }
    }

    private String findUserDN(String user) throws Exception {
        SearchRequest request = new SearchRequest(userSearchBaseDn, scope.scope(), createEqualityFilter(userAttribute, encodeValue(user))
                , SearchRequest.NO_ATTRIBUTES);
        request.setTimeLimitSeconds(Math.toIntExact(timeout.seconds()));
        LDAPConnectionPool connectionPool = connectionPool();
        SearchResultEntry entry = searchForEntry(connectionPool, request, logger);
        if (entry == null) {
            throw Exceptions.authenticationError("failed to find user [{}] with search base [{}] scope [{}]", user, userSearchBaseDn,
                    scope.toString().toLowerCase(Locale.ENGLISH));
        }
        return entry.getDN();
    }

    private void tryBind(String dn, SecuredString password) throws IOException {
        LDAPConnection bindConnection;
        try {
            bindConnection = serverSet.getConnection();
        } catch (LDAPException e) {
            throw new IOException("unable to connect to any LDAP servers for bind", e);
        }

        try {
            bindConnection.bind(dn, new String(password.internalChars()));
        } catch (LDAPException e) {
            throw Exceptions.authenticationError("failed LDAP authentication for DN [{}]", e, dn);
        } finally {
            bindConnection.close();
        }
    }

    /*
     * This method is used to cleanup the connections for tests
     */
    void shutdown() {
        if (connectionPool != null) {
            connectionPool.close();
        }
    }

    static GroupsResolver groupResolver(Settings settings) {
        Settings searchSettings = settings.getAsSettings("group_search");
        if (!searchSettings.names().isEmpty()) {
            return new SearchGroupsResolver(searchSettings);
        }
        return new UserAttributeGroupsResolver(settings);
    }
}

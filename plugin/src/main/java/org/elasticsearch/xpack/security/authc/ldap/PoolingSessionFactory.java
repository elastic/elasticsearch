/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import com.unboundid.ldap.sdk.BindRequest;
import com.unboundid.ldap.sdk.GetEntryLDAPConnectionPoolHealthCheck;
import com.unboundid.ldap.sdk.LDAPConnectionPool;
import com.unboundid.ldap.sdk.LDAPConnectionPoolHealthCheck;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.ServerSet;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.authc.RealmSettings;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapMetaDataResolver;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils;
import org.elasticsearch.xpack.security.authc.ldap.support.SessionFactory;
import org.elasticsearch.xpack.ssl.SSLService;

import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Base class for LDAP session factories that can make use of a connection pool
 */
abstract class PoolingSessionFactory extends SessionFactory implements Releasable {

    static final int DEFAULT_CONNECTION_POOL_SIZE = 20;
    static final int DEFAULT_CONNECTION_POOL_INITIAL_SIZE = 0;
    static final Setting<String> BIND_DN = Setting.simpleString("bind_dn", Setting.Property.NodeScope, Setting.Property.Filtered);
    static final Setting<String> BIND_PASSWORD = Setting.simpleString("bind_password", Setting.Property.NodeScope,
            Setting.Property.Filtered);

    private static final TimeValue DEFAULT_HEALTH_CHECK_INTERVAL = TimeValue.timeValueSeconds(60L);
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

    private final boolean useConnectionPool;
    private final LDAPConnectionPool connectionPool;

    final LdapMetaDataResolver metaDataResolver;
    final LdapSession.GroupsResolver groupResolver;


    /**
     * @param config the configuration for the realm
     * @param sslService the ssl service to get a socket factory or context from
     * @param groupResolver the resolver to use to find groups belonging to a user
     * @param poolingEnabled the setting that should be used to determine if connection pooling is enabled
     * @param bindRequestSupplier the supplier for a bind requests that should be used for pooled connections
     * @param healthCheckDNSupplier a supplier for the dn to query for health checks
     */
    PoolingSessionFactory(RealmConfig config, SSLService sslService, LdapSession.GroupsResolver groupResolver,
                          Setting<Boolean> poolingEnabled, Supplier<BindRequest> bindRequestSupplier,
                          Supplier<String> healthCheckDNSupplier) throws LDAPException {
        super(config, sslService);
        this.groupResolver = groupResolver;
        this.metaDataResolver = new LdapMetaDataResolver(config.settings(), ignoreReferralErrors);
        this.useConnectionPool = poolingEnabled.get(config.settings());
        if (useConnectionPool) {
            this.connectionPool = createConnectionPool(config, serverSet, timeout, logger, bindRequestSupplier, healthCheckDNSupplier);
        } else {
            this.connectionPool = null;
        }
    }

    @Override
    public final void session(String user, SecureString password, ActionListener<LdapSession> listener) {
        if (useConnectionPool) {
            getSessionWithPool(connectionPool, user, password, listener);
        } else {
            getSessionWithoutPool(user, password, listener);
        }
    }

    @Override
    public final void unauthenticatedSession(String user, ActionListener<LdapSession> listener) {
        if (useConnectionPool) {
            getUnauthenticatedSessionWithPool(connectionPool, user, listener);
        } else {
            getUnauthenticatedSessionWithoutPool(user, listener);
        }
    }

    /**
     * Attempts to get a {@link LdapSession} using the provided credentials and makes use of the provided connection pool
     */
    abstract void getSessionWithPool(LDAPConnectionPool connectionPool, String user, SecureString password,
                                     ActionListener<LdapSession> listener);

    /**
     * Attempts to get a {@link LdapSession} using the provided credentials and opens a new connection to the ldap server
     */
    abstract void getSessionWithoutPool(String user, SecureString password, ActionListener<LdapSession> listener);

    /**
     * Attempts to search using a pooled connection for the user and provides an unauthenticated {@link LdapSession} to the listener if the
     * user is found
     */
    abstract void getUnauthenticatedSessionWithPool(LDAPConnectionPool connectionPool, String user, ActionListener<LdapSession> listener);

    /**
     * Attempts to search using a new connection for the user and provides an unauthenticated {@link LdapSession} to the listener if the
     * user is found
     */
    abstract void getUnauthenticatedSessionWithoutPool(String user, ActionListener<LdapSession> listener);

    /**
     * Creates the connection pool that will be used by the session factory and initializes the health check support
     */
    static LDAPConnectionPool createConnectionPool(RealmConfig config, ServerSet serverSet, TimeValue timeout, Logger logger,
                                                   Supplier<BindRequest> bindRequestSupplier,
                                                   Supplier<String> healthCheckDnSupplier) throws LDAPException {
        Settings settings = config.settings();
        BindRequest bindRequest = bindRequestSupplier.get();
        final int initialSize = POOL_INITIAL_SIZE.get(settings);
        final int size = POOL_SIZE.get(settings);
        LDAPConnectionPool pool = null;
        boolean success = false;
        try {
            pool = LdapUtils.privilegedConnect(() -> new LDAPConnectionPool(serverSet, bindRequest, initialSize, size));
            pool.setRetryFailedOperationsDueToInvalidConnections(true);
            if (HEALTH_CHECK_ENABLED.get(settings)) {
                String entryDn = HEALTH_CHECK_DN.get(settings).orElseGet(healthCheckDnSupplier);
                final long healthCheckInterval = HEALTH_CHECK_INTERVAL.get(settings).millis();
                if (entryDn != null) {
                    // Checks the status of the LDAP connection at a specified interval in the background. We do not check on
                    // create as the LDAP server may require authentication to get an entry and a bind request has not been executed
                    // yet so we could end up never getting a connection. We do not check on checkout as we always set retry operations
                    // and the pool will handle a bad connection without the added latency on every operation
                    LDAPConnectionPoolHealthCheck healthCheck = new GetEntryLDAPConnectionPoolHealthCheck(entryDn, timeout.millis(),
                            false, false, false, true, false);
                    pool.setHealthCheck(healthCheck);
                    pool.setHealthCheckIntervalMillis(healthCheckInterval);
                } else {
                    logger.warn(new ParameterizedMessage("[{}] and [{}} have not been specified or are not valid distinguished names," +
                            "so connection health checking is disabled", RealmSettings.getFullSettingKey(config, BIND_DN),
                            RealmSettings.getFullSettingKey(config, HEALTH_CHECK_DN)));
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

    /**
     * This method is used to cleanup the connection pool if one is being used
     */
    @Override
    public final void close() {
        if (connectionPool != null) {
            connectionPool.close();
        }
    }

    public static Set<Setting<?>> getSettings() {
        return Sets.newHashSet(POOL_INITIAL_SIZE, POOL_SIZE, HEALTH_CHECK_ENABLED, HEALTH_CHECK_INTERVAL, HEALTH_CHECK_DN, BIND_DN,
                BIND_PASSWORD);
    }
}

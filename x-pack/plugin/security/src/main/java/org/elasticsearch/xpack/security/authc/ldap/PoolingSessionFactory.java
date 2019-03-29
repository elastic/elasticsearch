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
import com.unboundid.ldap.sdk.SASLBindRequest;
import com.unboundid.ldap.sdk.ServerSet;
import com.unboundid.ldap.sdk.SimpleBindRequest;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.PoolingSessionFactorySettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapMetaDataResolver;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils;
import org.elasticsearch.xpack.security.authc.ldap.support.SessionFactory;

import java.util.function.Supplier;

/**
 * Base class for LDAP session factories that can make use of a connection pool
 */
abstract class PoolingSessionFactory extends SessionFactory implements Releasable {

    private final boolean useConnectionPool;
    private final LDAPConnectionPool connectionPool;

    final BindRequest bindRequestCredentials;
    final LdapMetaDataResolver metaDataResolver;
    final LdapSession.GroupsResolver groupResolver;


    /**
     * @param config the configuration for the realm
     * @param sslService the ssl service to get a socket factory or context from
     * @param groupResolver the resolver to use to find groups belonging to a user
     * @param poolingEnabled the setting that should be used to determine if connection pooling is enabled
     * @param bindRequestCredentials bind request {@link BindRequest} to perform an LDAPv3 bind operation, which
     *        authenticates using a either {@link SimpleBindRequest} or {@link SASLBindRequest}.
     * @param healthCheckDNSupplier a supplier for the dn to query for health checks
     * @param threadPool a thread pool used for async queries execution
     */
    PoolingSessionFactory(RealmConfig config, SSLService sslService, LdapSession.GroupsResolver groupResolver,
                          Setting.AffixSetting<Boolean> poolingEnabled, BindRequest bindRequestCredentials,
                          Supplier<String> healthCheckDNSupplier, ThreadPool threadPool) throws LDAPException {
        super(config, sslService, threadPool);
        this.groupResolver = groupResolver;
        this.metaDataResolver = new LdapMetaDataResolver(config, ignoreReferralErrors);

        this.bindRequestCredentials = bindRequestCredentials;

        this.useConnectionPool = config.getSetting(poolingEnabled);
        if (useConnectionPool) {
            this.connectionPool = createConnectionPool(config, serverSet, timeout, logger, bindRequestCredentials, healthCheckDNSupplier);
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
                                                   BindRequest bindRequest,
                                                   Supplier<String> healthCheckDnSupplier) throws LDAPException {
        final int initialSize = config.getSetting(PoolingSessionFactorySettings.POOL_INITIAL_SIZE);
        final int size = config.getSetting(PoolingSessionFactorySettings.POOL_SIZE);
        LDAPConnectionPool pool = null;
        boolean success = false;
        try {
            pool = LdapUtils.privilegedConnect(() -> new LDAPConnectionPool(serverSet, bindRequest, initialSize, size));
            pool.setRetryFailedOperationsDueToInvalidConnections(true);
            if (config.getSetting(PoolingSessionFactorySettings.HEALTH_CHECK_ENABLED)) {
                String entryDn = config.getSetting(PoolingSessionFactorySettings.HEALTH_CHECK_DN).orElseGet(healthCheckDnSupplier);
                final long healthCheckInterval = config.getSetting(PoolingSessionFactorySettings.HEALTH_CHECK_INTERVAL).millis();
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
                            "so connection health checking is disabled", RealmSettings.getFullSettingKey(config,
                            PoolingSessionFactorySettings.BIND_DN),
                            RealmSettings.getFullSettingKey(config, PoolingSessionFactorySettings.HEALTH_CHECK_DN)));
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

    /**
     * For tests use only
     *
     * @return the connection pool for LDAP queries
     */
    LDAPConnectionPool getConnectionPool() {
        return connectionPool;
    }

}

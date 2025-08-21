/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import com.unboundid.ldap.sdk.BindRequest;
import com.unboundid.ldap.sdk.GetEntryLDAPConnectionPoolHealthCheck;
import com.unboundid.ldap.sdk.LDAPConnectionPool;
import com.unboundid.ldap.sdk.LDAPConnectionPoolHealthCheck;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.ServerSet;
import com.unboundid.ldap.sdk.SimpleBindRequest;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.core.CharArrays;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.PoolingSessionFactorySettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils;
import org.elasticsearch.xpack.security.authc.ldap.support.SessionFactory;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.security.authc.ldap.PoolingSessionFactorySettings.LEGACY_BIND_PASSWORD;
import static org.elasticsearch.xpack.core.security.authc.ldap.PoolingSessionFactorySettings.SECURE_BIND_PASSWORD;

/**
 * Base class for LDAP session factories that can make use of a connection pool
 */
abstract class PoolingSessionFactory extends SessionFactory implements Releasable {

    private final boolean useConnectionPool;
    private final LDAPConnectionPool connectionPool;

    private final String bindDn;
    private final AtomicReference<SimpleBindRequest> bindRequest;
    final LdapSession.GroupsResolver groupResolver;

    /**
     * @param config the configuration for the realm
     * @param sslService the ssl service to get a socket factory or context from
     * @param groupResolver the resolver to use to find groups belonging to a user
     * @param poolingEnabled the setting that should be used to determine if connection pooling is enabled
     * @param bindDn the DN of the user to be used for pooled connections (or null to perform anonymous bind)
     * @param healthCheckDNSupplier a supplier for the dn to query for health checks
     * @param threadPool a thread pool used for async queries execution
     */
    PoolingSessionFactory(
        RealmConfig config,
        SSLService sslService,
        LdapSession.GroupsResolver groupResolver,
        Setting.AffixSetting<Boolean> poolingEnabled,
        @Nullable String bindDn,
        Supplier<String> healthCheckDNSupplier,
        ThreadPool threadPool
    ) throws LDAPException {
        super(config, sslService, threadPool);
        this.groupResolver = groupResolver;
        this.bindDn = bindDn;
        this.bindRequest = new AtomicReference<>(buildBindRequest(config.settings()));
        this.useConnectionPool = config.getSetting(poolingEnabled);
        if (useConnectionPool) {
            this.connectionPool = createConnectionPool(config, serverSet, timeout, logger, bindRequest.get(), healthCheckDNSupplier);
        } else {
            this.connectionPool = null;
        }
    }

    /**
     * Builds a bind request that is used to authenticate users in LDAP user search mode. The returned {@link SimpleBindRequest} will hold
     * the configured bind DN and password. In case the bind DN and password are not configured, then this method will return a simple
     * bind request that will perform an anonymous bind.
     * <p>
     * This method can be called during initialization of session factory as well as during the reloading of secure setting.
     * This is controlled with the {@code reloadRequest} parameter. If {@code reloadRequest} is se to {@code true}, this method
     * will perform a setting consistency validation and throw {@link SettingsException} in case of violation.
     * Due to legacy reasons and BWC, when {@code reloadRequest} is se to {@code false}, this method will only log a warning message.
     *
     * @return A new {@link SimpleBindRequest} that contains configured bind DN and password.
     */
    private SimpleBindRequest buildBindRequest(Settings settings) {
        final byte[] bindPassword;
        final Setting<SecureString> legacyPasswordSetting = config.getConcreteSetting(LEGACY_BIND_PASSWORD);
        final Setting<SecureString> securePasswordSetting = config.getConcreteSetting(SECURE_BIND_PASSWORD);

        if (legacyPasswordSetting.exists(settings)) {
            if (securePasswordSetting.exists(settings)) {
                throw new IllegalArgumentException(
                    "You cannot specify both [" + legacyPasswordSetting.getKey() + "] and [" + securePasswordSetting.getKey() + "]"
                );
            }
            bindPassword = CharArrays.toUtf8Bytes(legacyPasswordSetting.get(settings).getChars());
        } else if (securePasswordSetting.exists(settings)) {
            bindPassword = CharArrays.toUtf8Bytes(securePasswordSetting.get(settings).getChars());
        } else {
            bindPassword = null;
        }

        if (this.bindDn == null) {
            return new SimpleBindRequest();
        } else {
            if (bindPassword == null) {
                throw new SettingsException(
                    "[{}] is set but no bind password is specified. Without a corresponding bind password, "
                        + "all {} realm authentication will fail. Specify a bind password via [{}].",
                    RealmSettings.getFullSettingKey(config, PoolingSessionFactorySettings.BIND_DN),
                    config.type(),
                    RealmSettings.getFullSettingKey(config, SECURE_BIND_PASSWORD)
                );
            }
            return new SimpleBindRequest(this.bindDn, bindPassword);
        }
    }

    @Override
    public void reload(Settings settings) {
        final SimpleBindRequest oldRequest = bindRequest.get();
        final SimpleBindRequest newRequest = buildBindRequest(settings);
        if (bindRequestEquals(newRequest, oldRequest) == false) {
            if (bindRequest.compareAndSet(oldRequest, newRequest)) {
                if (connectionPool != null) {
                    // When a connection is open and already bound, changing the bind password does not affect
                    // the existing pooled connections. LDAP connections are stateful, and once a connection is
                    // established and bound, it remains open until explicitly closed or until a connection
                    // timeout occurs. Changing the bind password on the LDAP server does not automatically
                    // invalidate existing connections. Hence, simply setting the new bind request is sufficient.
                    connectionPool.setBindRequest(bindRequest.get());
                }
            }
        }
    }

    private static boolean bindRequestEquals(SimpleBindRequest req1, SimpleBindRequest req2) {
        return req1.getBindDN().contentEquals(req2.getBindDN()) && req1.getPassword().equalsIgnoreType(req2.getPassword());
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
    abstract void getSessionWithPool(
        LDAPConnectionPool connectionPool,
        String user,
        SecureString password,
        ActionListener<LdapSession> listener
    );

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
    static LDAPConnectionPool createConnectionPool(
        RealmConfig config,
        ServerSet serverSet,
        TimeValue timeout,
        Logger logger,
        BindRequest bindRequest,
        Supplier<String> healthCheckDnSupplier
    ) throws LDAPException {
        final int initialSize = config.getSetting(PoolingSessionFactorySettings.POOL_INITIAL_SIZE);
        final int size = config.getSetting(PoolingSessionFactorySettings.POOL_SIZE);
        LDAPConnectionPool pool = null;
        boolean success = false;
        try {
            pool = LdapUtils.privilegedConnect(() -> new LDAPConnectionPool(serverSet, bindRequest, initialSize, size));
            pool.setConnectionPoolName("ldap-pool-" + config.identifier());
            pool.setRetryFailedOperationsDueToInvalidConnections(true);
            if (config.getSetting(PoolingSessionFactorySettings.HEALTH_CHECK_ENABLED)) {
                String entryDn = config.getSetting(PoolingSessionFactorySettings.HEALTH_CHECK_DN).orElseGet(healthCheckDnSupplier);
                final long healthCheckInterval = config.getSetting(PoolingSessionFactorySettings.HEALTH_CHECK_INTERVAL).millis();
                if (entryDn != null) {
                    // Checks the status of the LDAP connection at a specified interval in the background. We do not check on
                    // create as the LDAP server may require authentication to get an entry and a bind request has not been executed
                    // yet so we could end up never getting a connection. We do not check on checkout as we always set retry operations
                    // and the pool will handle a bad connection without the added latency on every operation
                    LDAPConnectionPoolHealthCheck healthCheck = new GetEntryLDAPConnectionPoolHealthCheck(
                        entryDn,
                        timeout.millis(),
                        false,
                        false,
                        false,
                        true,
                        false
                    );
                    pool.setHealthCheck(healthCheck);
                    pool.setHealthCheckIntervalMillis(healthCheckInterval);
                } else {
                    logger.warn(
                        () -> format(
                            "[%s] and [%s] have not been specified or are not valid distinguished names,"
                                + "so connection health checking is disabled",
                            RealmSettings.getFullSettingKey(config, PoolingSessionFactorySettings.BIND_DN),
                            RealmSettings.getFullSettingKey(config, PoolingSessionFactorySettings.HEALTH_CHECK_DN)
                        )
                    );
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

    SimpleBindRequest getBindRequest() {
        return bindRequest.get();
    }

}

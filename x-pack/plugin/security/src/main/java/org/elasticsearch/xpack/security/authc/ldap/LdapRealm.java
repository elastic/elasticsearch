/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import com.unboundid.ldap.sdk.LDAPException;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapRealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapSessionFactorySettings;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapUserSearchSessionFactorySettings;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper.UserData;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapLoadBalancing;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession;
import org.elasticsearch.xpack.security.authc.ldap.support.SessionFactory;
import org.elasticsearch.xpack.security.authc.support.CachingUsernamePasswordRealm;
import org.elasticsearch.xpack.security.authc.support.DelegatedAuthorizationSupport;
import org.elasticsearch.xpack.security.authc.support.mapper.CompositeRoleMapper;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.security.authc.ldap.LdapUserSearchSessionFactory.configuredUserSearchSettings;

/**
 * Authenticates username/password tokens against ldap, locates groups and maps them to roles.
 */
public final class LdapRealm extends CachingUsernamePasswordRealm {

    private final SessionFactory sessionFactory;
    private final UserRoleMapper roleMapper;
    private final ThreadPool threadPool;
    private final TimeValue executionTimeout;

    private DelegatedAuthorizationSupport delegatedRealms;

    public LdapRealm(
        RealmConfig config,
        SSLService sslService,
        ResourceWatcherService watcherService,
        NativeRoleMappingStore nativeRoleMappingStore,
        ThreadPool threadPool
    ) throws LDAPException {
        this(
            config,
            sessionFactory(config, sslService, threadPool),
            new CompositeRoleMapper(config, watcherService, nativeRoleMappingStore),
            threadPool
        );
    }

    // pkg private for testing
    LdapRealm(RealmConfig config, SessionFactory sessionFactory, UserRoleMapper roleMapper, ThreadPool threadPool) {
        super(config, threadPool);
        this.sessionFactory = sessionFactory;
        this.roleMapper = roleMapper;
        this.threadPool = threadPool;
        this.executionTimeout = config.getSetting(LdapRealmSettings.EXECUTION_TIMEOUT);
        roleMapper.refreshRealmOnChange(this);
    }

    static SessionFactory sessionFactory(RealmConfig config, SSLService sslService, ThreadPool threadPool) throws LDAPException {

        final SessionFactory sessionFactory;
        if (LdapRealmSettings.AD_TYPE.equals(config.type())) {
            sessionFactory = new ActiveDirectorySessionFactory(config, sslService, threadPool);
        } else {
            assert LdapRealmSettings.LDAP_TYPE.equals(config.type())
                : "type ["
                    + config.type()
                    + "] is unknown. expected one of ["
                    + LdapRealmSettings.AD_TYPE
                    + ", "
                    + LdapRealmSettings.LDAP_TYPE
                    + "]";
            final List<? extends Setting.AffixSetting<?>> configuredSearchSettings = configuredUserSearchSettings(config);
            final boolean hasTemplates = config.hasSetting(LdapSessionFactorySettings.USER_DN_TEMPLATES_SETTING);
            if (configuredSearchSettings.isEmpty()) {
                if (hasTemplates == false) {
                    throw new IllegalArgumentException(
                        "settings were not found for either user search ["
                            + RealmSettings.getFullSettingKey(config, LdapUserSearchSessionFactorySettings.SEARCH_BASE_DN)
                            + "] or user template ["
                            + RealmSettings.getFullSettingKey(config, LdapSessionFactorySettings.USER_DN_TEMPLATES_SETTING)
                            + "] modes of operation. "
                            + "Please provide the settings for the mode you wish to use. For more details refer to the ldap "
                            + "authentication section of the X-Pack guide."
                    );
                }
                sessionFactory = new LdapSessionFactory(config, sslService, threadPool);
            } else if (hasTemplates) {
                throw new IllegalArgumentException(
                    "settings were found for both user search ["
                        + configuredSearchSettings.stream()
                            .map(s -> RealmSettings.getFullSettingKey(config, s))
                            .collect(Collectors.joining(","))
                        + "] and user template ["
                        + RealmSettings.getFullSettingKey(config, LdapSessionFactorySettings.USER_DN_TEMPLATES_SETTING)
                        + "] modes of operation. "
                        + "Please remove the settings for the mode you do not wish to use. For more details refer to the ldap "
                        + "authentication section of the X-Pack guide."
                );
            } else {
                sessionFactory = new LdapUserSearchSessionFactory(config, sslService, threadPool);
            }
        }
        return sessionFactory;
    }

    /**
     * Given a username and password, open a connection to ldap, bind to authenticate, retrieve groups, map to roles and build the user.
     * This user will then be passed to the listener
     */
    @Override
    protected void doAuthenticate(UsernamePasswordToken token, ActionListener<AuthenticationResult<User>> listener) {
        assert delegatedRealms != null : "Realm has not been initialized correctly";
        // we submit to the threadpool because authentication using LDAP will execute blocking I/O for a bind request and we don't want
        // network threads stuck waiting for a socket to connect. After the bind, then all interaction with LDAP should be async
        final CancellableLdapRunnable<AuthenticationResult<User>> cancellableLdapRunnable = new CancellableLdapRunnable<>(
            listener,
            ex -> AuthenticationResult.unsuccessful("Authentication against realm [" + this.toString() + "] failed", ex),
            () -> sessionFactory.session(
                token.principal(),
                token.credentials(),
                contextPreservingListener(new LdapSessionActionListener("authenticate", token.principal(), listener))
            ),
            logger
        );
        threadPool.generic().execute(cancellableLdapRunnable);
        threadPool.schedule(cancellableLdapRunnable::maybeTimeout, executionTimeout, Names.SAME);
    }

    @Override
    protected void doLookupUser(String username, ActionListener<User> userActionListener) {
        if (sessionFactory.supportsUnauthenticatedSession()) {
            // we submit to the threadpool because authentication using LDAP will execute blocking I/O for a bind request and we don't want
            // network threads stuck waiting for a socket to connect. After the bind, then all interaction with LDAP should be async
            final ActionListener<AuthenticationResult<User>> sessionListener = ActionListener.wrap(
                result -> userActionListener.onResponse(result.getValue()),
                userActionListener::onFailure
            );
            final CancellableLdapRunnable<User> cancellableLdapRunnable = new CancellableLdapRunnable<>(
                userActionListener,
                e -> null,
                () -> sessionFactory.unauthenticatedSession(
                    username,
                    contextPreservingListener(new LdapSessionActionListener("lookup", username, sessionListener))
                ),
                logger
            );
            threadPool.generic().execute(cancellableLdapRunnable);
            threadPool.schedule(cancellableLdapRunnable::maybeTimeout, executionTimeout, Names.SAME);
        } else {
            userActionListener.onResponse(null);
        }
    }

    /**
     * Wraps the provided <code>sessionListener</code> to preserve the {@link ThreadContext} associated with the
     * current thread.
     * Responses headers are not preserved, as they are not needed. Response output should not yet exist, nor should
     * any be produced within the realm/ldap-session.
     */
    private ContextPreservingActionListener<LdapSession> contextPreservingListener(LdapSessionActionListener sessionListener) {
        final Supplier<ThreadContext.StoredContext> toRestore = config.threadContext().newRestorableContext(false);
        return new ContextPreservingActionListener<>(toRestore, sessionListener);
    }

    @Override
    public void initialize(Iterable<Realm> realms, XPackLicenseState licenseState) {
        if (delegatedRealms != null) {
            throw new IllegalStateException("Realm has already been initialized");
        }
        delegatedRealms = new DelegatedAuthorizationSupport(realms, config, licenseState);
    }

    @Override
    public void usageStats(ActionListener<Map<String, Object>> listener) {
        super.usageStats(ActionListener.wrap(usage -> {
            usage.put("size", getCacheSize());
            usage.put("load_balance_type", LdapLoadBalancing.resolve(config).toString());
            usage.put("ssl", sessionFactory.isSslUsed());
            usage.put("user_search", false == configuredUserSearchSettings(config).isEmpty());
            listener.onResponse(usage);
        }, listener::onFailure));
    }

    private static void buildUser(
        LdapSession session,
        String username,
        ActionListener<AuthenticationResult<User>> listener,
        UserRoleMapper roleMapper,
        DelegatedAuthorizationSupport delegatedAuthz
    ) {
        assert delegatedAuthz != null : "DelegatedAuthorizationSupport is null";
        if (session == null) {
            listener.onResponse(AuthenticationResult.notHandled());
        } else if (delegatedAuthz.hasDelegation()) {
            delegatedAuthz.resolve(username, listener);
        } else {
            lookupUserFromSession(username, session, roleMapper, listener);
        }
    }

    @Override
    protected void handleCachedAuthentication(User user, ActionListener<AuthenticationResult<User>> listener) {
        if (delegatedRealms.hasDelegation()) {
            delegatedRealms.resolve(user.principal(), listener);
        } else {
            super.handleCachedAuthentication(user, listener);
        }
    }

    private static void lookupUserFromSession(
        String username,
        LdapSession session,
        UserRoleMapper roleMapper,
        ActionListener<AuthenticationResult<User>> listener
    ) {
        boolean loadingGroups = false;
        try {
            final Consumer<Exception> onFailure = e -> {
                IOUtils.closeWhileHandlingException(session);
                listener.onFailure(e);
            };
            session.resolve(ActionListener.wrap((ldapData) -> {
                final Map<String, Object> metadata = new HashMap<>();
                metadata.put("ldap_dn", session.userDn());
                metadata.put("ldap_groups", ldapData.groups);
                metadata.putAll(ldapData.metadata);
                final UserData user = new UserData(username, session.userDn(), ldapData.groups, metadata, session.realm());
                roleMapper.resolveRoles(user, ActionListener.wrap(roles -> {
                    IOUtils.close(session);
                    String[] rolesArray = roles.toArray(new String[roles.size()]);
                    listener.onResponse(AuthenticationResult.success(new User(username, rolesArray, null, null, metadata, true)));
                }, onFailure));
            }, onFailure));
            loadingGroups = true;
        } finally {
            if (loadingGroups == false) {
                session.close();
            }
        }
    }

    /**
     * A special {@link ActionListener} that encapsulates the handling of a LdapSession, which is used to return a user. This class handles
     * cases where the session is null or where an exception may be caught after a session has been established, which requires the
     * closing of the session.
     */
    private class LdapSessionActionListener implements ActionListener<LdapSession> {

        private final AtomicReference<LdapSession> ldapSessionAtomicReference = new AtomicReference<>();
        private String action;
        private final String username;
        private final ActionListener<AuthenticationResult<User>> resultListener;

        LdapSessionActionListener(String action, String username, ActionListener<AuthenticationResult<User>> resultListener) {
            this.action = action;
            this.username = username;
            this.resultListener = resultListener;
        }

        @Override
        public void onResponse(LdapSession session) {
            if (session == null) {
                resultListener.onResponse(AuthenticationResult.notHandled());
            } else {
                ldapSessionAtomicReference.set(session);
                buildUser(session, username, resultListener, roleMapper, delegatedRealms);
            }
        }

        @Override
        public void onFailure(Exception e) {
            if (ldapSessionAtomicReference.get() != null) {
                IOUtils.closeWhileHandlingException(ldapSessionAtomicReference.get());
            }
            if (logger.isDebugEnabled()) {
                logger.debug(() -> format("Exception occurred during %s for %s", action, LdapRealm.this), e);
            }
            resultListener.onResponse(AuthenticationResult.unsuccessful(action + " failed", e));
        }

    }

    /**
     * A runnable that allows us to terminate and call the listener. We use this as a runnable can
     * be queued and not executed for a long time or ever and this causes user requests to appear
     * to hang. In these cases at least we can provide a response.
     */
    static class CancellableLdapRunnable<T> extends AbstractRunnable {

        private final Runnable in;
        private final ActionListener<T> listener;
        private final Function<Exception, T> defaultValue;
        private final Logger logger;
        private final AtomicReference<LdapRunnableState> state = new AtomicReference<>(LdapRunnableState.AWAITING_EXECUTION);

        CancellableLdapRunnable(ActionListener<T> listener, Function<Exception, T> defaultValue, Runnable in, Logger logger) {
            this.listener = listener;
            this.defaultValue = Objects.requireNonNull(defaultValue);
            this.in = in;
            this.logger = logger;
        }

        @Override
        public void onFailure(Exception e) {
            logger.error("execution of ldap runnable failed", e);
            final T result = defaultValue.apply(e);
            listener.onResponse(result);
        }

        @Override
        protected void doRun() throws Exception {
            if (state.compareAndSet(LdapRunnableState.AWAITING_EXECUTION, LdapRunnableState.EXECUTING)) {
                in.run();
            } else {
                logger.trace("skipping execution of ldap runnable as the current state is [{}]", state.get());
            }
        }

        @Override
        public void onRejection(Exception e) {
            listener.onFailure(e);
        }

        /**
         * If the execution of this runnable has not already started, the runnable is cancelled and we pass an exception to the user
         * listener
         */
        void maybeTimeout() {
            if (state.compareAndSet(LdapRunnableState.AWAITING_EXECUTION, LdapRunnableState.TIMED_OUT)) {
                logger.warn("skipping execution of ldap runnable as it has been waiting for " + "execution too long");
                listener.onFailure(new ElasticsearchTimeoutException("timed out waiting for " + "execution of ldap runnable"));
            }
        }

        enum LdapRunnableState {
            AWAITING_EXECUTION,
            EXECUTING,
            TIMED_OUT
        }
    }
}

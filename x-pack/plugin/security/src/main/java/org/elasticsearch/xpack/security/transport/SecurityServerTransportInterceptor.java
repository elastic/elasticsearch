/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.search.fetch.ShardFetchRequest;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteConnectionManager;
import org.elasticsearch.transport.SendRequestTransportException;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportService.ContextRestoreResponseHandler;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.RemoteAccessAuthentication;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.transport.ProfileConfigurations;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.elasticsearch.xpack.security.authz.AuthorizationUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.security.SecurityField.setting;

public class SecurityServerTransportInterceptor implements TransportInterceptor {

    // TODO this does not belong here
    public static final String REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY = "_remote_access_cluster_credential";
    private static final Version VERSION_REMOTE_ACCESS_HEADERS = Version.V_8_7_0;
    private static final Logger logger = LogManager.getLogger(SecurityServerTransportInterceptor.class);

    private final AuthenticationService authcService;
    private final AuthorizationService authzService;
    private final SSLService sslService;
    private final Map<String, ServerTransportFilter> profileFilters;
    private final ThreadPool threadPool;
    private final Settings settings;
    private final SecurityContext securityContext;
    private final RemoteClusterAuthorizationResolver remoteClusterAuthorizationResolver;
    private final Function<Transport.Connection, Optional<String>> remoteClusterAliasResolver;

    public SecurityServerTransportInterceptor(
        Settings settings,
        ThreadPool threadPool,
        AuthenticationService authcService,
        AuthorizationService authzService,
        SSLService sslService,
        SecurityContext securityContext,
        DestructiveOperations destructiveOperations,
        RemoteClusterAuthorizationResolver remoteClusterAuthorizationResolver
    ) {
        this(
            settings,
            threadPool,
            authcService,
            authzService,
            sslService,
            securityContext,
            destructiveOperations,
            remoteClusterAuthorizationResolver,
            RemoteConnectionManager::resolveRemoteClusterAlias
        );
    }

    public SecurityServerTransportInterceptor(
        Settings settings,
        ThreadPool threadPool,
        AuthenticationService authcService,
        AuthorizationService authzService,
        SSLService sslService,
        SecurityContext securityContext,
        DestructiveOperations destructiveOperations,
        RemoteClusterAuthorizationResolver remoteClusterAuthorizationResolver,
        // Inject for simplified testing
        Function<Transport.Connection, Optional<String>> remoteClusterAliasResolver
    ) {
        this.settings = settings;
        this.threadPool = threadPool;
        this.authcService = authcService;
        this.authzService = authzService;
        this.sslService = sslService;
        this.securityContext = securityContext;
        this.profileFilters = initializeProfileFilters(destructiveOperations);
        this.remoteClusterAuthorizationResolver = remoteClusterAuthorizationResolver;
        this.remoteClusterAliasResolver = remoteClusterAliasResolver;
    }

    @Override
    public AsyncSender interceptSender(AsyncSender sender) {
        return interceptForAllRequests(
            // Branching based on the feature flag is not strictly necessary here, but it makes it more obvious we are not interfering with
            // non-feature-flagged deployments
            TcpTransport.isUntrustedRemoteClusterEnabled() ? interceptForRemoteAccessRequests(sender) : sender
        );
    }

    private AsyncSender interceptForAllRequests(AsyncSender sender) {
        return new AsyncSender() {
            @Override
            public <T extends TransportResponse> void sendRequest(
                Transport.Connection connection,
                String action,
                TransportRequest request,
                TransportRequestOptions options,
                TransportResponseHandler<T> handler
            ) {
                // the transport in core normally does this check, BUT since we are serializing to a string header we need to do it
                // ourselves otherwise we wind up using a version newer than what we can actually send
                final Version minVersion = Version.min(connection.getVersion(), Version.CURRENT);

                // Sometimes a system action gets executed like a internal create index request or update mappings request
                // which means that the user is copied over to system actions so we need to change the user
                if (AuthorizationUtils.shouldReplaceUserWithSystem(threadPool.getThreadContext(), action)) {
                    securityContext.executeAsSystemUser(
                        minVersion,
                        original -> sendWithUser(
                            connection,
                            action,
                            request,
                            options,
                            new ContextRestoreResponseHandler<>(threadPool.getThreadContext().wrapRestorable(original), handler),
                            sender
                        )
                    );
                } else if (AuthorizationUtils.shouldSetUserBasedOnActionOrigin(threadPool.getThreadContext())) {
                    AuthorizationUtils.switchUserBasedOnActionOriginAndExecute(
                        threadPool.getThreadContext(),
                        securityContext,
                        minVersion,
                        (original) -> sendWithUser(
                            connection,
                            action,
                            request,
                            options,
                            new ContextRestoreResponseHandler<>(threadPool.getThreadContext().wrapRestorable(original), handler),
                            sender
                        )
                    );
                } else if (securityContext.getAuthentication() != null
                    && securityContext.getAuthentication().getEffectiveSubject().getVersion().equals(minVersion) == false) {
                        // re-write the authentication since we want the authentication version to match the version of the connection
                        securityContext.executeAfterRewritingAuthentication(
                            original -> sendWithUser(
                                connection,
                                action,
                                request,
                                options,
                                new ContextRestoreResponseHandler<>(threadPool.getThreadContext().wrapRestorable(original), handler),
                                sender
                            ),
                            minVersion
                        );
                    } else {
                        sendWithUser(connection, action, request, options, handler, sender);
                    }
            }
        };
    }

    private AsyncSender interceptForRemoteAccessRequests(final AsyncSender sender) {
        return new AsyncSender() {
            @Override
            public <T extends TransportResponse> void sendRequest(
                Transport.Connection connection,
                String action,
                TransportRequest request,
                TransportRequestOptions options,
                TransportResponseHandler<T> handler
            ) {
                if (shouldSendWithRemoteAccessHeaders(connection, request)) {
                    sendWithRemoteAccessHeaders(connection, action, request, options, handler);
                } else {
                    // Send regular request, without remote access headers
                    try {
                        sender.sendRequest(connection, action, request, options, handler);
                    } catch (Exception e) {
                        handler.handleException(new SendRequestTransportException(connection.getNode(), action, e));
                    }
                }
            }

            private boolean shouldSendWithRemoteAccessHeaders(final Transport.Connection connection, final TransportRequest request) {
                final Authentication authentication = securityContext.getAuthentication();
                assert authentication != null : "authentication must be present in security context";
                final Subject effectiveSubject = authentication.getEffectiveSubject();
                final Optional<String> remoteClusterAlias = remoteClusterAliasResolver.apply(connection);
                return remoteClusterAlias.isPresent()
                    && remoteClusterAuthorizationResolver.resolveAuthorization(remoteClusterAlias.get()) != null
                    && isWhitelistedForRemoteAccessHeaders(request)
                    && effectiveSubject.getType().equals(Subject.Type.USER)
                    && false == User.isInternal(effectiveSubject.getUser())
                    && Arrays.stream(effectiveSubject.getUser().roles()).noneMatch(ReservedRolesStore::isReserved);
            }

            private <T extends TransportResponse> void sendWithRemoteAccessHeaders(
                final Transport.Connection connection,
                final String action,
                final TransportRequest request,
                final TransportRequestOptions options,
                final TransportResponseHandler<T> handler
            ) {
                logger.debug("Sending request with remote access headers for [{}] action", action);
                if (connection.getVersion().before(VERSION_REMOTE_ACCESS_HEADERS)) {
                    handler.handleException(
                        new TransportException(
                            "Settings for remote cluster indicate remote access headers should be sent but target cluster version ["
                                + connection.getVersion()
                                + "] does not support receiving them"
                        )
                    );
                    return;
                }

                final Authentication authentication = securityContext.getAuthentication();
                assert authentication != null : "authentication must be present in security context";
                final Optional<String> remoteClusterAlias = remoteClusterAliasResolver.apply(connection);
                assert remoteClusterAlias.isPresent() : "remote cluster alias must be set for the transport connection";
                final ThreadContext threadContext = securityContext.getThreadContext();
                final Supplier<ThreadContext.StoredContext> contextSupplier = threadContext.newRestorableContext(true);
                final var contextRestoreHandler = new ContextRestoreResponseHandler<>(contextSupplier, handler);
                authzService.retrieveRemoteAccessRoleDescriptorsIntersection(
                    remoteClusterAlias.get(),
                    authentication.getEffectiveSubject(),
                    ActionListener.wrap(roleDescriptorsIntersection -> {
                        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
                            writeCredentialForClusterToContext(remoteClusterAlias.get(), threadContext);
                            final RemoteAccessAuthentication remoteAccessAuthentication = new RemoteAccessAuthentication(
                                authentication,
                                roleDescriptorsIntersection
                            );
                            remoteAccessAuthentication.writeToContext(threadContext);
                            sender.sendRequest(connection, action, request, options, handler);
                        }
                    }, e -> contextRestoreHandler.handleException(new SendRequestTransportException(connection.getNode(), action, e)))
                );
            }

            private boolean isWhitelistedForRemoteAccessHeaders(final TransportRequest request) {
                return request instanceof ShardSearchRequest
                    || request instanceof ShardFetchRequest
                    || request instanceof SearchRequest
                    || request instanceof ClusterSearchShardsRequest;
            }

            private void writeCredentialForClusterToContext(final String remoteClusterAlias, final ThreadContext threadContext) {
                final String clusterCredential = remoteClusterAuthorizationResolver.resolveAuthorization(remoteClusterAlias);
                // This can happen if the cluster credential was updated after we made the initial check to send remote access headers
                // In this case, fail the request. In the future we may want to retry instead, to pick up the updated remote cluster
                // configuration
                if (clusterCredential == null) {
                    throw new IllegalStateException(
                        "remote cluster credential unavailable for target cluster [" + remoteClusterAlias + "]"
                    );
                }
                threadContext.putHeader(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY, withApiKeyPrefix(clusterCredential));
            }

            private String withApiKeyPrefix(final String clusterCredential) {
                // TODO base64 encode?
                return "ApiKey " + clusterCredential;
            }
        };
    }

    private <T extends TransportResponse> void sendWithUser(
        Transport.Connection connection,
        String action,
        TransportRequest request,
        TransportRequestOptions options,
        TransportResponseHandler<T> handler,
        AsyncSender sender
    ) {
        if (securityContext.getAuthentication() == null) {
            // we use an assertion here to ensure we catch this in our testing infrastructure, but leave the ISE for cases we do not catch
            // in tests and may be hit by a user
            assertNoAuthentication(action);
            throw new IllegalStateException("there should always be a user when sending a message for action [" + action + "]");
        }

        try {
            sender.sendRequest(connection, action, request, options, handler);
        } catch (Exception e) {
            handler.handleException(new SendRequestTransportException(connection.getNode(), action, e));
        }
    }

    // pkg-private method to allow overriding for tests
    void assertNoAuthentication(String action) {
        assert false : "there should always be a user when sending a message for action [" + action + "]";
    }

    @Override
    public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
        String action,
        String executor,
        boolean forceExecution,
        TransportRequestHandler<T> actualHandler
    ) {
        return new ProfileSecuredRequestHandler<>(logger, action, forceExecution, executor, actualHandler, profileFilters, threadPool);
    }

    private Map<String, ServerTransportFilter> initializeProfileFilters(DestructiveOperations destructiveOperations) {
        final SslConfiguration sslConfiguration = sslService.getSSLConfiguration(setting("transport.ssl"));
        final Map<String, SslConfiguration> profileConfigurations = ProfileConfigurations.get(settings, sslService, sslConfiguration);

        Map<String, ServerTransportFilter> profileFilters = Maps.newMapWithExpectedSize(profileConfigurations.size() + 1);

        final boolean transportSSLEnabled = XPackSettings.TRANSPORT_SSL_ENABLED.get(settings);
        for (Map.Entry<String, SslConfiguration> entry : profileConfigurations.entrySet()) {
            final SslConfiguration profileConfiguration = entry.getValue();
            final boolean extractClientCert = transportSSLEnabled && SSLService.isSSLClientAuthEnabled(profileConfiguration);
            profileFilters.put(
                entry.getKey(),
                new ServerTransportFilter(
                    authcService,
                    authzService,
                    threadPool.getThreadContext(),
                    extractClientCert,
                    destructiveOperations,
                    securityContext
                )
            );
        }

        return Collections.unmodifiableMap(profileFilters);
    }

    public static class ProfileSecuredRequestHandler<T extends TransportRequest> implements TransportRequestHandler<T> {

        private final String action;
        private final TransportRequestHandler<T> handler;
        private final Map<String, ServerTransportFilter> profileFilters;
        private final ThreadContext threadContext;
        private final String executorName;
        private final ThreadPool threadPool;
        private final boolean forceExecution;
        private final Logger logger;

        ProfileSecuredRequestHandler(
            Logger logger,
            String action,
            boolean forceExecution,
            String executorName,
            TransportRequestHandler<T> handler,
            Map<String, ServerTransportFilter> profileFilters,
            ThreadPool threadPool
        ) {
            this.logger = logger;
            this.action = action;
            this.executorName = executorName;
            this.handler = handler;
            this.profileFilters = profileFilters;
            this.threadContext = threadPool.getThreadContext();
            this.threadPool = threadPool;
            this.forceExecution = forceExecution;
        }

        AbstractRunnable getReceiveRunnable(T request, TransportChannel channel, Task task) {
            final Runnable releaseRequest = new RunOnce(request::decRef);
            request.incRef();
            return new AbstractRunnable() {
                @Override
                public boolean isForceExecution() {
                    return forceExecution;
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception e1) {
                        e1.addSuppressed(e);
                        logger.warn("failed to send exception response for action [" + action + "]", e1);
                    }
                }

                @Override
                protected void doRun() throws Exception {
                    handler.messageReceived(request, channel, task);
                }

                @Override
                public void onAfter() {
                    releaseRequest.run();
                }
            };
        }

        @Override
        public String toString() {
            return "ProfileSecuredRequestHandler{"
                + "action='"
                + action
                + '\''
                + ", executorName='"
                + executorName
                + '\''
                + ", forceExecution="
                + forceExecution
                + '}';
        }

        @Override
        public void messageReceived(T request, TransportChannel channel, Task task) {
            try (ThreadContext.StoredContext ctx = threadContext.newStoredContextPreservingResponseHeaders()) {
                String profile = channel.getProfileName();
                ServerTransportFilter filter = profileFilters.get(profile);

                if (filter == null) {
                    if (TransportService.DIRECT_RESPONSE_PROFILE.equals(profile)) {
                        // apply the default filter to local requests. We never know what the request is or who sent it...
                        filter = profileFilters.get("default");
                    } else {
                        String msg = "transport profile [" + profile + "] is not associated with a transport filter";
                        throw new IllegalStateException(msg);
                    }
                }
                assert filter != null;

                final AbstractRunnable receiveMessage = getReceiveRunnable(request, channel, task);
                final ActionListener<Void> filterListener;
                if (ThreadPool.Names.SAME.equals(executorName)) {
                    filterListener = new AbstractFilterListener(receiveMessage) {
                        @Override
                        public void onResponse(Void unused) {
                            receiveMessage.run();
                        }
                    };
                } else {
                    final Thread executingThread = Thread.currentThread();
                    filterListener = new AbstractFilterListener(receiveMessage) {

                        @Override
                        public void onResponse(Void unused) {
                            if (executingThread == Thread.currentThread()) {
                                // only fork off if we get called on another thread this means we moved to
                                // an async execution and in this case we need to go back to the thread pool
                                // that was actually executing it. it's also possible that the
                                // thread-pool we are supposed to execute on is `SAME` in that case
                                // the handler is OK with executing on a network thread and we can just continue even if
                                // we are on another thread due to async operations
                                receiveMessage.run();
                            } else {
                                try {
                                    threadPool.executor(executorName).execute(receiveMessage);
                                } catch (Exception e) {
                                    onFailure(e);
                                }
                            }
                        }
                    };
                }
                filter.inbound(action, request, channel, filterListener);

            }
        }
    }

    private abstract static class AbstractFilterListener implements ActionListener<Void> {

        protected final AbstractRunnable receiveMessage;

        protected AbstractFilterListener(AbstractRunnable receiveMessage) {
            this.receiveMessage = receiveMessage;
        }

        @Override
        public void onFailure(Exception e) {
            try {
                receiveMessage.onFailure(e);
            } finally {
                receiveMessage.onAfter();
            }
        }
    }
}

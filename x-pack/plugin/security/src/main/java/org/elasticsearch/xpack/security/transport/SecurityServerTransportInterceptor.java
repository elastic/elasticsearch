/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.SendRequestTransportException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportChannel;
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
import org.elasticsearch.xpack.core.security.transport.ProfileConfigurations;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.SslProfile;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.elasticsearch.xpack.security.authz.AuthorizationUtils;
import org.elasticsearch.xpack.security.authz.PreAuthorizationUtils;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executor;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.transport.RemoteClusterPortSettings.REMOTE_CLUSTER_PROFILE;

public class SecurityServerTransportInterceptor implements TransportInterceptor {

    private static final Logger logger = LogManager.getLogger(SecurityServerTransportInterceptor.class);

    private final AuthenticationService authcService;
    private final AuthorizationService authzService;
    private final RemoteClusterTransportInterceptor remoteClusterTransportInterceptor;
    private final Map<String, ServerTransportFilter> profileFilters;
    private final ThreadPool threadPool;
    private final SecurityContext securityContext;
    private final Settings settings;

    public SecurityServerTransportInterceptor(
        Settings settings,
        ThreadPool threadPool,
        AuthenticationService authcService,
        AuthorizationService authzService,
        SSLService sslService,
        SecurityContext securityContext,
        DestructiveOperations destructiveOperations,
        RemoteClusterTransportInterceptor remoteClusterTransportInterceptor
    ) {
        this.remoteClusterTransportInterceptor = remoteClusterTransportInterceptor;
        this.securityContext = securityContext;
        this.threadPool = threadPool;
        this.settings = settings;
        this.authcService = authcService;
        this.authzService = authzService;
        final Map<String, SslProfile> profileConfigurations = ProfileConfigurations.get(settings, sslService, false);
        this.profileFilters = initializeProfileFilters(profileConfigurations, destructiveOperations);
    }

    private Map<String, ServerTransportFilter> initializeProfileFilters(
        final Map<String, SslProfile> profileConfigurations,
        final DestructiveOperations destructiveOperations
    ) {
        final Map<String, ServerTransportFilter> profileFilters = Maps.newMapWithExpectedSize(profileConfigurations.size() + 1);
        final boolean transportSSLEnabled = XPackSettings.TRANSPORT_SSL_ENABLED.get(settings);

        for (Map.Entry<String, SslProfile> entry : profileConfigurations.entrySet()) {
            final String profileName = entry.getKey();
            final SslProfile sslProfile = entry.getValue();
            if (profileName.equals(REMOTE_CLUSTER_PROFILE)) {
                var remoteProfileTransportFilter = this.remoteClusterTransportInterceptor.getRemoteProfileTransportFilter(
                    sslProfile,
                    destructiveOperations
                );
                if (remoteProfileTransportFilter.isPresent()) {
                    profileFilters.put(profileName, remoteProfileTransportFilter.get());
                    continue;
                }
            }

            final SslConfiguration profileConfiguration = sslProfile.configuration();
            assert profileConfiguration != null : "SSL Profile [" + sslProfile + "] for [" + profileName + "] has a null configuration";
            profileFilters.put(
                profileName,
                new ServerTransportFilter(
                    authcService,
                    authzService,
                    threadPool.getThreadContext(),
                    transportSSLEnabled && SSLService.isSSLClientAuthEnabled(profileConfiguration),
                    destructiveOperations,
                    securityContext
                )
            );
        }

        return Collections.unmodifiableMap(profileFilters);
    }

    @Override
    public AsyncSender interceptSender(AsyncSender sender) {
        return interceptForAllRequests(remoteClusterTransportInterceptor.interceptSender(sender));
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
                assert false == remoteClusterTransportInterceptor.hasRemoteClusterAccessHeadersInContext(securityContext)
                    : "remote cluster access headers should not be in security context";
                final boolean isRemoteClusterConnection = remoteClusterTransportInterceptor.isRemoteClusterConnection(connection);
                if (PreAuthorizationUtils.shouldRemoveParentAuthorizationFromThreadContext(
                    action,
                    securityContext,
                    isRemoteClusterConnection
                )) {
                    securityContext.executeAfterRemovingParentAuthorization(original -> {
                        sendRequestInner(
                            sender,
                            connection,
                            action,
                            request,
                            options,
                            new ContextRestoreResponseHandler<>(threadPool.getThreadContext().wrapRestorable(original), handler)
                        );
                    });
                } else {
                    sendRequestInner(sender, connection, action, request, options, handler);
                }
            }
        };
    }

    public <T extends TransportResponse> void sendRequestInner(
        AsyncSender sender,
        Transport.Connection connection,
        String action,
        TransportRequest request,
        TransportRequestOptions options,
        TransportResponseHandler<T> handler
    ) {
        // the transport in core normally does this check, BUT since we are serializing to a string header we need to do it
        // ourselves otherwise we wind up using a version newer than what we can actually send
        final TransportVersion minVersion = TransportVersion.min(connection.getTransportVersion(), TransportVersion.current());

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
            && securityContext.getAuthentication().getEffectiveSubject().getTransportVersion().equals(minVersion) == false) {
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

    // Package private for testing
    Map<String, ServerTransportFilter> getProfileFilters() {
        return profileFilters;
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

        assert securityContext.getParentAuthorization() == null
            || false == remoteClusterTransportInterceptor.isRemoteClusterConnection(connection)
            : "parent authorization header should not be set for remote cluster requests";

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
        Executor executor,
        boolean forceExecution,
        TransportRequestHandler<T> actualHandler
    ) {
        return new ProfileSecuredRequestHandler<>(logger, action, forceExecution, executor, actualHandler, profileFilters, threadPool);
    }

    public static class ProfileSecuredRequestHandler<T extends TransportRequest> implements TransportRequestHandler<T> {

        private final String action;
        private final TransportRequestHandler<T> handler;
        private final Map<String, ServerTransportFilter> profileFilters;
        private final ThreadContext threadContext;
        private final Executor executor;
        private final ThreadPool threadPool;
        private final boolean forceExecution;
        private final Logger logger;

        ProfileSecuredRequestHandler(
            Logger logger,
            String action,
            boolean forceExecution,
            Executor executor,
            TransportRequestHandler<T> handler,
            Map<String, ServerTransportFilter> profileFilters,
            ThreadPool threadPool
        ) {
            this.logger = logger;
            this.action = action;
            this.executor = executor;
            this.handler = handler;
            this.profileFilters = profileFilters;
            this.threadContext = threadPool.getThreadContext();
            this.threadPool = threadPool;
            this.forceExecution = forceExecution;
        }

        AbstractRunnable getReceiveRunnable(T request, TransportChannel channel, Task task) {
            final Runnable releaseRequest = new RunOnce(request::decRef);
            request.mustIncRef();
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
            return "ProfileSecuredRequestHandler{" + "action='" + action + '\'' + ", forceExecution=" + forceExecution + '}';
        }

        @Override
        public void messageReceived(T request, TransportChannel channel, Task task) {
            try (ThreadContext.StoredContext ctx = threadContext.newStoredContextPreservingResponseHeaders()) {
                String profile = channel.getProfileName();
                ServerTransportFilter filter = getServerTransportFilter(profile);
                assert filter != null;
                assert request != null;
                logger.trace(
                    () -> format(
                        "Applying transport filter [%s] for transport profile [%s] on request [%s]",
                        filter.getClass(),
                        profile,
                        request.getClass()
                    )
                );

                final AbstractRunnable receiveMessage = getReceiveRunnable(request, channel, task);
                final ActionListener<Void> filterListener;
                if (executor == EsExecutors.DIRECT_EXECUTOR_SERVICE) {
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
                                    executor.execute(receiveMessage);
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

        private ServerTransportFilter getServerTransportFilter(String profile) {
            final ServerTransportFilter filter = profileFilters.get(profile);
            if (filter != null) {
                return filter;
            }
            if (TransportService.DIRECT_RESPONSE_PROFILE.equals(profile)) {
                // apply the default filter to local requests. We never know what the request is or who sent it...
                return profileFilters.get("default");
            } else {
                String msg = "transport profile [" + profile + "] is not associated with a transport filter";
                throw new IllegalStateException(msg);
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

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
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.license.XPackLicenseState;
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
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.ssl.SSLConfiguration;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.elasticsearch.xpack.security.authz.AuthorizationUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.SecurityField.setting;

public class SecurityServerTransportInterceptor implements TransportInterceptor {

    private static final Logger logger = LogManager.getLogger(SecurityServerTransportInterceptor.class);

    private final AuthenticationService authcService;
    private final AuthorizationService authzService;
    private final SSLService sslService;
    private final Map<String, ServerTransportFilter> profileFilters;
    private final XPackLicenseState licenseState;
    private final ThreadPool threadPool;
    private final Settings settings;
    private final SecurityContext securityContext;

    private volatile boolean isStateNotRecovered = true;

    public SecurityServerTransportInterceptor(Settings settings,
                                              ThreadPool threadPool,
                                              AuthenticationService authcService,
                                              AuthorizationService authzService,
                                              XPackLicenseState licenseState,
                                              SSLService sslService,
                                              SecurityContext securityContext,
                                              DestructiveOperations destructiveOperations,
                                              ClusterService clusterService) {
        this.settings = settings;
        this.threadPool = threadPool;
        this.authcService = authcService;
        this.authzService = authzService;
        this.licenseState = licenseState;
        this.sslService = sslService;
        this.securityContext = securityContext;
        this.profileFilters = initializeProfileFilters(destructiveOperations);
        clusterService.addListener(e -> isStateNotRecovered = e.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK));
    }

    @Override
    public AsyncSender interceptSender(AsyncSender sender) {
        return new AsyncSender() {
            @Override
            public <T extends TransportResponse> void sendRequest(Transport.Connection connection, String action, TransportRequest request,
                                                                  TransportRequestOptions options, TransportResponseHandler<T> handler) {
                final boolean requireAuth = XPackSettings.SECURITY_ENABLED.get(settings);
                // the transport in core normally does this check, BUT since we are serializing to a string header we need to do it
                // ourselves otherwise we wind up using a version newer than what we can actually send
                final Version minVersion = Version.min(connection.getVersion(), Version.CURRENT);

                // Sometimes a system action gets executed like a internal create index request or update mappings request
                // which means that the user is copied over to system actions so we need to change the user
                if (AuthorizationUtils.shouldReplaceUserWithSystem(threadPool.getThreadContext(), action)) {
                    securityContext.executeAsUser(SystemUser.INSTANCE, (original) -> sendWithUser(connection, action, request, options,
                            new ContextRestoreResponseHandler<>(threadPool.getThreadContext().wrapRestorable(original)
                                    , handler), sender), minVersion);
                } else if (AuthorizationUtils.shouldSetUserBasedOnActionOrigin(threadPool.getThreadContext())) {
                    AuthorizationUtils.switchUserBasedOnActionOriginAndExecute(threadPool.getThreadContext(), securityContext,
                            (original) -> sendWithUser(connection, action, request, options,
                                    new ContextRestoreResponseHandler<>(threadPool.getThreadContext().wrapRestorable(original)
                                            , handler), sender));
                } else if (securityContext.getAuthentication() != null &&
                        securityContext.getAuthentication().getVersion().equals(minVersion) == false) {
                    // re-write the authentication since we want the authentication version to match the version of the connection
                    securityContext.executeAfterRewritingAuthentication(original -> sendWithUser(connection, action, request, options,
                        new ContextRestoreResponseHandler<>(threadPool.getThreadContext().wrapRestorable(original), handler), sender),
                        minVersion);
                } else {
                    sendWithUser(connection, action, request, options, handler, sender);
                }
            }
        };
    }

    private <T extends TransportResponse> void sendWithUser(Transport.Connection connection, String action, TransportRequest request,
                                                            TransportRequestOptions options, TransportResponseHandler<T> handler,
                                                            AsyncSender sender) {
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
    public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(String action, String executor,
                                                                                    boolean forceExecution,
                                                                                    TransportRequestHandler<T> actualHandler) {
        return new ProfileSecuredRequestHandler<>(logger, action, forceExecution, executor, actualHandler, profileFilters,
                settings, threadPool);
    }

    private Map<String, ServerTransportFilter> initializeProfileFilters(DestructiveOperations destructiveOperations) {
        final SSLConfiguration sslConfiguration = sslService.getSSLConfiguration(setting("transport.ssl"));
        final Map<String, SSLConfiguration> profileConfigurations = ProfileConfigurations.get(settings, sslService, sslConfiguration);

        Map<String, ServerTransportFilter> profileFilters = new HashMap<>(profileConfigurations.size() + 1);

        final boolean transportSSLEnabled = XPackSettings.TRANSPORT_SSL_ENABLED.get(settings);
        for (Map.Entry<String, SSLConfiguration> entry : profileConfigurations.entrySet()) {
            final SSLConfiguration profileConfiguration = entry.getValue();
            final boolean extractClientCert = transportSSLEnabled && sslService.isSSLClientAuthEnabled(profileConfiguration);
            profileFilters.put(entry.getKey(), new ServerTransportFilter(authcService, authzService, threadPool.getThreadContext(),
                extractClientCert, destructiveOperations, securityContext));
        }

        return Collections.unmodifiableMap(profileFilters);
    }

    public static class ProfileSecuredRequestHandler<T extends TransportRequest> implements TransportRequestHandler<T> {

        private final String action;
        private final TransportRequestHandler<T> handler;
        private final Map<String, ServerTransportFilter> profileFilters;
        private final Settings settings;
        private final ThreadContext threadContext;
        private final String executorName;
        private final ThreadPool threadPool;
        private final boolean forceExecution;
        private final Logger logger;

        ProfileSecuredRequestHandler(Logger logger, String action, boolean forceExecution, String executorName,
                                     TransportRequestHandler<T> handler, Map<String, ServerTransportFilter> profileFilters,
                                     Settings settings, ThreadPool threadPool) {
            this.logger = logger;
            this.action = action;
            this.executorName = executorName;
            this.handler = handler;
            this.profileFilters = profileFilters;
            this.settings = settings;
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
            return "ProfileSecuredRequestHandler{" +
                    "action='" + action + '\'' +
                    ", executorName='" + executorName + '\'' +
                    ", forceExecution=" + forceExecution +
                    '}';
        }

        @Override
        public void messageReceived(T request, TransportChannel channel, Task task) {
            try (ThreadContext.StoredContext ctx = threadContext.newStoredContext(true)) {
                if (XPackSettings.SECURITY_ENABLED.get(settings)) {
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
                } else {
                    getReceiveRunnable(request, channel, task).run();
                }
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

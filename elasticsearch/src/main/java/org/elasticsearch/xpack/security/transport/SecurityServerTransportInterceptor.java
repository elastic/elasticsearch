/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.xpack.security.SecurityContext;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.elasticsearch.xpack.security.authz.AuthorizationUtils;
import org.elasticsearch.xpack.security.authz.accesscontrol.RequestContext;
import org.elasticsearch.xpack.security.transport.netty3.SecurityNetty3Transport;
import org.elasticsearch.xpack.security.user.SystemUser;
import org.elasticsearch.xpack.ssl.SSLService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.XPackSettings.TRANSPORT_SSL_ENABLED;
import static org.elasticsearch.xpack.security.Security.setting;

public class SecurityServerTransportInterceptor implements TransportInterceptor {

    private static final String SETTING_NAME = "xpack.security.type";

    private final AuthenticationService authcService;
    private final AuthorizationService authzService;
    private final SSLService sslService;
    private final Map<String, ServerTransportFilter> profileFilters;
    private final XPackLicenseState licenseState;
    private final ThreadPool threadPool;
    private final Settings settings;
    private final SecurityContext securityContext;

    public SecurityServerTransportInterceptor(Settings settings,
                                              ThreadPool threadPool,
                                              AuthenticationService authcService,
                                              AuthorizationService authzService,
                                              XPackLicenseState licenseState,
                                              SSLService sslService,
                                              SecurityContext securityContext,
                                              DestructiveOperations destructiveOperations) {
        this.settings = settings;
        this.threadPool = threadPool;
        this.authcService = authcService;
        this.authzService = authzService;
        this.licenseState = licenseState;
        this.sslService = sslService;
        this.securityContext = securityContext;
        this.profileFilters = initializeProfileFilters(destructiveOperations);
    }

    @Override
    public AsyncSender interceptSender(AsyncSender sender) {
        return new AsyncSender() {
            @Override
            public <T extends TransportResponse> void sendRequest(DiscoveryNode node, String action, TransportRequest request,
                                                                  TransportRequestOptions options, TransportResponseHandler<T> handler) {
                if (licenseState.isAuthAllowed()) {
                    // Sometimes a system action gets executed like a internal create index request or update mappings request
                    // which means that the user is copied over to system actions so we need to change the user
                    if (AuthorizationUtils.shouldReplaceUserWithSystem(threadPool.getThreadContext(), action)) {
                        securityContext.executeAsUser(SystemUser.INSTANCE, (original) -> sendWithUser(node, action, request, options,
                                new ContextRestoreResponseHandler<>(threadPool.getThreadContext(), original, handler), sender));
                    } else {
                        sendWithUser(node, action, request, options, handler, sender);
                    }
                } else {
                    sender.sendRequest(node, action, request, options, handler);
                }
            }
        };
    }

    private <T extends TransportResponse> void sendWithUser(DiscoveryNode node, String action, TransportRequest request,
                                                            TransportRequestOptions options, TransportResponseHandler<T> handler,
                                                            AsyncSender sender) {
        // There cannot be a request outgoing from this node that is not associated with a user.
        if (securityContext.getAuthentication() == null) {
            throw new IllegalStateException("there should always be a user when sending a message");
        }

        try {
            sender.sendRequest(node, action, request, options, handler);
        } catch (Exception e) {
            handler.handleException(new TransportException("failed sending request", e));
        }
    }

    @Override
    public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(String action, String executor,
                                                                                    TransportRequestHandler<T> actualHandler) {
        return new ProfileSecuredRequestHandler<>(action, executor, actualHandler, profileFilters,
                licenseState, threadPool);
    }

    protected Map<String, ServerTransportFilter> initializeProfileFilters(DestructiveOperations destructiveOperations) {
        Map<String, Settings> profileSettingsMap = settings.getGroups("transport.profiles.", true);
        Map<String, ServerTransportFilter> profileFilters = new HashMap<>(profileSettingsMap.size() + 1);

        final Settings transportSSLSettings = settings.getByPrefix(setting("transport.ssl."));
        for (Map.Entry<String, Settings> entry : profileSettingsMap.entrySet()) {
            Settings profileSettings = entry.getValue();
            final boolean profileSsl = SecurityNetty3Transport.PROFILE_SSL_SETTING.get(profileSettings);
            final Settings profileSslSettings = SecurityNetty3Transport.profileSslSettings(profileSettings);
            final boolean clientAuth = sslService.isSSLClientAuthEnabled(profileSslSettings, transportSSLSettings);
            final boolean extractClientCert = profileSsl && clientAuth;
            String type = entry.getValue().get(SETTING_NAME, "node");
            switch (type) {
                case "client":
                    profileFilters.put(entry.getKey(), new ServerTransportFilter.ClientProfile(authcService, authzService,
                            threadPool.getThreadContext(), extractClientCert, destructiveOperations));
                    break;
                default:
                    profileFilters.put(entry.getKey(), new ServerTransportFilter.NodeProfile(authcService, authzService,
                            threadPool.getThreadContext(), extractClientCert, destructiveOperations));
            }
        }

        if (!profileFilters.containsKey(TransportSettings.DEFAULT_PROFILE)) {
            final boolean profileSsl = TRANSPORT_SSL_ENABLED.get(settings);
            final boolean clientAuth = sslService.isSSLClientAuthEnabled(transportSSLSettings);
            final boolean extractClientCert = profileSsl && clientAuth;
            profileFilters.put(TransportSettings.DEFAULT_PROFILE, new ServerTransportFilter.NodeProfile(authcService, authzService,
                    threadPool.getThreadContext(), extractClientCert, destructiveOperations));
        }

        return Collections.unmodifiableMap(profileFilters);
    }

    ServerTransportFilter transportFilter(String profile) {
        return profileFilters.get(profile);
    }

    public static class ProfileSecuredRequestHandler<T extends TransportRequest> implements TransportRequestHandler<T> {

        protected final String action;
        protected final TransportRequestHandler<T> handler;
        private final Map<String, ServerTransportFilter> profileFilters;
        private final XPackLicenseState licenseState;
        private final ThreadContext threadContext;
        private final String executorName;
        private final ThreadPool threadPool;

        private ProfileSecuredRequestHandler(String action, String executorName, TransportRequestHandler<T> handler,
                                             Map<String,ServerTransportFilter> profileFilters, XPackLicenseState licenseState,
                                             ThreadPool threadPool) {
            this.action = action;
            this.executorName = executorName;
            this.handler = handler;
            this.profileFilters = profileFilters;
            this.licenseState = licenseState;
            this.threadContext = threadPool.getThreadContext();
            this.threadPool = threadPool;
        }

        @Override
        public void messageReceived(T request, TransportChannel channel, Task task) throws Exception {
            final Consumer<Exception> onFailure = (e) -> {
                try {
                    channel.sendResponse(e);
                } catch (IOException e1) {
                    throw new UncheckedIOException(e1);
                }
            };
            final Runnable receiveMessage = () -> {
                // FIXME we should remove the RequestContext completely since we have ThreadContext but cannot yet due to
                // the query cache
                RequestContext context = new RequestContext(request, threadContext);
                RequestContext.setCurrent(context);
                try {
                    handler.messageReceived(request, channel, task);
                } catch (Exception e) {
                    onFailure.accept(e);
                } finally {
                    RequestContext.removeCurrent();
                }
            };
            try (ThreadContext.StoredContext ctx = threadContext.newStoredContext()) {

                if (licenseState.isAuthAllowed()) {
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
                    final Thread executingThread = Thread.currentThread();
                    Consumer<Void> consumer = (x) -> {
                        final Executor executor;
                        if (executingThread == Thread.currentThread()) {
                            // only fork off if we get called on another thread this means we moved to
                            // an async execution and in this case we need to go back to the thread pool
                            // that was actually executing it. it's also possible that the
                            // thread-pool we are supposed to execute on is `SAME` in that case
                            // the handler is OK with executing on a network thread and we can just continue even if
                            // we are on another thread due to async operations
                            executor = threadPool.executor(ThreadPool.Names.SAME);
                        } else {
                            executor = threadPool.executor(executorName);
                        }

                        try {
                            executor.execute(receiveMessage);
                        } catch (Exception e) {
                            onFailure.accept(e);
                        }

                    };
                    ActionListener<Void> filterListener = ActionListener.wrap(consumer, onFailure);
                    filter.inbound(action, request, channel, filterListener);
                } else {
                    receiveMessage.run();
                }
            } catch (Exception e) {
                channel.sendResponse(e);
            }
        }

        @Override
        public void messageReceived(T request, TransportChannel channel) throws Exception {
            throw new UnsupportedOperationException("task parameter is required for this operation");
        }
    }

    /**
     * This handler wrapper ensures that the response thread executes with the correct thread context. Before any of the handle methods
     * are invoked we restore the context.
     */
    static final class ContextRestoreResponseHandler<T extends TransportResponse> implements TransportResponseHandler<T> {
        private final TransportResponseHandler<T> delegate;
        private final ThreadContext.StoredContext context;
        private final ThreadContext threadContext;

        // pkg private for testing
        ContextRestoreResponseHandler(ThreadContext threadContext, ThreadContext.StoredContext context,
                                      TransportResponseHandler<T> delegate) {
            this.delegate = delegate;
            this.context = context;
            this.threadContext = threadContext;
        }

        @Override
        public T newInstance() {
            return delegate.newInstance();
        }

        @Override
        public void handleResponse(T response) {
            try (ThreadContext.StoredContext ignore = threadContext.newStoredContext()) {
                context.restore();
                delegate.handleResponse(response);
            }
        }

        @Override
        public void handleException(TransportException exp) {
            try (ThreadContext.StoredContext ignore = threadContext.newStoredContext()) {
                context.restore();
                delegate.handleException(exp);
            }
        }

        @Override
        public String executor() {
            return delegate.executor();
        }
    }
}

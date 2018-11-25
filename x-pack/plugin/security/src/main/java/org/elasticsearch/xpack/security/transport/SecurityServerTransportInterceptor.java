/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
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
import org.elasticsearch.xpack.core.security.transport.netty4.SecurityNetty4Transport;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.ssl.SSLConfiguration;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.elasticsearch.xpack.security.authz.AuthorizationUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static org.elasticsearch.xpack.core.security.SecurityField.setting;

public class SecurityServerTransportInterceptor implements TransportInterceptor {

    private static final Function<String, Setting<String>> TRANSPORT_TYPE_SETTING_TEMPLATE = key -> new Setting<>(key, "node", v -> {
        if (v.equals("node") || v.equals("client")) {
            return v;
        }
        throw new IllegalArgumentException("type must be one of [client, node]");
    }, Setting.Property.NodeScope);
    private static final String TRANSPORT_TYPE_SETTING_KEY = "xpack.security.type";
    private static final Logger logger = LogManager.getLogger(SecurityServerTransportInterceptor.class);

    public static final Setting.AffixSetting<String> TRANSPORT_TYPE_PROFILE_SETTING = Setting.affixKeySetting("transport.profiles.",
            TRANSPORT_TYPE_SETTING_KEY, TRANSPORT_TYPE_SETTING_TEMPLATE);

    private final AuthenticationService authcService;
    private final AuthorizationService authzService;
    private final SSLService sslService;
    private final Map<String, ServerTransportFilter> profileFilters;
    private final XPackLicenseState licenseState;
    private final ThreadPool threadPool;
    private final Settings settings;
    private final SecurityContext securityContext;
    private final boolean reservedRealmEnabled;

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
        this.reservedRealmEnabled = XPackSettings.RESERVED_REALM_ENABLED_SETTING.get(settings);
        clusterService.addListener(e -> isStateNotRecovered = e.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK));
    }

    @Override
    public AsyncSender interceptSender(AsyncSender sender) {
        return new AsyncSender() {
            @Override
            public <T extends TransportResponse> void sendRequest(Transport.Connection connection, String action, TransportRequest request,
                                                                  TransportRequestOptions options, TransportResponseHandler<T> handler) {
                // make a local copy of isStateNotRecovered as this is a volatile variable and it
                // is used multiple times in the method. The copy to a local variable allows us to
                // guarantee we use the same value wherever we would check the value for the state
                // being recovered
                final boolean stateNotRecovered = isStateNotRecovered;
                final boolean sendWithAuth = licenseState.isAuthAllowed() || stateNotRecovered;
                if (sendWithAuth) {
                    // the transport in core normally does this check, BUT since we are serializing to a string header we need to do it
                    // ourselves otherwise we wind up using a version newer than what we can actually send
                    final Version minVersion = Version.min(connection.getVersion(), Version.CURRENT);

                    // Sometimes a system action gets executed like a internal create index request or update mappings request
                    // which means that the user is copied over to system actions so we need to change the user
                    if (AuthorizationUtils.shouldReplaceUserWithSystem(threadPool.getThreadContext(), action)) {
                        securityContext.executeAsUser(SystemUser.INSTANCE, (original) -> sendWithUser(connection, action, request, options,
                                new ContextRestoreResponseHandler<>(threadPool.getThreadContext().wrapRestorable(original)
                                        , handler), sender, stateNotRecovered), minVersion);
                    } else if (AuthorizationUtils.shouldSetUserBasedOnActionOrigin(threadPool.getThreadContext())) {
                        AuthorizationUtils.switchUserBasedOnActionOriginAndExecute(threadPool.getThreadContext(), securityContext,
                                (original) -> sendWithUser(connection, action, request, options,
                                        new ContextRestoreResponseHandler<>(threadPool.getThreadContext().wrapRestorable(original)
                                                , handler), sender, stateNotRecovered));
                    } else if (securityContext.getAuthentication() != null &&
                            securityContext.getAuthentication().getVersion().equals(minVersion) == false) {
                        // re-write the authentication since we want the authentication version to match the version of the connection
                        securityContext.executeAfterRewritingAuthentication(original -> sendWithUser(connection, action, request, options,
                            new ContextRestoreResponseHandler<>(threadPool.getThreadContext().wrapRestorable(original), handler), sender,
                            stateNotRecovered), minVersion);
                    } else {
                        sendWithUser(connection, action, request, options, handler, sender, stateNotRecovered);
                    }
                } else {
                    sender.sendRequest(connection, action, request, options, handler);
                }
            }
        };
    }

    private <T extends TransportResponse> void sendWithUser(Transport.Connection connection, String action, TransportRequest request,
                                                            TransportRequestOptions options, TransportResponseHandler<T> handler,
                                                            AsyncSender sender, final boolean stateNotRecovered) {
        // There cannot be a request outgoing from this node that is not associated with a user
        // unless we do not know the actual license of the cluster
        if (securityContext.getAuthentication() == null && stateNotRecovered == false) {
            // we use an assertion here to ensure we catch this in our testing infrastructure, but leave the ISE for cases we do not catch
            // in tests and may be hit by a user
            assertNoAuthentication(action);
            throw new IllegalStateException("there should always be a user when sending a message for action [" + action + "]");
        }

        try {
            sender.sendRequest(connection, action, request, options, handler);
        } catch (Exception e) {
            handler.handleException(new TransportException("failed sending request", e));
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
                licenseState, threadPool);
    }

    protected Map<String, ServerTransportFilter> initializeProfileFilters(DestructiveOperations destructiveOperations) {
        final SSLConfiguration transportSslConfiguration = sslService.getSSLConfiguration(setting("transport.ssl"));
        final Map<String, SSLConfiguration> profileConfigurations = SecurityNetty4Transport.getTransportProfileConfigurations(settings,
            sslService, transportSslConfiguration);

        Map<String, ServerTransportFilter> profileFilters = new HashMap<>(profileConfigurations.size() + 1);

        final boolean transportSSLEnabled = XPackSettings.TRANSPORT_SSL_ENABLED.get(settings);
        for (Map.Entry<String, SSLConfiguration> entry : profileConfigurations.entrySet()) {
            final SSLConfiguration profileConfiguration = entry.getValue();
            final boolean extractClientCert = transportSSLEnabled && sslService.isSSLClientAuthEnabled(profileConfiguration);
            final String type = TRANSPORT_TYPE_PROFILE_SETTING.getConcreteSettingForNamespace(entry.getKey()).get(settings);
            switch (type) {
                case "client":
                    profileFilters.put(entry.getKey(), new ServerTransportFilter.ClientProfile(authcService, authzService,
                            threadPool.getThreadContext(), extractClientCert, destructiveOperations, reservedRealmEnabled,
                            securityContext));
                    break;
                case "node":
                    profileFilters.put(entry.getKey(), new ServerTransportFilter.NodeProfile(authcService, authzService,
                            threadPool.getThreadContext(), extractClientCert, destructiveOperations, reservedRealmEnabled,
                            securityContext));
                    break;
                default:
                   throw new IllegalStateException("unknown profile type: " + type);
            }
        }

        return Collections.unmodifiableMap(profileFilters);
    }

    public static class ProfileSecuredRequestHandler<T extends TransportRequest> implements TransportRequestHandler<T> {

        private final String action;
        private final TransportRequestHandler<T> handler;
        private final Map<String, ServerTransportFilter> profileFilters;
        private final XPackLicenseState licenseState;
        private final ThreadContext threadContext;
        private final String executorName;
        private final ThreadPool threadPool;
        private final boolean forceExecution;
        private final Logger logger;

        ProfileSecuredRequestHandler(Logger logger, String action, boolean forceExecution, String executorName,
                                     TransportRequestHandler<T> handler, Map<String, ServerTransportFilter> profileFilters,
                                     XPackLicenseState licenseState, ThreadPool threadPool) {
            this.logger = logger;
            this.action = action;
            this.executorName = executorName;
            this.handler = handler;
            this.profileFilters = profileFilters;
            this.licenseState = licenseState;
            this.threadContext = threadPool.getThreadContext();
            this.threadPool = threadPool;
            this.forceExecution = forceExecution;
        }

        AbstractRunnable getReceiveRunnable(T request, TransportChannel channel, Task task) {
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
        public void messageReceived(T request, TransportChannel channel, Task task) throws Exception {
            final AbstractRunnable receiveMessage = getReceiveRunnable(request, channel, task);
            try (ThreadContext.StoredContext ctx = threadContext.newStoredContext(true)) {
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

                    CheckedConsumer<Void, Exception> consumer = (x) -> {
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
                            receiveMessage.onFailure(e);
                        }

                    };
                    ActionListener<Void> filterListener = ActionListener.wrap(consumer, receiveMessage::onFailure);
                    filter.inbound(action, request, channel, filterListener);
                } else {
                    receiveMessage.run();
                }
            }
        }
    }
}

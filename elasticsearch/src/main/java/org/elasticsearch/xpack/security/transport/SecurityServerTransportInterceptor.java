/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
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
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.security.SecurityContext;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.elasticsearch.xpack.security.authz.AuthorizationUtils;
import org.elasticsearch.xpack.security.authz.accesscontrol.RequestContext;
import org.elasticsearch.xpack.security.transport.netty4.SecurityNetty4Transport;
import org.elasticsearch.xpack.security.user.KibanaUser;
import org.elasticsearch.xpack.security.user.SystemUser;
import org.elasticsearch.xpack.security.user.User;
import org.elasticsearch.xpack.ssl.SSLService;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

import static org.elasticsearch.xpack.XPackSettings.TRANSPORT_SSL_ENABLED;
import static org.elasticsearch.xpack.security.Security.setting;

public class SecurityServerTransportInterceptor extends AbstractComponent implements TransportInterceptor {

    private static final String SETTING_NAME = "xpack.security.type";

    private final AuthenticationService authcService;
    private final AuthorizationService authzService;
    private final SSLService sslService;
    private final Map<String, ServerTransportFilter> profileFilters;
    private final XPackLicenseState licenseState;
    private final ThreadPool threadPool;
    private final Settings settings;
    private final SecurityContext securityContext;
    private final boolean reservedRealmEnabled;

    public SecurityServerTransportInterceptor(Settings settings,
                                              ThreadPool threadPool,
                                              AuthenticationService authcService,
                                              AuthorizationService authzService,
                                              XPackLicenseState licenseState,
                                              SSLService sslService,
                                              SecurityContext securityContext,
                                              DestructiveOperations destructiveOperations) {
        super(settings);
        this.settings = settings;
        this.threadPool = threadPool;
        this.authcService = authcService;
        this.authzService = authzService;
        this.licenseState = licenseState;
        this.sslService = sslService;
        this.securityContext = securityContext;
        this.profileFilters = initializeProfileFilters(destructiveOperations);
        this.reservedRealmEnabled = XPackSettings.RESERVED_REALM_ENABLED_SETTING.get(settings);
    }

    @Override
    public AsyncSender interceptSender(AsyncSender sender) {
        return new AsyncSender() {
            @Override
            public <T extends TransportResponse> void sendRequest(Transport.Connection connection, String action, TransportRequest request,
                                                                  TransportRequestOptions options, TransportResponseHandler<T> handler) {
                if (licenseState.isAuthAllowed()) {
                    // Sometimes a system action gets executed like a internal create index request or update mappings request
                    // which means that the user is copied over to system actions so we need to change the user
                    if (AuthorizationUtils.shouldReplaceUserWithSystem(threadPool.getThreadContext(), action)) {
                        securityContext.executeAsUser(SystemUser.INSTANCE, (original) -> sendWithUser(connection, action, request, options,
                                new TransportService.ContextRestoreResponseHandler<>(threadPool.getThreadContext().wrapRestorable(original)
                                        , handler), sender));
                    } else if (reservedRealmEnabled && connection.getVersion().before(Version.V_5_2_0_UNRELEASED) &&
                            KibanaUser.NAME.equals(securityContext.getUser().principal())) {
                        final User kibanaUser = securityContext.getUser();
                        final User bwcKibanaUser = new User(kibanaUser.principal(), new String[] { "kibana" }, kibanaUser.fullName(),
                                kibanaUser.email(), kibanaUser.metadata(), kibanaUser.enabled());
                        securityContext.executeAsUser(bwcKibanaUser, (original) -> sendWithUser(connection, action, request, options,
                                new TransportService.ContextRestoreResponseHandler<>(threadPool.getThreadContext().wrapRestorable(original),
                                        handler), sender));
                    } else {
                        sendWithUser(connection, action, request, options, handler, sender);
                    }
                } else {
                    sender.sendRequest(connection, action, request, options, handler);
                }
            }
        };
    }

    private <T extends TransportResponse> void sendWithUser(Transport.Connection connection, String action, TransportRequest request,
                                                            TransportRequestOptions options, TransportResponseHandler<T> handler,
                                                            AsyncSender sender) {
        // There cannot be a request outgoing from this node that is not associated with a user.
        if (securityContext.getAuthentication() == null) {
            throw new IllegalStateException("there should always be a user when sending a message");
        }

        try {
            sender.sendRequest(connection, action, request, options, handler);
        } catch (Exception e) {
            handler.handleException(new TransportException("failed sending request", e));
        }
    }

    @Override
    public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(String action, String executor,
                                                                                    boolean forceExecution,
                                                                                    TransportRequestHandler<T> actualHandler) {
        return new ProfileSecuredRequestHandler<>(logger, action, forceExecution, executor, actualHandler, profileFilters,
                licenseState, threadPool);
    }

    protected Map<String, ServerTransportFilter> initializeProfileFilters(DestructiveOperations destructiveOperations) {
        Map<String, Settings> profileSettingsMap = settings.getGroups("transport.profiles.", true);
        Map<String, ServerTransportFilter> profileFilters = new HashMap<>(profileSettingsMap.size() + 1);

        final Settings transportSSLSettings = settings.getByPrefix(setting("transport.ssl."));
        for (Map.Entry<String, Settings> entry : profileSettingsMap.entrySet()) {
            Settings profileSettings = entry.getValue();
            final boolean profileSsl = SecurityNetty4Transport.PROFILE_SSL_SETTING.get(profileSettings);
            final Settings profileSslSettings = SecurityNetty4Transport.profileSslSettings(profileSettings);
            final boolean clientAuth = sslService.isSSLClientAuthEnabled(profileSslSettings, transportSSLSettings);
            final boolean extractClientCert = profileSsl && clientAuth;
            String type = entry.getValue().get(SETTING_NAME, "node");
            switch (type) {
                case "client":
                    profileFilters.put(entry.getKey(), new ServerTransportFilter.ClientProfile(authcService, authzService,
                            threadPool.getThreadContext(), extractClientCert, destructiveOperations, reservedRealmEnabled,
                            securityContext));
                    break;
                default:
                    profileFilters.put(entry.getKey(), new ServerTransportFilter.NodeProfile(authcService, authzService,
                            threadPool.getThreadContext(), extractClientCert, destructiveOperations, reservedRealmEnabled,
                            securityContext));
            }
        }

        if (!profileFilters.containsKey(TransportSettings.DEFAULT_PROFILE)) {
            final boolean profileSsl = TRANSPORT_SSL_ENABLED.get(settings);
            final boolean clientAuth = sslService.isSSLClientAuthEnabled(transportSSLSettings);
            final boolean extractClientCert = profileSsl && clientAuth;
            profileFilters.put(TransportSettings.DEFAULT_PROFILE, new ServerTransportFilter.NodeProfile(authcService, authzService,
                    threadPool.getThreadContext(), extractClientCert, destructiveOperations, reservedRealmEnabled, securityContext));
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
                    // FIXME we should remove the RequestContext completely since we have ThreadContext but cannot yet due to
                    // the query cache
                    RequestContext context = new RequestContext(request, threadContext);
                    RequestContext.setCurrent(context);
                    try {
                        handler.messageReceived(request, channel, task);
                    } finally {
                        RequestContext.removeCurrent();
                    }
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

        @Override
        public void messageReceived(T request, TransportChannel channel) throws Exception {
            throw new UnsupportedOperationException("task parameter is required for this operation");
        }
    }
}

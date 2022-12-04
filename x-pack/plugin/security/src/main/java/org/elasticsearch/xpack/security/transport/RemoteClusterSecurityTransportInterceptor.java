/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.transport.RemoteConnectionManager;
import org.elasticsearch.transport.SendRequestTransportException;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.RemoteAccessAuthentication;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authz.AuthorizationService;

import java.util.Optional;
import java.util.function.Supplier;

public class RemoteClusterSecurityTransportInterceptor implements TransportInterceptor {

    public static final String REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER = "_remote_access_cluster_credential";

    private final AuthorizationService authzService;
    private final RemoteClusterAuthorizationResolver remoteClusterAuthorizationResolver;
    private final SecurityContext securityContext;

    public RemoteClusterSecurityTransportInterceptor(
        AuthorizationService authzService,
        RemoteClusterAuthorizationResolver remoteClusterAuthorizationResolver,
        SecurityContext securityContext
    ) {
        this.authzService = authzService;
        this.remoteClusterAuthorizationResolver = remoteClusterAuthorizationResolver;
        this.securityContext = securityContext;
    }

    @Override
    public AsyncSender interceptSender(AsyncSender sender) {
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
                    sendWithRemoteAccessHeaders(sender, connection, action, request, options, handler);
                } else {
                    sender.sendRequest(connection, action, request, options, handler);
                }
            }
        };
    }

    boolean shouldSendWithRemoteAccessHeaders(final Transport.Connection connection, final TransportRequest request) {
        if (false == TcpTransport.isUntrustedRemoteClusterEnabled()
        // TODO more request types here
        // || false == (request instanceof SearchRequest || request instanceof ClusterSearchShardsRequest)
        ) {
            return false;
        }
        final Optional<String> remoteClusterAlias = RemoteConnectionManager.resolveRemoteClusterAlias(connection);
        if (false == remoteClusterAlias.isPresent()) {
            return false;
        }

        final Authentication authentication = securityContext.getAuthentication();
        assert authentication != null : "authentication must be present in context";
        if (User.isInternal(authentication.getEffectiveSubject().getUser())
            || authentication.isApiKey()
            || authentication.isServiceAccount()) {
            // The above authentication subject types are not yet supported, so use legacy remote cluster security mode
            return false;
        }

        // TODO we might also need to exclude users with reserved roles for now; if a user has a reserved role, fall back on legacy
        // TODO version check?
        return remoteClusterAuthorizationResolver.resolveAuthorization(remoteClusterAlias.get()) != null;
    }

    <T extends TransportResponse> void sendWithRemoteAccessHeaders(
        final TransportInterceptor.AsyncSender sender,
        final Transport.Connection connection,
        final String action,
        final TransportRequest request,
        final TransportRequestOptions options,
        final TransportResponseHandler<T> handler
    ) {
        final Authentication authentication = securityContext.getAuthentication();
        assert authentication != null : "authentication must be present in context";
        final Optional<String> remoteClusterAlias = RemoteConnectionManager.resolveRemoteClusterAlias(connection);
        // TODO race condition: what if settings have changed after we called shouldSendWithRemoteAccessHeaders?
        assert remoteClusterAlias.isPresent() : "there should be a remote cluster alias for the connection";
        authzService.retrieveRemoteAccessRoleDescriptorsIntersection(
            remoteClusterAlias.get(),
            authentication.getEffectiveSubject(),
            // TODO wrapPreservingContext?
            ActionListener.wrap(roleDescriptorsIntersection -> {
                final ThreadContext threadContext = securityContext.getThreadContext();
                final Supplier<ThreadContext.StoredContext> contextSupplier = threadContext.newRestorableContext(true);
                try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
                    RemoteAccessAuthentication.writeToContextAsRemoteAccessAuthentication(
                        threadContext,
                        authentication,
                        roleDescriptorsIntersection
                    );
                    final String clusterCredential = remoteClusterAuthorizationResolver.resolveAuthorization(remoteClusterAlias.get());
                    // TODO race condition: what if settings have changed after we called shouldSendWithRemoteAccessHeaders?
                    assert clusterCredential != null : "there should be a remote cluster credential";
                    threadContext.putHeader(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER, withApiKeyPrefix(clusterCredential));
                    sender.sendRequest(
                        connection,
                        action,
                        request,
                        options,
                        new TransportService.ContextRestoreResponseHandler<>(contextSupplier, handler)
                    );
                }
            }, e -> handler.handleException(new SendRequestTransportException(connection.getNode(), action, e)))
        );
    }

    private String withApiKeyPrefix(final String clusterCredential) {
        return "ApiKey " + clusterCredential;
    }
}

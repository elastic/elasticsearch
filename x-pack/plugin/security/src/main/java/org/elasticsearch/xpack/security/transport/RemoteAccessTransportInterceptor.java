/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authz.AuthorizationService;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Supplier;

public class RemoteAccessTransportInterceptor implements TransportInterceptor {

    public static final String REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER = "_remote_access_cluster_credential";
    private static final Logger logger = LogManager.getLogger(RemoteAccessTransportInterceptor.class);

    private final AuthorizationService authzService;
    private final RemoteClusterAuthorizationResolver remoteClusterAuthorizationResolver;
    private final SecurityContext securityContext;

    public RemoteAccessTransportInterceptor(
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

    private boolean shouldSendWithRemoteAccessHeaders(final Transport.Connection connection, final TransportRequest request) {
        if (false == TcpTransport.isUntrustedRemoteClusterEnabled()
            // TODO more request types here
            || false == (request instanceof SearchRequest || request instanceof ClusterSearchShardsRequest)) {
            return false;
        }

        final Optional<String> remoteClusterAlias = RemoteConnectionManager.resolveRemoteClusterAlias(connection);
        if (false == remoteClusterAlias.isPresent()) {
            return false;
        }

        // TODO version check

        final Authentication authentication = securityContext.getAuthentication();
        assert authentication != null : "authentication must be present in context";
        if (authentication.isApiKey()
            || authentication.isServiceAccount()
            || User.isInternal(authentication.getEffectiveSubject().getUser())
            // TODO is this necessary?
            || Arrays.stream(authentication.getEffectiveSubject().getUser().roles()).anyMatch(ReservedRolesStore::isReserved)) {
            // The above authentication subject types or their roles are not yet supported, so use legacy remote cluster security mode
            return false;
        }

        return remoteClusterAuthorizationResolver.resolveAuthorization(remoteClusterAlias.get()) != null;
    }

    private <T extends TransportResponse> void sendWithRemoteAccessHeaders(
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
        assert remoteClusterAlias.isPresent() : "remote cluster alias must be set for the transport connection";
        authzService.retrieveRemoteAccessRoleDescriptorsIntersection(
            remoteClusterAlias.get(),
            authentication.getEffectiveSubject(),
            // TODO wrapPreservingContext?
            ActionListener.wrap(roleDescriptorsIntersection -> {
                final ThreadContext threadContext = securityContext.getThreadContext();
                final Supplier<ThreadContext.StoredContext> contextSupplier = threadContext.newRestorableContext(true);
                try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
                    final RemoteAccessAuthentication remoteAccessAuthentication = new RemoteAccessAuthentication(
                        authentication,
                        roleDescriptorsIntersection
                    );
                    remoteAccessAuthentication.writeToContext(threadContext);
                    writeClusterCredentialToContext(remoteClusterAlias.get(), threadContext);
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

    private void writeClusterCredentialToContext(final String remoteClusterAlias, final ThreadContext threadContext) {
        final String clusterCredential = remoteClusterAuthorizationResolver.resolveAuthorization(remoteClusterAlias);
        // This can happen if the cluster credential after we made the initial check to send remote access headers
        // In this case, fail the request. In the future we may want to retry instead, to pick up the updated remote cluster configuration
        if (clusterCredential == null) {
            throw new IllegalStateException("remote cluster credential unavailable for target cluster [" + remoteClusterAlias + "]");
        }
        threadContext.putHeader(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER, withApiKeyPrefix(clusterCredential));
    }

    private String withApiKeyPrefix(final String clusterCredential) {
        return "ApiKey " + clusterCredential;
    }
}

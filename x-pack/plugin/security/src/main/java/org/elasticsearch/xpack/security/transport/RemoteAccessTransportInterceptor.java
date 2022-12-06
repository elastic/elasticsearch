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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.transport.ActionTransportException;
import org.elasticsearch.transport.RemoteConnectionManager;
import org.elasticsearch.transport.SendRequestTransportException;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.RemoteAccessAuthentication;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authz.AuthorizationService;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Supplier;

import static org.elasticsearch.action.support.ContextPreservingActionListener.wrapPreservingContext;

public class RemoteAccessTransportInterceptor implements TransportInterceptor {

    public static final String REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER = "_remote_access_cluster_credential";
    public static final Version VERSION_REMOTE_ACCESS_HEADERS = Version.V_8_7_0;
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
                    sendWithRemoteAccessHeaders(connection, action, request, options, handler);
                } else {
                    // Send regular request, without remote access headers
                    sendRequestWithWrappedException(connection, action, request, options, handler);
                }
            }

            private <T extends TransportResponse> void sendWithRemoteAccessHeaders(
                final Transport.Connection connection,
                final String action,
                final TransportRequest request,
                final TransportRequestOptions options,
                final TransportResponseHandler<T> handler
            ) {
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
                final Optional<String> remoteClusterAlias = RemoteConnectionManager.resolveRemoteClusterAlias(connection);
                assert remoteClusterAlias.isPresent() : "remote cluster alias must be set for the transport connection";
                authzService.retrieveRemoteAccessRoleDescriptorsIntersection(
                    remoteClusterAlias.get(),
                    authentication.getEffectiveSubject(),
                    wrapPreservingContext(ActionListener.wrap(roleDescriptorsIntersection -> {
                        final ThreadContext threadContext = securityContext.getThreadContext();
                        final Supplier<ThreadContext.StoredContext> contextSupplier = threadContext.newRestorableContext(true);
                        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
                            final RemoteAccessAuthentication remoteAccessAuthentication = new RemoteAccessAuthentication(
                                authentication,
                                roleDescriptorsIntersection
                            );
                            remoteAccessAuthentication.writeToContext(threadContext);
                            writeClusterCredentialToContext(remoteClusterAlias.get(), threadContext);
                            sendRequestWithWrappedException(
                                connection,
                                action,
                                request,
                                options,
                                new TransportService.ContextRestoreResponseHandler<>(contextSupplier, handler)
                            );
                        }
                    }, e -> handler.handleException(new RemoteAccessTransportException(connection.getNode(), action, e))),
                        securityContext.getThreadContext()
                    )
                );
            }

            private <T extends TransportResponse> void sendRequestWithWrappedException(
                final Transport.Connection connection,
                final String action,
                final TransportRequest request,
                final TransportRequestOptions options,
                final TransportResponseHandler<T> handler
            ) {
                try {
                    sender.sendRequest(connection, action, request, options, handler);
                } catch (Exception e) {
                    handler.handleException(new SendRequestTransportException(connection.getNode(), action, e));
                }
            }
        };
    }

    private boolean shouldSendWithRemoteAccessHeaders(final Transport.Connection connection, final TransportRequest request) {
        // This is not strictly necessary, but it allows us to skip all additional checks below early
        if (false == TcpTransport.isUntrustedRemoteClusterEnabled()) {
            return false;
        }

        final Optional<String> remoteClusterAlias = RemoteConnectionManager.resolveRemoteClusterAlias(connection);
        if (remoteClusterAlias.isEmpty()) {
            return false;
        } else if (false == (request instanceof SearchRequest || request instanceof ClusterSearchShardsRequest)) {
            logger.debug("Request type is not whitelisted for remote access headers");
            return false;
        }

        final Authentication authentication = securityContext.getAuthentication();
        assert authentication != null : "authentication must be present in security context";
        final Subject effectiveSubject = authentication.getEffectiveSubject();
        // We will lift this restriction as we add support for these types (and internal users)
        final boolean isSubjectTypeUnsupported = effectiveSubject.getType().equals(Subject.Type.API_KEY)
            || effectiveSubject.getType().equals(Subject.Type.SERVICE_ACCOUNT)
            || User.isInternal(effectiveSubject.getUser());
        if (isSubjectTypeUnsupported) {
            logger.debug("Effective subject type does not support remote access headers");
            return false;
        } else if (Arrays.stream(effectiveSubject.getUser().roles()).anyMatch(ReservedRolesStore::isReserved)) {
            logger.debug("Effective subject has reserved roles which is not supported for remote access headers");
            return false;
        }

        return remoteClusterAuthorizationResolver.resolveAuthorization(remoteClusterAlias.get()) != null;
    }

    private void writeClusterCredentialToContext(final String remoteClusterAlias, final ThreadContext threadContext) {
        final String clusterCredential = remoteClusterAuthorizationResolver.resolveAuthorization(remoteClusterAlias);
        // This can happen if the cluster credential was updated after we made the initial check to send remote access headers
        // In this case, fail the request. In the future we may want to retry instead, to pick up the updated remote cluster configuration
        if (clusterCredential == null) {
            throw new IllegalStateException("remote cluster credential unavailable for target cluster [" + remoteClusterAlias + "]");
        }
        threadContext.putHeader(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER, withApiKeyPrefix(clusterCredential));
    }

    private String withApiKeyPrefix(final String clusterCredential) {
        return "ApiKey " + clusterCredential;
    }

    static final class RemoteAccessTransportException extends ActionTransportException {
        RemoteAccessTransportException(DiscoveryNode node, String action, Throwable cause) {
            super(node == null ? null : node.getName(), node == null ? null : node.getAddress(), action, cause);
        }
    }
}

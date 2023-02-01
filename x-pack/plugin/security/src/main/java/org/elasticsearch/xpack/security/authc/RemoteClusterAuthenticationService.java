/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.RemoteAccessAuthentication;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.security.authc.RemoteAccessAuthentication.REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY;
import static org.elasticsearch.xpack.security.transport.SecurityServerTransportInterceptor.REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY;

public class RemoteClusterAuthenticationService {

    private static final RoleDescriptor CROSS_CLUSTER_SEARCH_ROLE = new RoleDescriptor(
        "_cross_cluster_search",
        new String[] { ClusterStateAction.NAME },
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
    private static final Logger logger = LogManager.getLogger(RemoteClusterAuthenticationService.class);

    private final AuthenticationService authenticationService;

    public RemoteClusterAuthenticationService(AuthenticationService authenticationService) {
        this.authenticationService = authenticationService;
    }

    public void authenticate(
        final String action,
        final TransportRequest request,
        final boolean allowAnonymous,
        final ActionListener<Authentication> listener
    ) {
        final Authenticator.Context authcContext = authenticationService.newContext(action, request, allowAnonymous);
        final ThreadContext threadContext = authcContext.getThreadContext();

        if (threadContext.getHeader(AuthenticationField.AUTHENTICATION_KEY) != null) {
            withRequestProcessingFailure(
                authcContext,
                new IllegalArgumentException("authentication header is not allowed with remote access"),
                listener
            );
            return;
        }

        final RemoteAccessHeaders remoteAccessHeaders;
        try {
            remoteAccessHeaders = RemoteAccessHeaders.readFromContext(threadContext);
        } catch (Exception ex) {
            withRequestProcessingFailure(authcContext, ex, listener);
            return;
        }

        try (
            ThreadContext.StoredContext ignored = threadContext.newStoredContext(
                Collections.emptyList(),
                // drop remote access authentication headers since we've read their values, and we want to maintain the invariant that
                // either the remote access authentication header is in the context, or the authentication header, but not both
                List.of(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY, REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY)
            )
        ) {
            final Supplier<ThreadContext.StoredContext> storedContextSupplier = threadContext.newRestorableContext(true);
            // Write remote access credential to the Authorization header, so we can re-use generic authentication service functionality for
            // authc token extraction
            threadContext.putHeader("Authorization", remoteAccessHeaders.clusterCredentialHeader());
            authenticationService.authenticate(
                authcContext,
                new ContextPreservingActionListener<>(storedContextSupplier, ActionListener.wrap(authentication -> {
                    final RemoteAccessAuthentication remoteAccessAuthentication = remoteAccessHeaders.remoteAccessAuthentication();
                    final Authentication receivedAuthentication = remoteAccessAuthentication.getAuthentication();
                    final User user = receivedAuthentication.getEffectiveSubject().getUser();
                    final Authentication finalAuthentication;
                    if (SystemUser.is(user)) {
                        finalAuthentication = authentication.toRemoteAccess(
                            new RemoteAccessAuthentication(
                                receivedAuthentication,
                                new RoleDescriptorsIntersection(CROSS_CLUSTER_SEARCH_ROLE)
                            )
                        );
                    } else if (User.isInternal(user)) {
                        throw new IllegalArgumentException(
                            "received cross cluster request from an unexpected internal user [" + user.principal() + "]"
                        );
                    } else {
                        finalAuthentication = authentication.toRemoteAccess(remoteAccessAuthentication);
                    }
                    writeAuthToContext(authcContext, finalAuthentication, listener);
                }, ex -> withRequestProcessingFailure(authcContext, ex, listener)))
            );
        }
    }

    private record RemoteAccessHeaders(String clusterCredentialHeader, RemoteAccessAuthentication remoteAccessAuthentication) {
        static RemoteAccessHeaders readFromContext(ThreadContext threadContext) throws IOException {
            final String clusterCredentialHeader = threadContext.getHeader(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY);
            if (clusterCredentialHeader == null) {
                throw new IllegalArgumentException(
                    "remote access header [" + REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY + "] is required"
                );
            }
            return new RemoteAccessHeaders(clusterCredentialHeader, RemoteAccessAuthentication.readFromContext(threadContext));
        }
    }

    private static void withRequestProcessingFailure(
        final Authenticator.Context context,
        final Exception ex,
        final ActionListener<Authentication> listener
    ) {
        final ElasticsearchSecurityException ese = context.getRequest()
            .exceptionProcessingRequest(ex, context.getMostRecentAuthenticationToken());
        context.addUnsuccessfulMessageToMetadata(ese);
        listener.onFailure(ese);
    }

    private void writeAuthToContext(
        final Authenticator.Context context,
        final Authentication authentication,
        final ActionListener<Authentication> listener
    ) {
        try {
            authentication.writeToContext(context.getThreadContext());
            // TODO specialize auditing via remoteAccessAuthenticationSuccess()?
            context.getRequest().authenticationSuccess(authentication);
        } catch (Exception e) {
            logger.debug(() -> format("Failed to store authentication [%s] for request [%s]", authentication, context.getRequest()), e);
            withRequestProcessingFailure(context, e, listener);
            return;
        }
        logger.trace("Established authentication [{}] for request [{}]", authentication, context.getRequest());
        listener.onResponse(authentication);
    }
}

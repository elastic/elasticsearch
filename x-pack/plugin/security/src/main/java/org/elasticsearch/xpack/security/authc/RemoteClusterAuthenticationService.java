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
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.RemoteAccessAuthentication;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.Collections;
import java.util.List;

import static org.elasticsearch.action.support.ContextPreservingActionListener.wrapPreservingContext;
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
        authenticate(authenticationService.newContext(action, request, allowAnonymous), listener);
    }

    private void authenticate(final Authenticator.Context context, final ActionListener<Authentication> listener) {
        // TODO restore context after executing?
        final ThreadContext threadContext = context.getThreadContext();
        try {
            if (threadContext.getHeader(AuthenticationField.AUTHENTICATION_KEY) != null) {
                throw new IllegalArgumentException("authentication header is not allowed");
            } else if (threadContext.getHeader(REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY) == null) {
                throw new IllegalArgumentException("remote access header [" + REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY + "] is required");
            }
            final String credentialsHeader = threadContext.getHeader(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY);
            if (credentialsHeader == null) {
                throw new IllegalArgumentException(
                    "remote access header [" + REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY + "] is required"
                );
            }
            // Write remote access credential to the Authorization header, so we can re-use generic authentication service functionality for
            // authc token extraction
            threadContext.putHeader("Authorization", credentialsHeader);
        } catch (Exception ex) {
            withRequestProcessingFailure(context, ex, listener);
            return;
        }

        authenticationService.authenticate(context, wrapPreservingContext(ActionListener.wrap(authentication -> {
            assert threadContext.getHeader(AuthenticationField.AUTHENTICATION_KEY) == null : "authentication header is not allowed";
            final RemoteAccessAuthentication remoteAccessAuthentication = RemoteAccessAuthentication.readFromContext(threadContext);
            try (
                ThreadContext.StoredContext ignored = threadContext.newStoredContext(
                    Collections.emptyList(),
                    // drop remote access authentication headers
                    List.of(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY, REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY)
                )
            ) {
                final Authentication receivedAuthentication = remoteAccessAuthentication.getAuthentication();
                final User user = receivedAuthentication.getEffectiveSubject().getUser();
                final Authentication successfulAuthentication;
                if (SystemUser.is(user)) {
                    successfulAuthentication = authentication.toRemoteAccess(
                        new RemoteAccessAuthentication(receivedAuthentication, new RoleDescriptorsIntersection(CROSS_CLUSTER_SEARCH_ROLE))
                    );
                } else if (User.isInternal(user)) {
                    throw new IllegalArgumentException(
                        "received cross cluster request from an unexpected internal user [" + user.principal() + "]"
                    );
                } else {
                    successfulAuthentication = authentication.toRemoteAccess(remoteAccessAuthentication);
                }
                writeAuthToContext(context, successfulAuthentication, listener);
            }
        }, ex -> withRequestProcessingFailure(context, ex, listener)), threadContext));
    }

    private void withRequestProcessingFailure(
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

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
import org.elasticsearch.xpack.core.security.authc.Subject;
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

public class RemoteAccessAuthenticationService {

    private static final RoleDescriptor CROSS_CLUSTER_INTERNAL_ROLE = new RoleDescriptor(
        "_cross_cluster_internal",
        new String[] { ClusterStateAction.NAME },
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
    private static final Logger logger = LogManager.getLogger(RemoteAccessAuthenticationService.class);
    private final ApiKeyService apiKeyService;
    private final AuthenticationService authenticationService;

    public RemoteAccessAuthenticationService(ApiKeyService apiKeyService, AuthenticationService authenticationService) {
        this.apiKeyService = apiKeyService;
        this.authenticationService = authenticationService;
    }

    public AuthenticationService getAuthenticationService() {
        return authenticationService;
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
            remoteAccessHeaders = RemoteAccessHeaders.readFromContext(apiKeyService, threadContext);
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
            final Supplier<ThreadContext.StoredContext> storedContextSupplier = threadContext.newRestorableContext(false);
            authcContext.addAuthenticationToken(remoteAccessHeaders.clusterCredential);
            authenticationService.authenticate(
                authcContext,
                new ContextPreservingActionListener<>(storedContextSupplier, ActionListener.wrap(authentication -> {
                    assert authentication.isApiKey() : "initial authentication for remote access must be by API key";
                    final RemoteAccessAuthentication remoteAccessAuthentication = remoteAccessHeaders.remoteAccessAuthentication();
                    final Authentication receivedAuthentication = remoteAccessAuthentication.getAuthentication();
                    final Subject receivedEffectiveSubject = receivedAuthentication.getEffectiveSubject();
                    final User user = receivedEffectiveSubject.getUser();

                    final Authentication finalAuthentication;
                    if (SystemUser.is(user)) {
                        finalAuthentication = authentication.toRemoteAccess(
                            new RemoteAccessAuthentication(
                                Authentication.newInternalAuthentication(
                                    SystemUser.INSTANCE,
                                    receivedEffectiveSubject.getTransportVersion(),
                                    receivedEffectiveSubject.getRealm().getNodeName()
                                ),
                                new RoleDescriptorsIntersection(CROSS_CLUSTER_INTERNAL_ROLE)
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

    private record RemoteAccessHeaders(
        ApiKeyService.ApiKeyCredentials clusterCredential,
        RemoteAccessAuthentication remoteAccessAuthentication
    ) {
        static RemoteAccessHeaders readFromContext(ApiKeyService apiKeyService, ThreadContext threadContext) throws IOException {
            apiKeyService.ensureEnabled();
            final String clusterCredentialHeader = threadContext.getHeader(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY);
            if (clusterCredentialHeader == null) {
                throw new IllegalArgumentException(
                    "remote access header [" + REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY + "] is required"
                );
            }
            final ApiKeyService.ApiKeyCredentials apiKeyCredential = apiKeyService.getCredentialsFromHeader(clusterCredentialHeader);
            if (apiKeyCredential == null) {
                throw new IllegalArgumentException(
                    "remote access header [" + REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY + "] value must be a valid API key credential"
                );
            }
            return new RemoteAccessHeaders(apiKeyCredential, RemoteAccessAuthentication.readFromContext(threadContext));
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

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
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.RemoteAccessAuthentication;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.security.authc.RemoteAccessAuthentication.REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY;
import static org.elasticsearch.xpack.security.transport.SecurityServerTransportInterceptor.REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY;

public class RemoteAccessAuthenticationService {

    public static final TransportVersion VERSION_REMOTE_ACCESS_AUTHENTICATION = TransportVersion.V_8_8_0;

    public static final RoleDescriptor CROSS_CLUSTER_INTERNAL_ROLE = new RoleDescriptor(
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

    private final ClusterService clusterService;
    private final ApiKeyService apiKeyService;
    private final AuthenticationService authenticationService;

    public RemoteAccessAuthenticationService(
        ClusterService clusterService,
        ApiKeyService apiKeyService,
        AuthenticationService authenticationService
    ) {
        this.clusterService = clusterService;
        this.apiKeyService = apiKeyService;
        this.authenticationService = authenticationService;
    }

    public void authenticate(final String action, final TransportRequest request, final ActionListener<Authentication> listener) {
        final Authenticator.Context authcContext = authenticationService.newContext(action, request, false);
        final ThreadContext threadContext = authcContext.getThreadContext();

        // TODO revisit this once Node's Version is refactored
        if (getMinNodeTransportVersion().before(VERSION_REMOTE_ACCESS_AUTHENTICATION)) {
            withRequestProcessingFailure(
                authcContext,
                new IllegalArgumentException(
                    "all nodes must have version ["
                        + VERSION_REMOTE_ACCESS_AUTHENTICATION
                        + "] or higher to support cross cluster requests through the dedicated remote cluster port"
                ),
                listener
            );
            return;
        }

        final RemoteAccessHeaders remoteAccessHeaders;
        try {
            remoteAccessHeaders = extractRemoteAccessHeaders(threadContext);
        } catch (Exception ex) {
            withRequestProcessingFailure(authcContext, ex, listener);
            return;
        }

        // This is ensured by the RemoteAccessServerTransportFilter -- validating the internal consistency here
        assert threadContext.getHeaders().keySet().stream().noneMatch(ClientHelper.SECURITY_HEADER_FILTERS::contains);
        try (
            ThreadContext.StoredContext ignored = threadContext.newStoredContext(
                Collections.emptyList(),
                // drop remote access authentication headers since we've read their values, and we want to maintain the invariant that
                // either the remote access authentication header is in the context, or the authentication header, but not both
                List.of(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY, REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY)
            )
        ) {
            final Supplier<ThreadContext.StoredContext> storedContextSupplier = threadContext.newRestorableContext(false);
            authcContext.addAuthenticationToken(remoteAccessHeaders.clusterCredential());
            authenticationService.authenticate(
                authcContext,
                new ContextPreservingActionListener<>(storedContextSupplier, ActionListener.wrap(authentication -> {
                    assert authentication.isApiKey() : "initial authentication for remote access must be by API key";
                    assert false == authentication.isRunAs() : "initial authentication for remote access cannot be run-as";
                    final RemoteAccessAuthentication remoteAccessAuthentication = remoteAccessHeaders.remoteAccessAuthentication();
                    validate(remoteAccessAuthentication);
                    writeAuthToContext(
                        authcContext,
                        authentication.toRemoteAccess(maybeRewriteForSystemUser(remoteAccessAuthentication)),
                        listener
                    );
                }, ex -> withRequestProcessingFailure(authcContext, ex, listener)))
            );
        }
    }

    private static RemoteAccessAuthentication maybeRewriteForSystemUser(RemoteAccessAuthentication remoteAccessAuthentication)
        throws IOException {
        final Subject receivedEffectiveSubject = remoteAccessAuthentication.getAuthentication().getEffectiveSubject();
        final User user = receivedEffectiveSubject.getUser();
        if (SystemUser.is(user)) {
            return new RemoteAccessAuthentication(
                Authentication.newInternalAuthentication(
                    SystemUser.INSTANCE,
                    receivedEffectiveSubject.getTransportVersion(),
                    receivedEffectiveSubject.getRealm().getNodeName()
                ),
                new RoleDescriptorsIntersection(CROSS_CLUSTER_INTERNAL_ROLE)
            );
        } else if (User.isInternal(user)) {
            throw new IllegalArgumentException(
                "received cross cluster request from an unexpected internal user [" + user.principal() + "]"
            );
        } else {
            return remoteAccessAuthentication;
        }
    }

    public AuthenticationService getAuthenticationService() {
        return authenticationService;
    }

    private record RemoteAccessHeaders(
        ApiKeyService.ApiKeyCredentials clusterCredential,
        RemoteAccessAuthentication remoteAccessAuthentication
    ) {}

    private RemoteAccessHeaders extractRemoteAccessHeaders(final ThreadContext threadContext) throws IOException {
        apiKeyService.ensureEnabled();
        final String clusterCredentialHeader = threadContext.getHeader(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY);
        if (clusterCredentialHeader == null) {
            throw new IllegalArgumentException("remote access header [" + REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY + "] is required");
        }
        final ApiKeyService.ApiKeyCredentials apiKeyCredential = apiKeyService.getCredentialsFromHeader(clusterCredentialHeader);
        if (apiKeyCredential == null) {
            throw new IllegalArgumentException(
                "remote access header [" + REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY + "] value must be a valid API key credential"
            );
        }
        if (threadContext.getHeader(REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY) == null) {
            throw new IllegalArgumentException("remote access header [" + REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY + "] is required");
        }
        return new RemoteAccessHeaders(apiKeyCredential, RemoteAccessAuthentication.readFromContext(threadContext));
    }

    private void validate(final RemoteAccessAuthentication remoteAccessAuthentication) {
        final Subject effectiveSubject = remoteAccessAuthentication.getAuthentication().getEffectiveSubject();
        for (RemoteAccessAuthentication.RoleDescriptorsBytes roleDescriptorsBytes : remoteAccessAuthentication
            .getRoleDescriptorsBytesList()) {
            final Set<RoleDescriptor> roleDescriptors = roleDescriptorsBytes.toRoleDescriptors();
            for (RoleDescriptor roleDescriptor : roleDescriptors) {
                final boolean privilegesOtherThanIndex = roleDescriptor.hasClusterPrivileges()
                    || roleDescriptor.hasConfigurableClusterPrivileges()
                    || roleDescriptor.hasApplicationPrivileges()
                    || roleDescriptor.hasRunAs()
                    || roleDescriptor.hasRemoteIndicesPrivileges();
                if (privilegesOtherThanIndex) {
                    throw new IllegalArgumentException(
                        "role descriptor for remote access can only contain index privileges but other privileges found for subject ["
                            + effectiveSubject.getUser().principal()
                            + "]"
                    );
                }
            }
        }
        if (false == effectiveSubject.getType().equals(Subject.Type.USER)) {
            throw new IllegalArgumentException(
                "subject ["
                    + effectiveSubject.getUser().principal()
                    + "] has type ["
                    + effectiveSubject.getType()
                    + "] which is not supported for remote access"
            );
        }
    }

    private TransportVersion getMinNodeTransportVersion() {
        return clusterService.state().nodes().getMinNodeVersion().transportVersion;
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

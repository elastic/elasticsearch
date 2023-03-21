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
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo;
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
import static org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo.CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY;
import static org.elasticsearch.xpack.security.authc.CrossClusterAccessHeaders.CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY;

public class CrossClusterAccessAuthenticationService {

    public static final Version VERSION_CROSS_CLUSTER_ACCESS_AUTHENTICATION = Version.V_8_8_0;

    public static final RoleDescriptor CROSS_CLUSTER_INTERNAL_ROLE = new RoleDescriptor(
        "_cross_cluster_internal",
        new String[] { "cross_cluster_access" },
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
    private static final Logger logger = LogManager.getLogger(CrossClusterAccessAuthenticationService.class);

    private final ClusterService clusterService;
    private final ApiKeyService apiKeyService;
    private final AuthenticationService authenticationService;

    public CrossClusterAccessAuthenticationService(
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

        final CrossClusterAccessHeaders crossClusterAccessHeaders;
        try {
            // parse and add as authentication token as early as possible so that failure events in audit log include API key ID
            crossClusterAccessHeaders = CrossClusterAccessHeaders.readFromContext(threadContext);
            authcContext.addAuthenticationToken(crossClusterAccessHeaders.credentials());
            apiKeyService.ensureEnabled();
        } catch (Exception ex) {
            withRequestProcessingFailure(authcContext, ex, listener);
            return;
        }

        if (getMinNodeVersion().before(VERSION_CROSS_CLUSTER_ACCESS_AUTHENTICATION)) {
            withRequestProcessingFailure(
                authcContext,
                new IllegalArgumentException(
                    "all nodes must have version ["
                        + VERSION_CROSS_CLUSTER_ACCESS_AUTHENTICATION
                        + "] or higher to support cross cluster requests through the dedicated remote cluster port"
                ),
                listener
            );
            return;
        }

        // This is ensured by the RemoteAccessServerTransportFilter -- validating the internal consistency here
        assert threadContext.getHeaders().keySet().stream().noneMatch(ClientHelper.SECURITY_HEADER_FILTERS::contains);
        try (
            ThreadContext.StoredContext ignored = threadContext.newStoredContext(
                Collections.emptyList(),
                // drop cross cluster access authentication headers since we've read their values, and we want to maintain the invariant
                // that either the cross cluster access subject info header is in the context, or the authentication header, but not both
                List.of(CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY, CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY)
            )
        ) {
            final Supplier<ThreadContext.StoredContext> storedContextSupplier = threadContext.newRestorableContext(false);
            authenticationService.authenticate(
                authcContext,
                new ContextPreservingActionListener<>(storedContextSupplier, ActionListener.wrap(authentication -> {
                    assert authentication.isApiKey() : "initial authentication for cross cluster access must be by API key";
                    assert false == authentication.isRunAs() : "initial authentication for cross cluster access cannot be run-as";
                    // try-catch so any failure here is wrapped by `withRequestProcessingFailure`, whereas `authenticate` failures are not
                    // we should _not_ wrap `authenticate` failures since this produces duplicate audit events
                    try {
                        final CrossClusterAccessSubjectInfo crossClusterAccessSubjectInfo = crossClusterAccessHeaders.subjectInfo();
                        validate(crossClusterAccessSubjectInfo);
                        writeAuthToContext(
                            authcContext,
                            authentication.toCrossClusterAccess(maybeRewriteForSystemUser(crossClusterAccessSubjectInfo)),
                            listener
                        );
                    } catch (Exception ex) {
                        withRequestProcessingFailure(authcContext, ex, listener);
                    }
                }, listener::onFailure))
            );
        }
    }

    private static CrossClusterAccessSubjectInfo maybeRewriteForSystemUser(
        final CrossClusterAccessSubjectInfo crossClusterAccessSubjectInfo
    ) throws IOException {
        final Subject receivedEffectiveSubject = crossClusterAccessSubjectInfo.getAuthentication().getEffectiveSubject();
        final User user = receivedEffectiveSubject.getUser();
        if (SystemUser.is(user)) {
            return new CrossClusterAccessSubjectInfo(
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
            return crossClusterAccessSubjectInfo;
        }
    }

    public AuthenticationService getAuthenticationService() {
        return authenticationService;
    }

    private void validate(final CrossClusterAccessSubjectInfo crossClusterAccessSubjectInfo) {
        final Authentication authentication = crossClusterAccessSubjectInfo.getAuthentication();
        authentication.checkConsistency();
        final Subject effectiveSubject = authentication.getEffectiveSubject();
        if (false == effectiveSubject.getType().equals(Subject.Type.USER)
            && false == effectiveSubject.getType().equals(Subject.Type.SERVICE_ACCOUNT)) {
            throw new IllegalArgumentException(
                "subject ["
                    + effectiveSubject.getUser().principal()
                    + "] has type ["
                    + effectiveSubject.getType()
                    + "] which is not supported for cross cluster access"
            );
        }

        for (CrossClusterAccessSubjectInfo.RoleDescriptorsBytes roleDescriptorsBytes : crossClusterAccessSubjectInfo
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
                        "role descriptor for cross cluster access can only contain index privileges "
                            + "but other privileges found for subject ["
                            + effectiveSubject.getUser().principal()
                            + "]"
                    );
                }
            }
        }
    }

    private Version getMinNodeVersion() {
        return clusterService.state().nodes().getMinNodeVersion();
    }

    private static void withRequestProcessingFailure(
        final Authenticator.Context context,
        final Exception ex,
        final ActionListener<Authentication> listener
    ) {
        logger.debug(() -> format("Failed to authenticate cross cluster access for request [%s]", context.getRequest()), ex);
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

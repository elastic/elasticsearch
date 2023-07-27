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
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKey;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.transport.RemoteClusterPortSettings.TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY_CCR;
import static org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo.CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY;
import static org.elasticsearch.xpack.security.authc.CrossClusterAccessHeaders.CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY;

public class CrossClusterAccessAuthenticationService {

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
            final ApiKeyService.ApiKeyCredentials apiKeyCredentials = crossClusterAccessHeaders.credentials();
            assert ApiKey.Type.CROSS_CLUSTER == apiKeyCredentials.getExpectedType();
            authcContext.addAuthenticationToken(apiKeyCredentials);
            apiKeyService.ensureEnabled();
        } catch (Exception ex) {
            withRequestProcessingFailure(authcContext, ex, listener);
            return;
        }

        // This check is to ensure all nodes understand cross_cluster_access subject type
        if (getMinTransportVersion().before(TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY_CCR)) {
            withRequestProcessingFailure(
                authcContext,
                new IllegalArgumentException(
                    "all nodes must have transport version ["
                        + TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY_CCR
                        + "] or higher to support cross cluster requests through the dedicated remote cluster port"
                ),
                listener
            );
            return;
        }

        // This is ensured by CrossClusterAccessServerTransportFilter -- validating for internal consistency here
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
                        final CrossClusterAccessSubjectInfo subjectInfo = crossClusterAccessHeaders.getCleanAndValidatedSubjectInfo();
                        writeAuthToContext(authcContext, authentication.toCrossClusterAccess(subjectInfo), listener);
                    } catch (Exception ex) {
                        withRequestProcessingFailure(authcContext, ex, listener);
                    }
                }, listener::onFailure))
            );
        }
    }

    public AuthenticationService getAuthenticationService() {
        return authenticationService;
    }

    private TransportVersion getMinTransportVersion() {
        return clusterService.state().getMinTransportVersion();
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

    private static void writeAuthToContext(
        final Authenticator.Context context,
        final Authentication authentication,
        final ActionListener<Authentication> listener
    ) {
        try {
            authentication.writeToContext(context.getThreadContext());
            context.getRequest().authenticationSuccess(authentication);
        } catch (Exception e) {
            logger.debug(
                () -> format("Failed to store authentication [%s] for cross cluster request [%s]", authentication, context.getRequest()),
                e
            );
            withRequestProcessingFailure(context, e, listener);
            return;
        }
        logger.trace("Established authentication [{}] for cross cluster request [{}]", authentication, context.getRequest());
        listener.onResponse(authentication);
    }
}

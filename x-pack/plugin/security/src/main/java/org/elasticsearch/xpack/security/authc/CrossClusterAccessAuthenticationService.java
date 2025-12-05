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
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKey;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo;
import org.elasticsearch.xpack.core.security.support.Exceptions;
import org.elasticsearch.xpack.security.transport.CrossClusterApiKeySignatureManager;
import org.elasticsearch.xpack.security.transport.X509CertificateSignature;

import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo.CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY;
import static org.elasticsearch.xpack.security.authc.CrossClusterAccessHeaders.CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY;
import static org.elasticsearch.xpack.security.authc.CrossClusterAccessHeaders.getCertificateIdentity;
import static org.elasticsearch.xpack.security.transport.X509CertificateSignature.CROSS_CLUSTER_ACCESS_SIGNATURE_HEADER_KEY;

public class CrossClusterAccessAuthenticationService implements RemoteClusterAuthenticationService {

    private static final Logger logger = LogManager.getLogger(CrossClusterAccessAuthenticationService.class);

    private final ClusterService clusterService;
    private final ApiKeyService apiKeyService;
    private final AuthenticationService authenticationService;
    private final CrossClusterApiKeySignatureManager.Verifier crossClusterApiKeySignatureVerifier;

    public CrossClusterAccessAuthenticationService(
        ClusterService clusterService,
        ApiKeyService apiKeyService,
        AuthenticationService authenticationService,
        CrossClusterApiKeySignatureManager.Verifier crossClusterApiKeySignatureVerifier
    ) {
        this.clusterService = clusterService;
        this.apiKeyService = apiKeyService;
        this.authenticationService = authenticationService;
        this.crossClusterApiKeySignatureVerifier = crossClusterApiKeySignatureVerifier;
    }

    @Override
    public void authenticate(final String action, final TransportRequest request, final ActionListener<Authentication> listener) {
        final ThreadContext threadContext = clusterService.threadPool().getThreadContext();
        final CrossClusterAccessHeaders crossClusterAccessHeaders;
        final Authenticator.Context authcContext;
        try {
            // parse and add as authentication token as early as possible so that failure events in audit log include API key ID
            crossClusterAccessHeaders = CrossClusterAccessHeaders.readFromContext(threadContext);
            // Extract credentials, including certificate identity from the optional signature without actually verifying the signature
            final ApiKeyService.ApiKeyCredentials apiKeyCredentials = crossClusterAccessHeaders.credentials();
            assert ApiKey.Type.CROSS_CLUSTER == apiKeyCredentials.getExpectedType();
            // authn must verify only the provided api key and not try to extract any other credential from the thread context
            authcContext = authenticationService.newContext(action, request, apiKeyCredentials);
            var signingInfo = crossClusterAccessHeaders.signature();

            // Verify the signing info if provided. The signing info contains both the signature and the certificate identity, but only the
            // signature is validated here. The certificate identity is validated later as part of the ApiKeyCredentials validation
            if (signingInfo != null && verifySignature(authcContext, signingInfo, crossClusterAccessHeaders, listener) == false) {
                return;
            }
        } catch (Exception ex) {
            withRequestProcessingFailure(authenticationService.newContext(action, request, null), ex, listener);
            return;
        }

        try {
            apiKeyService.ensureEnabled();
        } catch (Exception ex) {
            withRequestProcessingFailure(authcContext, ex, listener);
            return;
        }

        // This check is to ensure all nodes understand cross_cluster_access subject type
        if (getMinTransportVersion().before(TransportVersions.V_8_10_X)) {
            withRequestProcessingFailure(
                authcContext,
                new IllegalArgumentException(
                    "all nodes must have version ["
                        + TransportVersions.V_8_10_X.toReleaseVersion()
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

    private boolean verifySignature(
        Authenticator.Context context,
        X509CertificateSignature signature,
        CrossClusterAccessHeaders crossClusterAccessHeaders,
        ActionListener<Authentication> listener
    ) {
        assert signature.certificates().length > 0 : "Signatures without certificates should not be considered for verification";
        ElasticsearchSecurityException authException = null;
        try {
            if (crossClusterApiKeySignatureVerifier.verify(signature, crossClusterAccessHeaders.signablePayload()) == false) {
                logger.debug(Strings.format("Invalid cross cluster api key signature received [%s]", signature));
                authException = Exceptions.authenticationError(
                    "Invalid cross cluster api key signature from [{}]",
                    X509CertificateSignature.certificateToString(signature.leafCertificate())
                );
            }
        } catch (GeneralSecurityException securityException) {
            logger.debug(Strings.format("Failed to verify cross cluster api key signature certificate [%s]", signature), securityException);
            authException = Exceptions.authenticationError(
                "Failed to verify cross cluster api key signature certificate from [{}]",
                X509CertificateSignature.certificateToString(signature.leafCertificate())
            );
        }
        if (authException != null) {
            // TODO handle audit logging
            listener.onFailure(context.getRequest().exceptionProcessingRequest(authException, context.getMostRecentAuthenticationToken()));
            return false;
        }
        return true;
    }

    @Override
    public void authenticateHeaders(Map<String, String> headers, ActionListener<Void> listener) {
        final ApiKeyService.ApiKeyCredentials credentials;
        try {
            credentials = extractApiKeyCredentialsFromHeaders(headers);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        tryAuthenticate(credentials, listener);
    }

    // package-private for testing
    void tryAuthenticate(ApiKeyService.ApiKeyCredentials credentials, ActionListener<Void> listener) {
        Objects.requireNonNull(credentials);
        apiKeyService.tryAuthenticate(clusterService.threadPool().getThreadContext(), credentials, ActionListener.wrap(authResult -> {
            if (authResult.isAuthenticated()) {
                logger.trace("Cross cluster credentials authentication successful for [{}]", credentials.principal());
                listener.onResponse(null);
                return;
            }
            // TODO handle audit logging
            if (authResult.getStatus() == AuthenticationResult.Status.TERMINATE) {
                Exception e = (authResult.getException() != null)
                    ? authResult.getException()
                    : Exceptions.authenticationError(authResult.getMessage());
                logger.debug(() -> "API key service terminated authentication", e);
                listener.onFailure(e);
            } else {
                if (authResult.getMessage() != null) {
                    if (authResult.getException() != null) {
                        logger.warn(
                            () -> format("Authentication using apikey failed - %s", authResult.getMessage()),
                            authResult.getException()
                        );
                    } else {
                        logger.warn("Authentication using apikey failed - {}", authResult.getMessage());
                    }
                }
                listener.onFailure(Exceptions.authenticationError(authResult.getMessage(), authResult.getException()));
            }
        }, e -> listener.onFailure(Exceptions.authenticationError("failed to authenticate cross cluster credentials", e))));
    }

    public ApiKeyService.ApiKeyCredentials extractApiKeyCredentialsFromHeaders(Map<String, String> headers) {
        try {
            apiKeyService.ensureEnabled();
            final String credentials = headers == null ? null : headers.get(CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY);
            if (credentials == null) {
                throw requiredHeaderMissingException(CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY);
            }

            String certificateIdentity = null;
            final String signature = headers.get(CROSS_CLUSTER_ACCESS_SIGNATURE_HEADER_KEY);
            if (signature != null) {
                certificateIdentity = getCertificateIdentity(X509CertificateSignature.decode(signature));
            }

            return CrossClusterAccessHeaders.parseCredentialsHeader(credentials, certificateIdentity);
        } catch (Exception ex) {
            throw Exceptions.authenticationError("failed to extract credentials from headers", ex);
        }
    }

    public static IllegalArgumentException requiredHeaderMissingException(String headerKey) {
        return new IllegalArgumentException(
            "Cross cluster requests through the dedicated remote cluster server port require transport header ["
                + headerKey
                + "] but none found. "
                + "Please ensure you have configured remote cluster credentials on the cluster originating the request."
        );
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

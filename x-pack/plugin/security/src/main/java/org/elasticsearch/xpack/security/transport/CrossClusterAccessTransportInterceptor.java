/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.TaskCancellationService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteConnectionManager;
import org.elasticsearch.transport.RemoteConnectionManager.RemoteClusterAliasWithCredentials;
import org.elasticsearch.transport.SendRequestTransportException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo;
import org.elasticsearch.xpack.core.security.user.InternalUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.SslProfile;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.action.SecurityActionMapper;
import org.elasticsearch.xpack.security.audit.AuditUtil;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.CrossClusterAccessAuthenticationService;
import org.elasticsearch.xpack.security.authc.CrossClusterAccessHeaders;
import org.elasticsearch.xpack.security.authz.AuthorizationService;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.transport.RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED;

public class CrossClusterAccessTransportInterceptor implements RemoteClusterTransportInterceptor {

    private static final Logger logger = LogManager.getLogger(CrossClusterAccessTransportInterceptor.class);

    private static final Map<String, String> RCS_INTERNAL_ACTIONS_REPLACEMENTS = Map.of(
        "internal:admin/ccr/restore/session/put",
        "indices:internal/admin/ccr/restore/session/put",
        "internal:admin/ccr/restore/session/clear",
        "indices:internal/admin/ccr/restore/session/clear",
        "internal:admin/ccr/restore/file_chunk/get",
        "indices:internal/admin/ccr/restore/file_chunk/get",
        "internal:data/read/esql/open_exchange",
        "cluster:internal:data/read/esql/open_exchange",
        "internal:data/read/esql/exchange",
        "cluster:internal:data/read/esql/exchange",
        TaskCancellationService.BAN_PARENT_ACTION_NAME,
        TaskCancellationService.REMOTE_CLUSTER_BAN_PARENT_ACTION_NAME,
        TaskCancellationService.CANCEL_CHILD_ACTION_NAME,
        TaskCancellationService.REMOTE_CLUSTER_CANCEL_CHILD_ACTION_NAME
    );

    // Visible for testing
    static final TransportVersion ADD_CROSS_CLUSTER_API_KEY_SIGNATURE = TransportVersion.fromName("add_cross_cluster_api_key_signature");

    private final Function<Transport.Connection, Optional<RemoteClusterAliasWithCredentials>> remoteClusterCredentialsResolver;
    private final CrossClusterAccessAuthenticationService crossClusterAccessAuthcService;
    private final CrossClusterApiKeySignatureManager crossClusterApiKeySignatureManager;
    private final AuthorizationService authzService;
    private final XPackLicenseState licenseState;
    private final SecurityContext securityContext;
    private final ThreadPool threadPool;
    private final Settings settings;

    public CrossClusterAccessTransportInterceptor(
        Settings settings,
        ThreadPool threadPool,
        AuthenticationService authcService,
        AuthorizationService authzService,
        SecurityContext securityContext,
        CrossClusterAccessAuthenticationService crossClusterAccessAuthcService,
        CrossClusterApiKeySignatureManager crossClusterApiKeySignatureManager,
        XPackLicenseState licenseState
    ) {
        this(
            settings,
            threadPool,
            authcService,
            authzService,
            securityContext,
            crossClusterAccessAuthcService,
            crossClusterApiKeySignatureManager,
            licenseState,
            RemoteConnectionManager::resolveRemoteClusterAliasWithCredentials
        );
    }

    // package-protected for testing
    CrossClusterAccessTransportInterceptor(
        Settings settings,
        ThreadPool threadPool,
        AuthenticationService authcService,
        AuthorizationService authzService,
        SecurityContext securityContext,
        CrossClusterAccessAuthenticationService crossClusterAccessAuthcService,
        CrossClusterApiKeySignatureManager crossClusterApiKeySignatureManager,
        XPackLicenseState licenseState,
        Function<Transport.Connection, Optional<RemoteClusterAliasWithCredentials>> remoteClusterCredentialsResolver
    ) {
        this.remoteClusterCredentialsResolver = remoteClusterCredentialsResolver;
        this.crossClusterAccessAuthcService = crossClusterAccessAuthcService;
        this.crossClusterApiKeySignatureManager = crossClusterApiKeySignatureManager;
        this.authzService = authzService;
        this.licenseState = licenseState;
        this.securityContext = securityContext;
        this.threadPool = threadPool;
        this.settings = settings;
    }

    @Override
    public TransportInterceptor.AsyncSender interceptSender(TransportInterceptor.AsyncSender sender) {
        return new TransportInterceptor.AsyncSender() {
            @Override
            public <T extends TransportResponse> void sendRequest(
                Transport.Connection connection,
                String action,
                TransportRequest request,
                TransportRequestOptions options,
                TransportResponseHandler<T> handler
            ) {
                final Optional<RemoteClusterCredentials> remoteClusterCredentials = getRemoteClusterCredentials(connection);
                if (remoteClusterCredentials.isPresent()) {
                    sendWithCrossClusterAccessHeaders(remoteClusterCredentials.get(), connection, action, request, options, handler);
                } else {
                    // Send regular request, without cross cluster access headers
                    try {
                        sender.sendRequest(connection, action, request, options, handler);
                    } catch (Exception e) {
                        handler.handleException(new SendRequestTransportException(connection.getNode(), action, e));
                    }
                }
            }

            /**
             * Returns cluster credentials if the connection is remote, and cluster credentials are set up for the target cluster.
             */
            private Optional<RemoteClusterCredentials> getRemoteClusterCredentials(Transport.Connection connection) {
                final Optional<RemoteClusterAliasWithCredentials> remoteClusterAliasWithCredentials = remoteClusterCredentialsResolver
                    .apply(connection);
                if (remoteClusterAliasWithCredentials.isEmpty()) {
                    logger.trace("Connection is not remote");
                    return Optional.empty();
                }

                final String remoteClusterAlias = remoteClusterAliasWithCredentials.get().clusterAlias();
                final SecureString remoteClusterCredentials = remoteClusterAliasWithCredentials.get().credentials();
                if (remoteClusterCredentials == null) {
                    logger.trace("No cluster credentials are configured for remote cluster [{}]", remoteClusterAlias);
                    return Optional.empty();
                }

                return Optional.of(
                    new RemoteClusterCredentials(remoteClusterAlias, ApiKeyService.withApiKeyPrefix(remoteClusterCredentials.toString()))
                );
            }

            private <T extends TransportResponse> void sendWithCrossClusterAccessHeaders(
                final RemoteClusterCredentials remoteClusterCredentials,
                final Transport.Connection connection,
                final String action,
                final TransportRequest request,
                final TransportRequestOptions options,
                final TransportResponseHandler<T> handler
            ) {
                if (false == Security.ADVANCED_REMOTE_CLUSTER_SECURITY_FEATURE.check(licenseState)) {
                    throw LicenseUtils.newComplianceException(Security.ADVANCED_REMOTE_CLUSTER_SECURITY_FEATURE.getName());
                }
                final String remoteClusterAlias = remoteClusterCredentials.clusterAlias();

                if (connection.getTransportVersion().before(TransportVersions.V_8_10_X)) {
                    throw illegalArgumentExceptionWithDebugLog(
                        "Settings for remote cluster ["
                            + remoteClusterAlias
                            + "] indicate cross cluster access headers should be sent but target cluster version ["
                            + connection.getTransportVersion().toReleaseVersion()
                            + "] does not support receiving them"
                    );
                }

                logger.trace(
                    () -> format(
                        "Sending [%s] request for [%s] action to [%s] with cross cluster access headers",
                        request.getClass(),
                        action,
                        remoteClusterAlias
                    )
                );

                final Authentication authentication = securityContext.getAuthentication();
                assert authentication != null : "authentication must be present in security context";
                var signer = crossClusterApiKeySignatureManager.signerForClusterAlias(remoteClusterCredentials.clusterAlias());
                final User user = authentication.getEffectiveSubject().getUser();
                if (user instanceof InternalUser && false == SystemUser.is(user)) {
                    final String message = "Internal user [" + user.principal() + "] should not be used for cross cluster requests";
                    assert false : message;
                    throw illegalArgumentExceptionWithDebugLog(message);
                } else if (SystemUser.is(user) || action.equals(ClusterStateAction.NAME)) {
                    if (SystemUser.is(user)) {
                        logger.trace(
                            "Request [{}] for action [{}] towards [{}] initiated by the system user. "
                                + "Sending request with internal cross cluster access user headers",
                            request.getClass(),
                            action,
                            remoteClusterAlias
                        );
                    } else {
                        // Use system user for cluster state requests (CCR has many calls of cluster state with end-user context)
                        logger.trace(
                            () -> format(
                                "Switching to the system user for cluster state action towards [{}]. Original user is [%s]",
                                remoteClusterAlias,
                                user
                            )
                        );
                    }
                    final var crossClusterAccessHeaders = new CrossClusterAccessHeaders(
                        remoteClusterCredentials.credentials(),
                        SystemUser.crossClusterAccessSubjectInfo(
                            authentication.getEffectiveSubject().getTransportVersion(),
                            authentication.getEffectiveSubject().getRealm().getNodeName()
                        )
                    );

                    // To be able to enforce index-level privileges under the new remote cluster security model,
                    // we switch from old-style internal actions to their new equivalent indices actions so that
                    // they will be checked for index privileges against the index specified in the requests
                    final String effectiveAction = RCS_INTERNAL_ACTIONS_REPLACEMENTS.getOrDefault(action, action);
                    if (false == effectiveAction.equals(action)) {
                        logger.trace("switching internal action from [{}] to [{}]", action, effectiveAction);
                    }
                    sendWithCrossClusterAccessHeaders(
                        crossClusterAccessHeaders,
                        connection,
                        effectiveAction,
                        signer,
                        request,
                        options,
                        handler
                    );
                } else {
                    assert false == action.startsWith("internal:") : "internal action must be sent with system user";
                    authzService.getRoleDescriptorsIntersectionForRemoteCluster(
                        remoteClusterAlias,
                        connection.getTransportVersion(),
                        authentication.getEffectiveSubject(),
                        ActionListener.wrap(roleDescriptorsIntersection -> {
                            logger.trace(
                                () -> format(
                                    "Subject [%s] has role descriptors intersection [%s] for action [%s] towards remote cluster [%s]",
                                    authentication.getEffectiveSubject(),
                                    roleDescriptorsIntersection,
                                    action,
                                    remoteClusterAlias
                                )
                            );
                            if (roleDescriptorsIntersection.isEmpty()) {
                                throw authzService.remoteActionDenied(
                                    authentication,
                                    SecurityActionMapper.action(action, request),
                                    remoteClusterAlias
                                );
                            }
                            final var crossClusterAccessHeaders = new CrossClusterAccessHeaders(
                                remoteClusterCredentials.credentials(),
                                new CrossClusterAccessSubjectInfo(authentication, roleDescriptorsIntersection)
                            );
                            sendWithCrossClusterAccessHeaders(
                                crossClusterAccessHeaders,
                                connection,
                                action,
                                signer,
                                request,
                                options,
                                handler
                            );
                        }, // it's safe to not use a context restore handler here since `getRoleDescriptorsIntersectionForRemoteCluster`
                           // uses a context preserving listener internally, and `sendWithCrossClusterAccessHeaders` uses a context restore
                           // handler
                            e -> handler.handleException(new SendRequestTransportException(connection.getNode(), action, e))
                        )
                    );
                }
            }

            private <T extends TransportResponse> void sendWithCrossClusterAccessHeaders(
                final CrossClusterAccessHeaders crossClusterAccessHeaders,
                final Transport.Connection connection,
                final String action,
                @Nullable final CrossClusterApiKeySignatureManager.Signer signer,
                final TransportRequest request,
                final TransportRequestOptions options,
                final TransportResponseHandler<T> handler
            ) {
                final ThreadContext threadContext = securityContext.getThreadContext();
                final var contextRestoreHandler = new TransportService.ContextRestoreResponseHandler<>(
                    threadContext.newRestorableContext(true),
                    handler
                );
                try (
                    ThreadContext.StoredContext ignored = threadContext.stashContextPreservingRequestHeaders(
                        ThreadContext.HeadersFor.REMOTE_CLUSTER,
                        AuditUtil.AUDIT_REQUEST_ID
                    )
                ) {
                    if (connection.getTransportVersion().supports(ADD_CROSS_CLUSTER_API_KEY_SIGNATURE)) {
                        crossClusterAccessHeaders.writeToContext(threadContext, signer);
                    } else {
                        crossClusterAccessHeaders.writeToContext(threadContext, null);
                    }
                    sender.sendRequest(connection, action, request, options, contextRestoreHandler);
                } catch (Exception e) {
                    contextRestoreHandler.handleException(new SendRequestTransportException(connection.getNode(), action, e));
                }
            }

            private static IllegalArgumentException illegalArgumentExceptionWithDebugLog(String message) {
                logger.debug(message);
                return new IllegalArgumentException(message);
            }
        };
    }

    @Override
    public boolean isRemoteClusterConnection(Transport.Connection connection) {
        return remoteClusterCredentialsResolver.apply(connection).map(RemoteClusterAliasWithCredentials::clusterAlias).isPresent();
    }

    @Override
    public Optional<ServerTransportFilter> getRemoteProfileTransportFilter(
        SslProfile sslProfile,
        DestructiveOperations destructiveOperations
    ) {
        final SslConfiguration profileConfiguration = sslProfile.configuration();
        final boolean remoteClusterServerEnabled = REMOTE_CLUSTER_SERVER_ENABLED.get(settings);
        final boolean remoteClusterServerSSLEnabled = XPackSettings.REMOTE_CLUSTER_SERVER_SSL_ENABLED.get(settings);
        if (remoteClusterServerEnabled) {
            return Optional.of(
                new CrossClusterAccessServerTransportFilter(
                    crossClusterAccessAuthcService,
                    authzService,
                    threadPool.getThreadContext(),
                    remoteClusterServerSSLEnabled && SSLService.isSSLClientAuthEnabled(profileConfiguration),
                    destructiveOperations,
                    securityContext,
                    licenseState
                )
            );
        }
        return Optional.empty();
    }

    @Override
    public boolean hasRemoteClusterAccessHeadersInContext(SecurityContext securityContext) {
        return securityContext.getThreadContext().getHeader(CrossClusterAccessHeaders.CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY) != null
            || securityContext.getThreadContext()
                .getHeader(CrossClusterAccessSubjectInfo.CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY) != null;
    }

    // package-protected for testing
    CrossClusterApiKeySignatureManager getCrossClusterApiKeySignatureManager() {
        return crossClusterApiKeySignatureManager;
    }

    record RemoteClusterCredentials(String clusterAlias, String credentials) {

        @Override
        public String toString() {
            return "RemoteClusterCredentials{clusterAlias='" + clusterAlias + "', credentials='::es_redacted::'}";
        }
    }

}

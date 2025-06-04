/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchRoleRestrictionException;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DelegatingActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.TransportIndicesAliasesAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.MigrateToDataStreamAction;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.InvalidSelectorException;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.UnsupportedSelectorException;
import org.elasticsearch.action.support.replication.TransportReplicationAction.ConcreteShardRequest;
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationFailureHandler;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authc.esnative.ClientReservedRealm;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AsyncSupplier;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationContext;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationResult;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.EmptyAuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.IndexAuthorizationResult;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.ParentActionAuthorization;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.RequestInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.ResolvedIndices;
import org.elasticsearch.xpack.core.security.authz.RestrictedIndices;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.InternalUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.audit.AuditLevel;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.audit.AuditUtil;
import org.elasticsearch.xpack.security.authz.accesscontrol.wrapper.DlsFlsFeatureTrackingIndicesAccessControlWrapper;
import org.elasticsearch.xpack.security.authz.interceptor.RequestInterceptor;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.operator.OperatorPrivileges.OperatorPrivilegesService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.action.support.ContextPreservingActionListener.wrapPreservingContext;
import static org.elasticsearch.xpack.core.security.SecurityField.setting;
import static org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField.ACTION_SCOPE_AUTHORIZATION_KEYS;
import static org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField.AUTHORIZATION_INFO_KEY;
import static org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField.ORIGINATING_ACTION_KEY;
import static org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl.allowAll;
import static org.elasticsearch.xpack.core.security.support.Exceptions.authorizationError;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.PRINCIPAL_ROLES_FIELD_NAME;

public class AuthorizationService {
    public static final Setting<Boolean> ANONYMOUS_AUTHORIZATION_EXCEPTION_SETTING = Setting.boolSetting(
        setting("authc.anonymous.authz_exception"),
        true,
        Property.NodeScope
    );
    private static final AuthorizationInfo SYSTEM_AUTHZ_INFO = () -> Collections.singletonMap(
        PRINCIPAL_ROLES_FIELD_NAME,
        new String[] { SystemUser.ROLE_NAME }
    );
    private static final String IMPLIED_INDEX_ACTION = TransportIndexAction.NAME + ":op_type/index";
    private static final String IMPLIED_CREATE_ACTION = TransportIndexAction.NAME + ":op_type/create";

    private static final Logger logger = LogManager.getLogger(AuthorizationService.class);

    private final Settings settings;
    private final ClusterService clusterService;
    private final AuditTrailService auditTrailService;
    private final IndicesAndAliasesResolver indicesAndAliasesResolver;
    private final AuthenticationFailureHandler authcFailureHandler;
    private final ThreadContext threadContext;
    private final SecurityContext securityContext;
    private final AnonymousUser anonymousUser;
    private final AuthorizationEngine rbacEngine;
    private final AuthorizationEngine authorizationEngine;
    private final Set<RequestInterceptor> requestInterceptors;
    private final XPackLicenseState licenseState;
    private final OperatorPrivilegesService operatorPrivilegesService;
    private final RestrictedIndices restrictedIndices;
    private final AuthorizationDenialMessages authorizationDenialMessages;
    private final ProjectResolver projectResolver;

    private final boolean isAnonymousEnabled;
    private final boolean anonymousAuthzExceptionEnabled;
    private final DlsFlsFeatureTrackingIndicesAccessControlWrapper indicesAccessControlWrapper;

    public AuthorizationService(
        Settings settings,
        CompositeRolesStore rolesStore,
        FieldPermissionsCache fieldPermissionsCache,
        ClusterService clusterService,
        AuditTrailService auditTrailService,
        AuthenticationFailureHandler authcFailureHandler,
        ThreadPool threadPool,
        AnonymousUser anonymousUser,
        @Nullable AuthorizationEngine authorizationEngine,
        Set<RequestInterceptor> requestInterceptors,
        XPackLicenseState licenseState,
        IndexNameExpressionResolver resolver,
        OperatorPrivilegesService operatorPrivilegesService,
        RestrictedIndices restrictedIndices,
        AuthorizationDenialMessages authorizationDenialMessages,
        ProjectResolver projectResolver
    ) {
        this.clusterService = clusterService;
        this.auditTrailService = auditTrailService;
        this.restrictedIndices = restrictedIndices;
        this.indicesAndAliasesResolver = new IndicesAndAliasesResolver(settings, clusterService, resolver);
        this.authcFailureHandler = authcFailureHandler;
        this.threadContext = threadPool.getThreadContext();
        this.securityContext = new SecurityContext(settings, this.threadContext);
        this.anonymousUser = anonymousUser;
        this.isAnonymousEnabled = AnonymousUser.isAnonymousEnabled(settings);
        this.anonymousAuthzExceptionEnabled = ANONYMOUS_AUTHORIZATION_EXCEPTION_SETTING.get(settings);
        this.rbacEngine = new RBACEngine(
            settings,
            rolesStore,
            fieldPermissionsCache,
            new LoadAuthorizedIndicesTimeChecker.Factory(logger, settings, clusterService.getClusterSettings())
        );
        this.authorizationEngine = authorizationEngine == null ? this.rbacEngine : authorizationEngine;
        this.requestInterceptors = requestInterceptors;
        this.settings = settings;
        this.licenseState = licenseState;
        this.operatorPrivilegesService = operatorPrivilegesService;
        this.indicesAccessControlWrapper = new DlsFlsFeatureTrackingIndicesAccessControlWrapper(settings, licenseState);
        this.authorizationDenialMessages = authorizationDenialMessages;
        this.projectResolver = projectResolver;
    }

    public void checkPrivileges(
        Subject subject,
        AuthorizationEngine.PrivilegesToCheck privilegesToCheck,
        Collection<ApplicationPrivilegeDescriptor> applicationPrivilegeDescriptors,
        ActionListener<AuthorizationEngine.PrivilegesCheckResult> listener
    ) {
        final AuthorizationEngine authorizationEngine = getAuthorizationEngineForSubject(subject);
        authorizationEngine.resolveAuthorizationInfo(
            subject,
            wrapPreservingContext(
                listener.delegateFailure(
                    (delegateListener, authorizationInfo) -> authorizationEngine.checkPrivileges(
                        authorizationInfo,
                        privilegesToCheck,
                        applicationPrivilegeDescriptors,
                        wrapPreservingContext(delegateListener, threadContext)
                    )
                ),
                threadContext
            )
        );
    }

    public void retrieveUserPrivileges(
        Subject subject,
        AuthorizationInfo authorizationInfo,
        ActionListener<GetUserPrivilegesResponse> listener
    ) {
        final AuthorizationEngine authorizationEngine = getAuthorizationEngineForSubject(subject);
        // TODO the AuthorizationInfo is associated to the Subject; the argument is redundant and a possible source of conflict
        authorizationEngine.getUserPrivileges(authorizationInfo, wrapPreservingContext(listener, threadContext));
    }

    public void getRoleDescriptorsIntersectionForRemoteCluster(
        final String remoteClusterAlias,
        final TransportVersion remoteClusterVersion,
        final Subject subject,
        final ActionListener<RoleDescriptorsIntersection> listener
    ) {
        if (subject.getUser() instanceof InternalUser) {
            final String message = "the user ["
                + subject.getUser().principal()
                + "] is an internal user and we should never try to retrieve its roles descriptors towards a remote cluster";
            assert false : message;
            logger.warn(message);
            listener.onFailure(new IllegalArgumentException(message));
            return;
        }

        final AuthorizationEngine authorizationEngine = getAuthorizationEngineForSubject(subject);
        // AuthZ info can be null for persistent tasks
        if (threadContext.<AuthorizationInfo>getTransient(AUTHORIZATION_INFO_KEY) == null) {
            logger.debug("authorization info not available in thread context, resolving it for subject [{}]", subject);
        }
        authorizationEngine.resolveAuthorizationInfo(
            subject,
            wrapPreservingContext(
                listener.delegateFailure(
                    (delegatedLister, resolvedAuthzInfo) -> authorizationEngine.getRoleDescriptorsIntersectionForRemoteCluster(
                        remoteClusterAlias,
                        remoteClusterVersion,
                        resolvedAuthzInfo,
                        wrapPreservingContext(delegatedLister, threadContext)
                    )
                ),
                threadContext
            )
        );
    }

    /**
     * Verifies that the given user can execute the given request (and action). If the user doesn't
     * have the appropriate privileges for this action/request, an {@link ElasticsearchSecurityException}
     * will be thrown.
     *
     * @param authentication  The authentication information
     * @param action          The action
     * @param originalRequest The request
     * @param listener        The listener that gets called. A call to {@link ActionListener#onResponse(Object)} indicates success
     */
    public void authorize(
        final Authentication authentication,
        final String action,
        final TransportRequest originalRequest,
        final ActionListener<Void> listener
    ) {

        final AuthorizationContext enclosingContext = extractAuthorizationContext(threadContext, action);
        final ParentActionAuthorization parentAuthorization = securityContext.getParentAuthorization();

        /* authorization fills in certain transient headers, which must be observed in the listener (action handler execution)
         * as well, but which must not bleed across different action context (eg parent-child action contexts).
         * <p>
         * Therefore we begin by clearing the existing ones up, as they might already be set during the authorization of a
         * previous parent action that ran under the same thread context (also on the same node).
         * When the returned {@code StoredContext} is closed, ALL the original headers are restored.
         */
        try (
            ThreadContext.StoredContext ignore = threadContext.newStoredContext(
                ACTION_SCOPE_AUTHORIZATION_KEYS,
                List.of(ParentActionAuthorization.THREAD_CONTEXT_KEY)
            )
        ) {
            // this does not clear {@code AuthorizationServiceField.ORIGINATING_ACTION_KEY}
            // prior to doing any authorization lets set the originating action in the thread context
            // the originating action is the current action if no originating action has yet been set in the current thread context
            // if there is already an original action, that stays put (eg. the current action is a child action)
            putTransientIfNonExisting(ORIGINATING_ACTION_KEY, action);

            final String auditId;
            try {
                auditId = requireAuditId(authentication, action, originalRequest);
            } catch (ElasticsearchSecurityException e) {
                listener.onFailure(e);
                return;
            }

            // sometimes a request might be wrapped within another, which is the case for proxied
            // requests and concrete shard requests
            final TransportRequest unwrappedRequest = maybeUnwrapRequest(authentication, originalRequest, action, auditId);

            try {
                checkOperatorPrivileges(authentication, action, originalRequest);
            } catch (ElasticsearchException e) {
                listener.onFailure(e);
                return;
            }

            if (SystemUser.is(authentication.getEffectiveSubject().getUser())) {
                // this never goes async so no need to wrap the listener
                authorizeSystemUser(authentication, action, auditId, unwrappedRequest, listener);
            } else {
                final RequestInfo requestInfo = new RequestInfo(
                    authentication,
                    unwrappedRequest,
                    action,
                    enclosingContext,
                    parentAuthorization
                );
                final AuthorizationEngine engine = getAuthorizationEngine(authentication);
                final ActionListener<AuthorizationInfo> authzInfoListener = wrapPreservingContext(ActionListener.wrap(authorizationInfo -> {
                    threadContext.putTransient(AUTHORIZATION_INFO_KEY, authorizationInfo);
                    maybeAuthorizeRunAs(requestInfo, auditId, authorizationInfo, listener);
                }, e -> {
                    if (e instanceof ElasticsearchRoleRestrictionException) {
                        logger.debug(
                            () -> Strings.format(
                                "denying action [%s] due to role restriction for authentication [%s]",
                                action,
                                authentication
                            ),
                            e
                        );
                        listener.onFailure(actionDenied(authentication, EmptyAuthorizationInfo.INSTANCE, action, unwrappedRequest, e));
                    } else {
                        listener.onFailure(e);
                    }
                }), threadContext);
                engine.resolveAuthorizationInfo(requestInfo, authzInfoListener);
            }
        }
    }

    @Nullable
    private static AuthorizationContext extractAuthorizationContext(ThreadContext threadContext, String childAction) {
        final String originatingAction = threadContext.getTransient(ORIGINATING_ACTION_KEY);
        if (Strings.isNullOrEmpty(originatingAction)) {
            // No parent action
            return null;
        }
        AuthorizationInfo authorizationInfo = threadContext.getTransient(AUTHORIZATION_INFO_KEY);
        if (authorizationInfo == null) {
            throw internalError(
                "While attempting to authorize action ["
                    + childAction
                    + "], found originating action ["
                    + originatingAction
                    + "] but no authorization info"
            );
        }

        final IndicesAccessControl parentAccessControl = threadContext.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY);
        return new AuthorizationContext(originatingAction, authorizationInfo, parentAccessControl);
    }

    private String requireAuditId(Authentication authentication, String action, TransportRequest originalRequest) {
        String auditId = AuditUtil.extractRequestId(threadContext);
        if (auditId == null) {
            // We would like to assert that there is an existing request-id, but if this is a system action, then that might not be
            // true because the request-id is generated during authentication
            if (authentication.getEffectiveSubject().getUser() instanceof InternalUser) {
                auditId = AuditUtil.getOrGenerateRequestId(threadContext);
            } else {
                auditTrailService.get().tamperedRequest(null, authentication, action, originalRequest);
                throw internalError(
                    "Attempt to authorize action ["
                        + action
                        + "] for ["
                        + authentication.getEffectiveSubject().getUser().principal()
                        + "] without an existing request-id"
                );
            }
        }
        return auditId;
    }

    private static ElasticsearchSecurityException internalError(String message) {
        // When running with assertions enabled (testing) kill the node so that there is a hard failure in CI
        assert false : message;
        // Otherwise (production) just throw an exception so that we don't authorize something incorrectly
        return new ElasticsearchSecurityException(message);
    }

    private void checkOperatorPrivileges(Authentication authentication, String action, TransportRequest originalRequest)
        throws ElasticsearchSecurityException {
        // Check operator privileges
        // TODO: audit?
        final ElasticsearchSecurityException operatorException = operatorPrivilegesService.check(
            authentication,
            action,
            originalRequest,
            threadContext
        );
        if (operatorException != null) {
            throw actionDenied(authentication, null, action, originalRequest, "because it requires operator privileges", operatorException);
        }
        operatorPrivilegesService.maybeInterceptRequest(threadContext, originalRequest);
    }

    private void maybeAuthorizeRunAs(
        final RequestInfo requestInfo,
        final String requestId,
        final AuthorizationInfo authzInfo,
        final ActionListener<Void> listener
    ) {
        final Authentication authentication = requestInfo.getAuthentication();
        final TransportRequest request = requestInfo.getRequest();
        final String action = requestInfo.getAction();
        final boolean isRunAs = authentication.isRunAs();
        final AuditTrail auditTrail = auditTrailService.get();
        if (isRunAs) {
            ActionListener<AuthorizationResult> runAsListener = wrapPreservingContext(ActionListener.wrap(result -> {
                if (result.isGranted()) {
                    auditTrail.runAsGranted(requestId, authentication, action, request, authzInfo.getAuthenticatedUserAuthorizationInfo());
                    authorizeAction(requestInfo, requestId, authzInfo, listener);
                } else {
                    auditTrail.runAsDenied(requestId, authentication, action, request, authzInfo.getAuthenticatedUserAuthorizationInfo());
                    listener.onFailure(runAsDenied(authentication, authzInfo, action));
                }
            }, e -> {
                auditTrail.runAsDenied(requestId, authentication, action, request, authzInfo.getAuthenticatedUserAuthorizationInfo());
                listener.onFailure(actionDenied(authentication, authzInfo, action, request));
            }), threadContext);
            authorizeRunAs(requestInfo, authzInfo, runAsListener);
        } else {
            authorizeAction(requestInfo, requestId, authzInfo, listener);
        }
    }

    private void authorizeAction(
        final RequestInfo requestInfo,
        final String requestId,
        final AuthorizationInfo authzInfo,
        final ActionListener<Void> listener
    ) {
        final Authentication authentication = requestInfo.getAuthentication();
        final TransportRequest request = requestInfo.getRequest();
        final String action = requestInfo.getAction();
        final AuthorizationEngine authzEngine = getAuthorizationEngine(authentication);
        final AuditTrail auditTrail = auditTrailService.get();

        if (ClusterPrivilegeResolver.isClusterAction(action)) {
            final ActionListener<AuthorizationResult> clusterAuthzListener = wrapPreservingContext(
                new AuthorizationResultListener<>(result -> {
                    securityContext.putIndicesAccessControl(allowAll());
                    listener.onResponse(null);
                }, listener::onFailure, requestInfo, requestId, authzInfo),
                threadContext
            );
            authzEngine.authorizeClusterAction(requestInfo, authzInfo, clusterAuthzListener.delegateFailureAndWrap((l, result) -> {
                if (false == result.isGranted() && QueryApiKeyAction.NAME.equals(action)) {
                    assert request instanceof QueryApiKeyRequest : "request does not match action";
                    final QueryApiKeyRequest queryApiKeyRequest = (QueryApiKeyRequest) request;
                    if (false == queryApiKeyRequest.isFilterForCurrentUser()) {
                        queryApiKeyRequest.setFilterForCurrentUser();
                        authzEngine.authorizeClusterAction(requestInfo, authzInfo, l);
                        return;
                    }
                }
                l.onResponse(result);
            }));
        } else if (isIndexAction(action)) {
            final ProjectMetadata projectMetadata = projectResolver.getProjectMetadata(clusterService.state());
            assert projectMetadata != null;
            final AsyncSupplier<ResolvedIndices> resolvedIndicesAsyncSupplier = new CachingAsyncSupplier<>(() -> {
                if (request instanceof SearchRequest searchRequest && searchRequest.pointInTimeBuilder() != null) {
                    var resolvedIndices = indicesAndAliasesResolver.resolvePITIndices(searchRequest);
                    return SubscribableListener.newSucceeded(resolvedIndices);
                }
                final ResolvedIndices resolvedIndices = indicesAndAliasesResolver.tryResolveWithoutWildcards(action, request);
                if (resolvedIndices != null) {
                    return SubscribableListener.newSucceeded(resolvedIndices);
                } else {
                    final SubscribableListener<ResolvedIndices> resolvedIndicesListener = new SubscribableListener<>();
                    authzEngine.loadAuthorizedIndices(
                        requestInfo,
                        authzInfo,
                        projectMetadata.getIndicesLookup(),
                        ActionListener.wrap(
                            authorizedIndices -> resolvedIndicesListener.onResponse(
                                indicesAndAliasesResolver.resolve(action, request, projectMetadata, authorizedIndices)
                            ),
                            e -> {
                                if (e instanceof InvalidIndexNameException
                                    || e instanceof InvalidSelectorException
                                    || e instanceof UnsupportedSelectorException) {
                                    logger.debug(
                                        () -> Strings.format(
                                            "failed [%s] action authorization for [%s] due to [%s] exception",
                                            action,
                                            authentication,
                                            e.getClass().getSimpleName()
                                        ),
                                        e
                                    );
                                    listener.onFailure(e);
                                    return;
                                }
                                auditTrail.accessDenied(requestId, authentication, action, request, authzInfo);
                                if (e instanceof IndexNotFoundException) {
                                    listener.onFailure(e);
                                } else {
                                    listener.onFailure(actionDenied(authentication, authzInfo, action, request, e));
                                }
                            }
                        )
                    );
                    return resolvedIndicesListener;
                }
            });
            authzEngine.authorizeIndexAction(requestInfo, authzInfo, resolvedIndicesAsyncSupplier, projectMetadata)
                .addListener(
                    wrapPreservingContext(
                        new AuthorizationResultListener<>(
                            result -> handleIndexActionAuthorizationResult(
                                result,
                                requestInfo,
                                requestId,
                                authzInfo,
                                authzEngine,
                                resolvedIndicesAsyncSupplier,
                                projectMetadata,
                                listener
                            ),
                            listener::onFailure,
                            requestInfo,
                            requestId,
                            authzInfo
                        ),
                        threadContext
                    )
                );
        } else {
            logger.warn("denying access for [{}] as action [{}] is not an index or cluster action", authentication, action);
            auditTrail.accessDenied(requestId, authentication, action, request, authzInfo);
            listener.onFailure(actionDenied(authentication, authzInfo, action, request));
        }
    }

    private void handleIndexActionAuthorizationResult(
        final IndexAuthorizationResult result,
        final RequestInfo requestInfo,
        final String requestId,
        final AuthorizationInfo authzInfo,
        final AuthorizationEngine authzEngine,
        final AsyncSupplier<ResolvedIndices> resolvedIndicesAsyncSupplier,
        final ProjectMetadata projectMetadata,
        final ActionListener<Void> listener
    ) {
        final IndicesAccessControl indicesAccessControl = indicesAccessControlWrapper.wrap(result.getIndicesAccessControl());
        final Authentication authentication = requestInfo.getAuthentication();
        final TransportRequest request = requestInfo.getRequest();
        final String action = requestInfo.getAction();
        securityContext.putIndicesAccessControl(indicesAccessControl);

        final AuthorizationContext authzContext = new AuthorizationContext(action, authzInfo, indicesAccessControl);
        PreAuthorizationUtils.maybeSkipChildrenActionAuthorization(securityContext, authzContext);

        // if we are creating an index we need to authorize potential aliases created at the same time
        if (IndexPrivilege.CREATE_INDEX_MATCHER.test(action)) {
            assert (request instanceof CreateIndexRequest)
                || (request instanceof MigrateToDataStreamAction.Request)
                || (request instanceof CreateDataStreamAction.Request);
            if (request instanceof CreateDataStreamAction.Request
                || (request instanceof MigrateToDataStreamAction.Request)
                || ((CreateIndexRequest) request).aliases().isEmpty()) {
                runRequestInterceptors(requestInfo, authzInfo, authorizationEngine, listener);
            } else {
                Set<Alias> aliases = ((CreateIndexRequest) request).aliases();
                final RequestInfo aliasesRequestInfo = new RequestInfo(
                    authentication,
                    request,
                    TransportIndicesAliasesAction.NAME,
                    authzContext
                );
                authzEngine.authorizeIndexAction(aliasesRequestInfo, authzInfo, () -> {
                    SubscribableListener<ResolvedIndices> ril = new SubscribableListener<>();
                    resolvedIndicesAsyncSupplier.getAsync().addListener(ril.delegateFailureAndWrap((l, resolvedIndices) -> {
                        List<String> aliasesAndIndices = new ArrayList<>(resolvedIndices.getLocal());
                        for (Alias alias : aliases) {
                            aliasesAndIndices.add(alias.name());
                        }
                        ResolvedIndices withAliases = new ResolvedIndices(aliasesAndIndices, Collections.emptyList());
                        l.onResponse(withAliases);
                    }));
                    return ril;
                }, projectMetadata)
                    .addListener(
                        wrapPreservingContext(
                            new AuthorizationResultListener<>(
                                authorizationResult -> runRequestInterceptors(requestInfo, authzInfo, authorizationEngine, listener),
                                listener::onFailure,
                                aliasesRequestInfo,
                                requestId,
                                authzInfo
                            ),
                            threadContext
                        )
                    );
            }
        } else if (action.equals(TransportShardBulkAction.ACTION_NAME)) {
            // if this is performing multiple actions on the index, then check each of those actions.
            assert request instanceof BulkShardRequest
                : "Action " + action + " requires " + BulkShardRequest.class + " but was " + request.getClass();
            authorizeBulkItems(
                requestInfo,
                authzContext,
                authzEngine,
                resolvedIndicesAsyncSupplier,
                projectMetadata,
                requestId,
                wrapPreservingContext(
                    listener.delegateFailureAndWrap((l, ignore) -> runRequestInterceptors(requestInfo, authzInfo, authorizationEngine, l)),
                    threadContext
                )
            );
        } else {
            runRequestInterceptors(requestInfo, authzInfo, authorizationEngine, listener);
        }
    }

    private void runRequestInterceptors(
        RequestInfo requestInfo,
        AuthorizationInfo authorizationInfo,
        AuthorizationEngine authorizationEngine,
        ActionListener<Void> listener
    ) {
        final Iterator<RequestInterceptor> requestInterceptorIterator = requestInterceptors.iterator();
        while (requestInterceptorIterator.hasNext()) {
            var res = requestInterceptorIterator.next().intercept(requestInfo, authorizationEngine, authorizationInfo);
            if (res.isSuccess() == false) {
                res.addListener(new DelegatingActionListener<>(listener) {
                    @Override
                    public void onResponse(Void unused) {
                        if (requestInterceptorIterator.hasNext()) {
                            requestInterceptorIterator.next()
                                .intercept(requestInfo, authorizationEngine, authorizationInfo)
                                .addListener(this);
                        } else {
                            delegate.onResponse(null);
                        }
                    }
                });
                return;
            }
        }
        listener.onResponse(null);
    }

    // pkg-private for testing
    AuthorizationEngine getRunAsAuthorizationEngine(final Authentication authentication) {
        return getAuthorizationEngineForSubject(authentication.getAuthenticatingSubject());
    }

    // pkg-private for testing
    AuthorizationEngine getAuthorizationEngine(final Authentication authentication) {
        return getAuthorizationEngineForSubject(authentication.getEffectiveSubject());
    }

    AuthorizationEngine getAuthorizationEngineForSubject(final Subject subject) {
        return getAuthorizationEngineForUser(subject.getUser());
    }

    private AuthorizationEngine getAuthorizationEngineForUser(final User user) {
        if (rbacEngine != authorizationEngine && Security.AUTHORIZATION_ENGINE_FEATURE.check(licenseState)) {
            if (ClientReservedRealm.isReserved(user.principal(), settings) || user instanceof InternalUser) {
                return rbacEngine;
            } else {
                return authorizationEngine;
            }
        } else {
            return rbacEngine;
        }
    }

    private void authorizeSystemUser(
        final Authentication authentication,
        final String action,
        final String requestId,
        final TransportRequest request,
        final ActionListener<Void> listener
    ) {
        final AuditTrail auditTrail = auditTrailService.get();
        if (SystemUser.isAuthorized(action)) {
            securityContext.putIndicesAccessControl(allowAll());
            threadContext.putTransient(AUTHORIZATION_INFO_KEY, SYSTEM_AUTHZ_INFO);
            auditTrail.accessGranted(requestId, authentication, action, request, SYSTEM_AUTHZ_INFO);
            listener.onResponse(null);
        } else {
            auditTrail.accessDenied(requestId, authentication, action, request, SYSTEM_AUTHZ_INFO);
            listener.onFailure(actionDenied(authentication, SYSTEM_AUTHZ_INFO, action, request));
        }
    }

    private TransportRequest maybeUnwrapRequest(
        Authentication authentication,
        TransportRequest originalRequest,
        String action,
        String requestId
    ) {
        final TransportRequest request;
        if (originalRequest instanceof ConcreteShardRequest) {
            request = ((ConcreteShardRequest<?>) originalRequest).getRequest();
            assert TransportActionProxy.isProxyRequest(request) == false : "expected non-proxy request for action: " + action;
        } else {
            request = TransportActionProxy.unwrapRequest(originalRequest);
            final boolean isOriginalRequestProxyRequest = TransportActionProxy.isProxyRequest(originalRequest);
            final boolean isProxyAction = TransportActionProxy.isProxyAction(action);
            final AuditTrail auditTrail = auditTrailService.get();
            if (isProxyAction && isOriginalRequestProxyRequest == false) {
                IllegalStateException cause = new IllegalStateException(
                    "originalRequest is not a proxy request: [" + originalRequest + "] but action: [" + action + "] is a proxy action"
                );
                auditTrail.accessDenied(requestId, authentication, action, request, EmptyAuthorizationInfo.INSTANCE);
                throw actionDenied(authentication, null, action, request, cause);
            }
            if (TransportActionProxy.isProxyRequest(originalRequest) && TransportActionProxy.isProxyAction(action) == false) {
                IllegalStateException cause = new IllegalStateException(
                    "originalRequest is a proxy request for: [" + request + "] but action: [" + action + "] isn't"
                );
                auditTrail.accessDenied(requestId, authentication, action, request, EmptyAuthorizationInfo.INSTANCE);
                throw actionDenied(authentication, null, action, request, cause);
            }
        }
        return request;
    }

    private void authorizeRunAs(
        final RequestInfo requestInfo,
        final AuthorizationInfo authzInfo,
        final ActionListener<AuthorizationResult> listener
    ) {
        final Authentication authentication = requestInfo.getAuthentication();
        assert authentication.isRunAs() : "authentication must have run-as for run-as to be authorized";
        if (authentication.getEffectiveSubject().getRealm() == null) {
            // this user did not really exist
            // TODO(jaymode) find a better way to indicate lookup failed for a user and we need to fail authz
            listener.onResponse(AuthorizationResult.deny());
        } else {
            final AuthorizationEngine runAsAuthzEngine = getRunAsAuthorizationEngine(authentication);
            runAsAuthzEngine.authorizeRunAs(requestInfo, authzInfo, listener);
        }
    }

    /**
     * Performs authorization checks on the items within a {@link BulkShardRequest}.
     * This inspects the {@link BulkItemRequest items} within the request, computes
     * an <em>implied</em> action for each item's {@link DocWriteRequest#opType()},
     * and then checks whether that action is allowed on the targeted index. Items
     * that fail this checks are {@link BulkItemRequest#abort(String, Exception)
     * aborted}, with an
     * {@link #actionDenied(Authentication, AuthorizationInfo, String, TransportRequest, String, Exception)
     * access denied} exception. Because a shard level request is for exactly 1 index,
     * and there are a small number of possible item {@link DocWriteRequest.OpType
     * types}, the number of distinct authorization checks that need to be performed
     * is very small, but the results must be cached, to avoid adding a high
     * overhead to each bulk request.
     */
    private void authorizeBulkItems(
        RequestInfo requestInfo,
        AuthorizationContext bulkAuthzContext,
        AuthorizationEngine authzEngine,
        AsyncSupplier<ResolvedIndices> resolvedIndicesAsyncSupplier,
        ProjectMetadata projectMetadata,
        String requestId,
        ActionListener<Void> listener
    ) {
        final Authentication authentication = requestInfo.getAuthentication();
        final AuthorizationInfo authzInfo = bulkAuthzContext.getAuthorizationInfo();
        final BulkShardRequest request = (BulkShardRequest) requestInfo.getRequest();
        // Maps original-index -> expanded-index-name (expands date-math, but not aliases)
        final Map<String, String> resolvedIndexNames = new HashMap<>();
        // Maps action -> resolved indices set (there are 4 action types total)
        final Map<String, Set<String>> actionToIndicesMap = new HashMap<>(4);
        final AuditTrail auditTrail = auditTrailService.get();

        resolvedIndicesAsyncSupplier.getAsync().addListener(ActionListener.wrap(overallResolvedIndices -> {
            final Set<String> localIndices = new HashSet<>(overallResolvedIndices.getLocal());
            for (BulkItemRequest item : request.items()) {
                final String itemAction = getAction(item);
                final String resolvedIndex = resolvedIndexNames.computeIfAbsent(item.index(), key -> {
                    final String resolved = resolveIndexNameDateMath(item);
                    if (localIndices.contains(resolved) == false) {
                        throw illegalArgument(
                            "Found bulk item that writes to index " + resolved + " but the request writes to " + localIndices
                        );
                    }
                    return resolved;
                });
                actionToIndicesMap.computeIfAbsent(itemAction, k -> new HashSet<>()).add(resolvedIndex);
            }

            final ActionListener<Collection<Tuple<String, IndexAuthorizationResult>>> bulkAuthzListener = ActionListener.wrap(
                collection -> {
                    final Map<String, IndicesAccessControl> actionToIndicesAccessControl = new HashMap<>(4);
                    collection.forEach(tuple -> {
                        final IndicesAccessControl existing = actionToIndicesAccessControl.putIfAbsent(
                            tuple.v1(),
                            tuple.v2().getIndicesAccessControl()
                        );
                        if (existing != null) {
                            throw new IllegalStateException("a value already exists for action " + tuple.v1());
                        }
                    });

                    final Map<String, Set<String>> actionToGrantedIndicesMap = new HashMap<>(4);
                    final Map<String, Set<String>> actionToDeniedIndicesMap = new HashMap<>(4);
                    for (BulkItemRequest item : request.items()) {
                        final String resolvedIndex = resolvedIndexNames.get(item.index());
                        final String itemAction = getAction(item);
                        if (actionToIndicesAccessControl.get(itemAction).hasIndexPermissions(resolvedIndex)) {
                            actionToGrantedIndicesMap.computeIfAbsent(itemAction, ignore -> new HashSet<>()).add(resolvedIndex);
                        } else {
                            actionToDeniedIndicesMap.computeIfAbsent(itemAction, ignore -> new HashSet<>()).add(resolvedIndex);
                            item.abort(
                                resolvedIndex,
                                actionDenied(
                                    authentication,
                                    authzInfo,
                                    itemAction,
                                    request,
                                    IndexAuthorizationResult.getFailureDescription(List.of(resolvedIndex), restrictedIndices),
                                    null
                                )
                            );
                        }
                    }
                    actionToDeniedIndicesMap.forEach((action, resolvedIndicesSet) -> {
                        auditTrail.explicitIndexAccessEvent(
                            requestId,
                            AuditLevel.ACCESS_DENIED,
                            authentication,
                            action,
                            resolvedIndicesSet.toArray(new String[0]),
                            BulkItemRequest.class.getSimpleName(),
                            request.remoteAddress(),
                            authzInfo
                        );
                    });
                    actionToGrantedIndicesMap.forEach((action, resolvedIndicesSet) -> {
                        auditTrail.explicitIndexAccessEvent(
                            requestId,
                            AuditLevel.ACCESS_GRANTED,
                            authentication,
                            action,
                            resolvedIndicesSet.toArray(new String[0]),
                            BulkItemRequest.class.getSimpleName(),
                            request.remoteAddress(),
                            authzInfo
                        );
                    });
                    listener.onResponse(null);
                },
                listener::onFailure
            );
            final ActionListener<Tuple<String, IndexAuthorizationResult>> groupedActionListener = wrapPreservingContext(
                new GroupedActionListener<>(actionToIndicesMap.size(), bulkAuthzListener),
                threadContext
            );

            actionToIndicesMap.forEach((bulkItemAction, indices) -> {
                final RequestInfo bulkItemInfo = new RequestInfo(
                    requestInfo.getAuthentication(),
                    requestInfo.getRequest(),
                    bulkItemAction,
                    bulkAuthzContext,
                    requestInfo.getParentAuthorization()
                );
                authzEngine.authorizeIndexAction(
                    bulkItemInfo,
                    authzInfo,
                    () -> SubscribableListener.newSucceeded(new ResolvedIndices(new ArrayList<>(indices), Collections.emptyList())),
                    projectMetadata
                )
                    .addListener(
                        groupedActionListener.delegateFailureAndWrap(
                            (l, indexAuthorizationResult) -> l.onResponse(new Tuple<>(bulkItemAction, indexAuthorizationResult))
                        )
                    );
            });
        }, listener::onFailure));
    }

    private String resolveIndexNameDateMath(BulkItemRequest bulkItemRequest) {
        final ResolvedIndices resolvedIndices = indicesAndAliasesResolver.resolveIndicesAndAliasesWithoutWildcards(
            getAction(bulkItemRequest),
            bulkItemRequest.request()
        );
        if (resolvedIndices.getRemote().size() != 0) {
            throw illegalArgument(
                "Bulk item should not write to remote indices, but request writes to " + String.join(",", resolvedIndices.getRemote())
            );
        }
        if (resolvedIndices.getLocal().size() != 1) {
            throw illegalArgument(
                "Bulk item should write to exactly 1 index, but request writes to " + String.join(",", resolvedIndices.getLocal())
            );
        }
        return resolvedIndices.getLocal().get(0);
    }

    private static IllegalArgumentException illegalArgument(String message) {
        assert false : message;
        return new IllegalArgumentException(message);
    }

    static boolean isIndexAction(String action) {
        return IndexPrivilege.ACTION_MATCHER.test(action);
    }

    private static String getAction(BulkItemRequest item) {
        final DocWriteRequest<?> docWriteRequest = item.request();
        return switch (docWriteRequest.opType()) {
            case INDEX -> IMPLIED_INDEX_ACTION;
            case CREATE -> IMPLIED_CREATE_ACTION;
            case UPDATE -> TransportUpdateAction.NAME;
            case DELETE -> TransportDeleteAction.NAME;
        };
    }

    private void putTransientIfNonExisting(String key, Object value) {
        Object existing = threadContext.getTransient(key);
        if (existing == null) {
            threadContext.putTransient(key, value);
        }
    }

    private ElasticsearchSecurityException runAsDenied(
        Authentication authentication,
        @Nullable AuthorizationInfo authorizationInfo,
        String action
    ) {
        return denialException(
            authentication,
            action,
            () -> authorizationDenialMessages.runAsDenied(authentication, authorizationInfo, action),
            null
        );
    }

    public ElasticsearchSecurityException remoteActionDenied(Authentication authentication, String action, String clusterAlias) {
        final AuthorizationInfo authorizationInfo = threadContext.getTransient(AUTHORIZATION_INFO_KEY);
        return denialException(
            authentication,
            action,
            () -> authorizationDenialMessages.remoteActionDenied(authentication, authorizationInfo, action, clusterAlias),
            null
        );
    }

    ElasticsearchSecurityException actionDenied(
        Authentication authentication,
        @Nullable AuthorizationInfo authorizationInfo,
        String action,
        TransportRequest request
    ) {
        return actionDenied(authentication, authorizationInfo, action, request, null);
    }

    private ElasticsearchSecurityException actionDenied(
        Authentication authentication,
        @Nullable AuthorizationInfo authorizationInfo,
        String action,
        TransportRequest request,
        Exception cause
    ) {
        return actionDenied(authentication, authorizationInfo, action, request, null, cause);
    }

    private ElasticsearchSecurityException actionDenied(
        Authentication authentication,
        @Nullable AuthorizationInfo authorizationInfo,
        String action,
        TransportRequest request,
        @Nullable String context,
        Exception cause
    ) {
        return denialException(
            authentication,
            action,
            () -> authorizationDenialMessages.actionDenied(authentication, authorizationInfo, action, request, context),
            cause
        );
    }

    private ElasticsearchSecurityException denialException(
        Authentication authentication,
        String action,
        Supplier<String> authzDenialMessageSupplier,
        Exception cause
    ) {
        // Special case for anonymous user
        if (isAnonymousEnabled
            && anonymousUser.equals(authentication.getAuthenticatingSubject().getUser())
            && anonymousAuthzExceptionEnabled == false) {
            return authcFailureHandler.authenticationRequired(action, threadContext);
        }

        String message = authzDenialMessageSupplier.get();
        logger.debug(message);
        return authorizationError(message, cause);
    }

    private class AuthorizationResultListener<T extends AuthorizationResult> implements ActionListener<T> {

        private final Consumer<T> responseConsumer;
        private final Consumer<Exception> failureConsumer;
        private final RequestInfo requestInfo;
        private final String requestId;
        private final AuthorizationInfo authzInfo;

        private AuthorizationResultListener(
            Consumer<T> responseConsumer,
            Consumer<Exception> failureConsumer,
            RequestInfo requestInfo,
            String requestId,
            AuthorizationInfo authzInfo
        ) {
            this.responseConsumer = responseConsumer;
            this.failureConsumer = failureConsumer;
            this.requestInfo = requestInfo;
            this.requestId = requestId;
            this.authzInfo = authzInfo;
        }

        @Override
        public void onResponse(T result) {
            if (result.isGranted()) {
                auditTrailService.get()
                    .accessGranted(
                        requestId,
                        requestInfo.getAuthentication(),
                        requestInfo.getAction(),
                        requestInfo.getRequest(),
                        authzInfo
                    );
                try {
                    responseConsumer.accept(result);
                } catch (Exception e) {
                    failureConsumer.accept(e);
                }
            } else {
                handleFailure(result.getFailureContext(requestInfo, restrictedIndices), null);
            }
        }

        @Override
        public void onFailure(Exception e) {
            handleFailure(null, e);
        }

        private void handleFailure(@Nullable String context, @Nullable Exception e) {
            Authentication authentication = requestInfo.getAuthentication();
            String action = requestInfo.getAction();
            TransportRequest request = requestInfo.getRequest();
            auditTrailService.get().accessDenied(requestId, authentication, action, request, authzInfo);
            failureConsumer.accept(actionDenied(authentication, authzInfo, action, request, context, e));
        }
    }

    private static class CachingAsyncSupplier<V> implements AsyncSupplier<V> {

        private final AsyncSupplier<V> asyncSupplier;
        private volatile ListenableFuture<V> valueFuture = null;

        private CachingAsyncSupplier(AsyncSupplier<V> supplier) {
            this.asyncSupplier = supplier;
        }

        @Override
        public SubscribableListener<V> getAsync() {
            if (valueFuture == null) {
                boolean firstInvocation = false;
                synchronized (this) {
                    if (valueFuture == null) {
                        valueFuture = new ListenableFuture<>();
                        firstInvocation = true;
                    }
                }
                if (firstInvocation) {
                    asyncSupplier.getAsync().addListener(valueFuture);
                }
            }
            return valueFuture;
        }
    }

    public static void addSettings(List<Setting<?>> settings) {
        settings.add(ANONYMOUS_AUTHORIZATION_EXCEPTION_SETTING);
        settings.addAll(LoadAuthorizedIndicesTimeChecker.Factory.getSettings());
    }
}

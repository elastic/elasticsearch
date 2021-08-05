/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.replication.TransportReplicationAction.ConcreteShardRequest;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.XPackLicenseState.Feature;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.MigrateToDataStreamAction;
import org.elasticsearch.xpack.core.action.CreateDataStreamAction;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesResponse;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.AuthenticationType;
import org.elasticsearch.xpack.core.security.authc.AuthenticationFailureHandler;
import org.elasticsearch.xpack.core.security.authc.esnative.ClientReservedRealm;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AsyncSupplier;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationResult;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.EmptyAuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.IndexAuthorizationResult;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.RequestInfo;
import org.elasticsearch.xpack.core.security.authz.ResolvedIndices;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.audit.AuditLevel;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.audit.AuditUtil;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.elasticsearch.action.support.ContextPreservingActionListener.wrapPreservingContext;
import static org.elasticsearch.common.Strings.collectionToCommaDelimitedString;
import static org.elasticsearch.xpack.core.security.SecurityField.setting;
import static org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField.ACTION_SCOPE_AUTHORIZATION_KEYS;
import static org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField.AUTHORIZATION_INFO_KEY;
import static org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField.INDICES_PERMISSIONS_KEY;
import static org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField.ORIGINATING_ACTION_KEY;
import static org.elasticsearch.xpack.core.security.support.Exceptions.authorizationError;
import static org.elasticsearch.xpack.core.security.user.User.isInternal;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.PRINCIPAL_ROLES_FIELD_NAME;

public class AuthorizationService {
    public static final Setting<Boolean> ANONYMOUS_AUTHORIZATION_EXCEPTION_SETTING =
        Setting.boolSetting(setting("authc.anonymous.authz_exception"), true, Property.NodeScope);
    private static final AuthorizationInfo SYSTEM_AUTHZ_INFO =
        () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, new String[] { SystemUser.ROLE_NAME });
    private static final String IMPLIED_INDEX_ACTION = IndexAction.NAME + ":op_type/index";
    private static final String IMPLIED_CREATE_ACTION = IndexAction.NAME + ":op_type/create";

    private static final Logger logger = LogManager.getLogger(AuthorizationService.class);

    private final Settings settings;
    private final ClusterService clusterService;
    private final AuditTrailService auditTrailService;
    private final IndicesAndAliasesResolver indicesAndAliasesResolver;
    private final AuthenticationFailureHandler authcFailureHandler;
    private final ThreadContext threadContext;
    private final AnonymousUser anonymousUser;
    private final AuthorizationEngine rbacEngine;
    private final AuthorizationEngine authorizationEngine;
    private final Set<RequestInterceptor> requestInterceptors;
    private final XPackLicenseState licenseState;
    private final OperatorPrivilegesService operatorPrivilegesService;
    private final boolean isAnonymousEnabled;
    private final boolean anonymousAuthzExceptionEnabled;

    public AuthorizationService(Settings settings, CompositeRolesStore rolesStore, ClusterService clusterService,
                                AuditTrailService auditTrailService, AuthenticationFailureHandler authcFailureHandler,
                                ThreadPool threadPool, AnonymousUser anonymousUser, @Nullable AuthorizationEngine authorizationEngine,
                                Set<RequestInterceptor> requestInterceptors, XPackLicenseState licenseState,
                                IndexNameExpressionResolver resolver, OperatorPrivilegesService operatorPrivilegesService) {
        this.clusterService = clusterService;
        this.auditTrailService = auditTrailService;
        this.indicesAndAliasesResolver = new IndicesAndAliasesResolver(settings, clusterService, resolver);
        this.authcFailureHandler = authcFailureHandler;
        this.threadContext = threadPool.getThreadContext();
        this.anonymousUser = anonymousUser;
        this.isAnonymousEnabled = AnonymousUser.isAnonymousEnabled(settings);
        this.anonymousAuthzExceptionEnabled = ANONYMOUS_AUTHORIZATION_EXCEPTION_SETTING.get(settings);
        this.rbacEngine = new RBACEngine(settings, rolesStore);
        this.authorizationEngine = authorizationEngine == null ? this.rbacEngine : authorizationEngine;
        this.requestInterceptors = requestInterceptors;
        this.settings = settings;
        this.licenseState = licenseState;
        this.operatorPrivilegesService = operatorPrivilegesService;
    }

    public void checkPrivileges(Authentication authentication, HasPrivilegesRequest request,
                                Collection<ApplicationPrivilegeDescriptor> applicationPrivilegeDescriptors,
                                ActionListener<HasPrivilegesResponse> listener) {
        getAuthorizationEngine(authentication).checkPrivileges(authentication, getAuthorizationInfoFromContext(), request,
            applicationPrivilegeDescriptors, wrapPreservingContext(listener, threadContext));
    }

    public void retrieveUserPrivileges(Authentication authentication, GetUserPrivilegesRequest request,
                                       ActionListener<GetUserPrivilegesResponse> listener) {
        getAuthorizationEngine(authentication).getUserPrivileges(authentication, getAuthorizationInfoFromContext(), request, listener);
    }

    private AuthorizationInfo getAuthorizationInfoFromContext() {
        return Objects.requireNonNull(threadContext.getTransient(AUTHORIZATION_INFO_KEY), "authorization info is missing from context");
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
     * @throws ElasticsearchSecurityException If the given user is no allowed to execute the given request
     */
    public void authorize(final Authentication authentication, final String action, final TransportRequest originalRequest,
                          final ActionListener<Void> listener) throws ElasticsearchSecurityException {
        /* authorization fills in certain transient headers, which must be observed in the listener (action handler execution)
         * as well, but which must not bleed across different action context (eg parent-child action contexts).
         * <p>
         * Therefore we begin by clearing the existing ones up, as they might already be set during the authorization of a
         * previous parent action that ran under the same thread context (also on the same node).
         * When the returned {@code StoredContext} is closed, ALL the original headers are restored.
         */
        try (ThreadContext.StoredContext ignore = threadContext.newStoredContext(false,
                ACTION_SCOPE_AUTHORIZATION_KEYS)) { // this does not clear {@code AuthorizationServiceField.ORIGINATING_ACTION_KEY}
            // prior to doing any authorization lets set the originating action in the thread context
            // the originating action is the current action if no originating action has yet been set in the current thread context
            // if there is already an original action, that stays put (eg. the current action is a child action)
            putTransientIfNonExisting(ORIGINATING_ACTION_KEY, action);

            String auditId = AuditUtil.extractRequestId(threadContext);
            if (auditId == null) {
                // We would like to assert that there is an existing request-id, but if this is a system action, then that might not be
                // true because the request-id is generated during authentication
                if (isInternal(authentication.getUser())) {
                    auditId = AuditUtil.getOrGenerateRequestId(threadContext);
                } else {
                    auditTrailService.get().tamperedRequest(null, authentication, action, originalRequest);
                    final String message = "Attempt to authorize action [" + action + "] for [" + authentication.getUser().principal()
                            + "] without an existing request-id";
                    assert false : message;
                    listener.onFailure(new ElasticsearchSecurityException(message));
                    return;
                }
            }

            // sometimes a request might be wrapped within another, which is the case for proxied
            // requests and concrete shard requests
            final TransportRequest unwrappedRequest = maybeUnwrapRequest(authentication, originalRequest, action, auditId);

            // Check operator privileges
            // TODO: audit?
            final ElasticsearchSecurityException operatorException =
                operatorPrivilegesService.check(action, originalRequest, threadContext);
            if (operatorException != null) {
                listener.onFailure(denialException(authentication, action, originalRequest, operatorException));
                return;
            }
            operatorPrivilegesService.maybeInterceptRequest(threadContext, originalRequest);

            if (SystemUser.is(authentication.getUser())) {
                // this never goes async so no need to wrap the listener
                authorizeSystemUser(authentication, action, auditId, unwrappedRequest, listener);
            } else {
                final String finalAuditId = auditId;
                final RequestInfo requestInfo = new RequestInfo(authentication, unwrappedRequest, action);
                final ActionListener<AuthorizationInfo> authzInfoListener = wrapPreservingContext(ActionListener.wrap(
                        authorizationInfo -> {
                            threadContext.putTransient(AUTHORIZATION_INFO_KEY, authorizationInfo);
                            maybeAuthorizeRunAs(requestInfo, finalAuditId, authorizationInfo, listener);
                        }, listener::onFailure), threadContext);
                getAuthorizationEngine(authentication).resolveAuthorizationInfo(requestInfo, authzInfoListener);
            }
        }
    }

    private void maybeAuthorizeRunAs(final RequestInfo requestInfo, final String requestId, final AuthorizationInfo authzInfo,
                                     final ActionListener<Void> listener) {
        final Authentication authentication = requestInfo.getAuthentication();
        final TransportRequest request = requestInfo.getRequest();
        final String action = requestInfo.getAction();
        final boolean isRunAs = authentication.getUser().isRunAs();
        final AuditTrail auditTrail = auditTrailService.get();
        if (isRunAs) {
            ActionListener<AuthorizationResult> runAsListener = wrapPreservingContext(ActionListener.wrap(result -> {
                if (result.isGranted()) {
                    if (result.isAuditable()) {
                        auditTrail.runAsGranted(requestId, authentication, action, request,
                            authzInfo.getAuthenticatedUserAuthorizationInfo());
                    }
                    authorizeAction(requestInfo, requestId, authzInfo, listener);
                } else {
                    if (result.isAuditable()) {
                        auditTrail.runAsDenied(requestId, authentication, action, request,
                            authzInfo.getAuthenticatedUserAuthorizationInfo());
                    }
                    listener.onFailure(denialException(authentication, action, request, null));
                }
            }, e -> {
                auditTrail.runAsDenied(requestId, authentication, action, request,
                    authzInfo.getAuthenticatedUserAuthorizationInfo());
                listener.onFailure(denialException(authentication, action, request, null));
            }), threadContext);
            authorizeRunAs(requestInfo, authzInfo, runAsListener);
        } else {
            authorizeAction(requestInfo, requestId, authzInfo, listener);
        }
    }

    private void authorizeAction(final RequestInfo requestInfo, final String requestId, final AuthorizationInfo authzInfo,
                                 final ActionListener<Void> listener) {
        final Authentication authentication = requestInfo.getAuthentication();
        final TransportRequest request = requestInfo.getRequest();
        final String action = requestInfo.getAction();
        final AuthorizationEngine authzEngine = getAuthorizationEngine(authentication);
        final AuditTrail auditTrail = auditTrailService.get();

        if (ClusterPrivilegeResolver.isClusterAction(action)) {
            final ActionListener<AuthorizationResult> clusterAuthzListener =
                wrapPreservingContext(new AuthorizationResultListener<>(result -> {
                        threadContext.putTransient(INDICES_PERMISSIONS_KEY, IndicesAccessControl.ALLOW_ALL);
                        listener.onResponse(null);
                    }, listener::onFailure, requestInfo, requestId, authzInfo), threadContext);
            authzEngine.authorizeClusterAction(requestInfo, authzInfo, ActionListener.wrap(result -> {
                if (false == result.isGranted() && QueryApiKeyAction.NAME.equals(action)) {
                    assert request instanceof QueryApiKeyRequest : "request does not match action";
                    final QueryApiKeyRequest queryApiKeyRequest = (QueryApiKeyRequest) request;
                    if (false == queryApiKeyRequest.isFilterForCurrentUser()) {
                        queryApiKeyRequest.setFilterForCurrentUser();
                        authzEngine.authorizeClusterAction(requestInfo, authzInfo, clusterAuthzListener);
                        return;
                    }
                }
                clusterAuthzListener.onResponse(result);
            }, clusterAuthzListener::onFailure));
        } else if (isIndexAction(action)) {
            final Metadata metadata = clusterService.state().metadata();
            final AsyncSupplier<Set<String>> authorizedIndicesSupplier = new CachingAsyncSupplier<>(authzIndicesListener ->
                authzEngine.loadAuthorizedIndices(requestInfo, authzInfo, metadata.getIndicesLookup(),
                    authzIndicesListener));
            final AsyncSupplier<ResolvedIndices> resolvedIndicesAsyncSupplier = new CachingAsyncSupplier<>(resolvedIndicesListener ->
                    authorizedIndicesSupplier.getAsync(
                        ActionListener.wrap(
                            authorizedIndices ->
                                resolvedIndicesListener.onResponse(
                                    indicesAndAliasesResolver.resolve(action, request, metadata, authorizedIndices)
                                ),
                            e -> {
                                auditTrail.accessDenied(requestId, authentication, action, request, authzInfo);
                                if (e instanceof IndexNotFoundException) {
                                    listener.onFailure(e);
                                } else {
                                    listener.onFailure(denialException(authentication, action, request, e));
                                }
                            }
                        )
                    )
            );
            authzEngine.authorizeIndexAction(requestInfo, authzInfo, resolvedIndicesAsyncSupplier,
                metadata.getIndicesLookup(), wrapPreservingContext(new AuthorizationResultListener<>(result ->
                    handleIndexActionAuthorizationResult(result, requestInfo, requestId, authzInfo, authzEngine, authorizedIndicesSupplier,
                        resolvedIndicesAsyncSupplier, metadata, listener),
                    listener::onFailure, requestInfo, requestId, authzInfo), threadContext));
        } else {
            logger.warn("denying access as action [{}] is not an index or cluster action", action);
            auditTrail.accessDenied(requestId, authentication, action, request, authzInfo);
            listener.onFailure(denialException(authentication, action, request, null));
        }
    }

    private void handleIndexActionAuthorizationResult(final IndexAuthorizationResult result, final RequestInfo requestInfo,
                                                      final String requestId, final AuthorizationInfo authzInfo,
                                                      final AuthorizationEngine authzEngine,
                                                      final AsyncSupplier<Set<String>> authorizedIndicesSupplier,
                                                      final AsyncSupplier<ResolvedIndices> resolvedIndicesAsyncSupplier,
                                                      final Metadata metadata,
                                                      final ActionListener<Void> listener) {
        final Authentication authentication = requestInfo.getAuthentication();
        final TransportRequest request = requestInfo.getRequest();
        final String action = requestInfo.getAction();
        if (result.getIndicesAccessControl() != null) {
            threadContext.putTransient(INDICES_PERMISSIONS_KEY, result.getIndicesAccessControl());
        }
        //if we are creating an index we need to authorize potential aliases created at the same time
        if (IndexPrivilege.CREATE_INDEX_MATCHER.test(action)) {
            assert (request instanceof CreateIndexRequest) || (request instanceof MigrateToDataStreamAction.Request) ||
                (request instanceof CreateDataStreamAction.Request);
            if (request instanceof CreateDataStreamAction.Request || (request instanceof MigrateToDataStreamAction.Request) ||
                ((CreateIndexRequest) request).aliases().isEmpty()) {
                runRequestInterceptors(requestInfo, authzInfo, authorizationEngine, listener);
            } else {
                Set<Alias> aliases = ((CreateIndexRequest) request).aliases();
                final RequestInfo aliasesRequestInfo = new RequestInfo(authentication, request, IndicesAliasesAction.NAME);
                authzEngine.authorizeIndexAction(aliasesRequestInfo, authzInfo,
                    ril -> {
                        resolvedIndicesAsyncSupplier.getAsync(ActionListener.wrap(resolvedIndices -> {
                            List<String> aliasesAndIndices = new ArrayList<>(resolvedIndices.getLocal());
                            for (Alias alias : aliases) {
                                aliasesAndIndices.add(alias.name());
                            }
                            ResolvedIndices withAliases = new ResolvedIndices(aliasesAndIndices, Collections.emptyList());
                            ril.onResponse(withAliases);
                        }, ril::onFailure));
                    },
                    metadata.getIndicesLookup(),
                    wrapPreservingContext(new AuthorizationResultListener<>(
                        authorizationResult -> runRequestInterceptors(requestInfo, authzInfo, authorizationEngine, listener),
                        listener::onFailure, aliasesRequestInfo, requestId, authzInfo), threadContext));
            }
        } else if (action.equals(TransportShardBulkAction.ACTION_NAME)) {
            // if this is performing multiple actions on the index, then check each of those actions.
            assert request instanceof BulkShardRequest
                : "Action " + action + " requires " + BulkShardRequest.class + " but was " + request.getClass();
            authorizeBulkItems(requestInfo, authzInfo, authzEngine, resolvedIndicesAsyncSupplier, authorizedIndicesSupplier, metadata,
                    requestId,
                    wrapPreservingContext(
                            ActionListener.wrap(ignore -> runRequestInterceptors(requestInfo, authzInfo, authorizationEngine, listener),
                                    listener::onFailure),
                            threadContext));
        } else {
            runRequestInterceptors(requestInfo, authzInfo, authorizationEngine, listener);
        }
    }

    private void runRequestInterceptors(RequestInfo requestInfo, AuthorizationInfo authorizationInfo,
                                        AuthorizationEngine authorizationEngine, ActionListener<Void> listener) {
        if (requestInterceptors.isEmpty()) {
            listener.onResponse(null);
        } else {
            final Iterator<RequestInterceptor> requestInterceptorIterator = requestInterceptors.iterator();
            requestInterceptorIterator.next().intercept(requestInfo, authorizationEngine, authorizationInfo,
                new ActionListener.Delegating<>(listener) {
                    @Override
                    public void onResponse(Void unused) {
                        if (requestInterceptorIterator.hasNext()) {
                            requestInterceptorIterator.next().intercept(requestInfo, authorizationEngine, authorizationInfo, this);
                        } else {
                            listener.onResponse(null);
                        }
                    }
                }
            );
        }
    }


    // pkg-private for testing
    AuthorizationEngine getRunAsAuthorizationEngine(final Authentication authentication) {
        return getAuthorizationEngineForUser(authentication.getUser().authenticatedUser());
    }

    // pkg-private for testing
    AuthorizationEngine getAuthorizationEngine(final Authentication authentication) {
        return getAuthorizationEngineForUser(authentication.getUser());
    }

    private AuthorizationEngine getAuthorizationEngineForUser(final User user) {
        if (rbacEngine != authorizationEngine && licenseState.isSecurityEnabled() &&
            licenseState.checkFeature(Feature.SECURITY_AUTHORIZATION_ENGINE)) {
            if (ClientReservedRealm.isReserved(user.principal(), settings) || isInternal(user)) {
                return rbacEngine;
            } else {
                return authorizationEngine;
            }
        } else {
            return rbacEngine;
        }
    }

    private void authorizeSystemUser(final Authentication authentication, final String action, final String requestId,
                                     final TransportRequest request, final ActionListener<Void> listener) {
        final AuditTrail auditTrail = auditTrailService.get();
        if (SystemUser.isAuthorized(action)) {
            threadContext.putTransient(INDICES_PERMISSIONS_KEY, IndicesAccessControl.ALLOW_ALL);
            threadContext.putTransient(AUTHORIZATION_INFO_KEY, SYSTEM_AUTHZ_INFO);
            auditTrail.accessGranted(requestId, authentication, action, request, SYSTEM_AUTHZ_INFO);
            listener.onResponse(null);
        } else {
            auditTrail.accessDenied(requestId, authentication, action, request, SYSTEM_AUTHZ_INFO);
            listener.onFailure(denialException(authentication, action, request, null));
        }
    }

    private TransportRequest maybeUnwrapRequest(Authentication authentication, TransportRequest originalRequest, String action,
                                                String requestId) {
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
                IllegalStateException cause = new IllegalStateException("originalRequest is not a proxy request: [" + originalRequest +
                    "] but action: [" + action + "] is a proxy action");
                auditTrail.accessDenied(requestId, authentication, action, request, EmptyAuthorizationInfo.INSTANCE);
                throw denialException(authentication, action, request, cause);
            }
            if (TransportActionProxy.isProxyRequest(originalRequest) && TransportActionProxy.isProxyAction(action) == false) {
                IllegalStateException cause = new IllegalStateException("originalRequest is a proxy request for: [" + request +
                    "] but action: [" + action + "] isn't");
                auditTrail.accessDenied(requestId, authentication, action, request, EmptyAuthorizationInfo.INSTANCE);
                throw denialException(authentication, action, request, cause);
            }
        }
        return request;
    }

    private void authorizeRunAs(final RequestInfo requestInfo, final AuthorizationInfo authzInfo,
                                final ActionListener<AuthorizationResult> listener) {
        final Authentication authentication = requestInfo.getAuthentication();
        if (authentication.getLookedUpBy() == null) {
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
     * {@link #denialException(Authentication, String, TransportRequest, Exception) access
     * denied} exception. Because a shard level request is for exactly 1 index, and
     * there are a small number of possible item {@link DocWriteRequest.OpType
     * types}, the number of distinct authorization checks that need to be performed
     * is very small, but the results must be cached, to avoid adding a high
     * overhead to each bulk request.
     */
    private void authorizeBulkItems(RequestInfo requestInfo, AuthorizationInfo authzInfo,
                                    AuthorizationEngine authzEngine, AsyncSupplier<ResolvedIndices> resolvedIndicesAsyncSupplier,
                                    AsyncSupplier<Set<String>> authorizedIndicesSupplier,
                                    Metadata metadata, String requestId, ActionListener<Void> listener) {
        final Authentication authentication = requestInfo.getAuthentication();
        final BulkShardRequest request = (BulkShardRequest) requestInfo.getRequest();
        // Maps original-index -> expanded-index-name (expands date-math, but not aliases)
        final Map<String, String> resolvedIndexNames = new HashMap<>();
        // Maps action -> resolved indices set
        final Map<String, Set<String>> actionToIndicesMap = new HashMap<>();
        final AuditTrail auditTrail = auditTrailService.get();

        authorizedIndicesSupplier.getAsync(ActionListener.wrap(authorizedIndices -> {
            resolvedIndicesAsyncSupplier.getAsync(ActionListener.wrap(overallResolvedIndices -> {
                final Set<String> localIndices = new HashSet<>(overallResolvedIndices.getLocal());
                for (BulkItemRequest item : request.items()) {
                    final String itemAction = getAction(item);
                    String resolvedIndex = resolvedIndexNames.computeIfAbsent(item.index(), key -> {
                        final ResolvedIndices resolvedIndices =
                            indicesAndAliasesResolver.resolveIndicesAndAliases(itemAction, item.request(), metadata, authorizedIndices);
                        if (resolvedIndices.getRemote().size() != 0) {
                            throw illegalArgument("Bulk item should not write to remote indices, but request writes to "
                                + String.join(",", resolvedIndices.getRemote()));
                        }
                        if (resolvedIndices.getLocal().size() != 1) {
                            throw illegalArgument("Bulk item should write to exactly 1 index, but request writes to "
                                + String.join(",", resolvedIndices.getLocal()));
                        }
                        final String resolved = resolvedIndices.getLocal().get(0);
                        if (localIndices.contains(resolved) == false) {
                            throw illegalArgument("Found bulk item that writes to index " + resolved + " but the request writes to " +
                                localIndices);
                        }
                        return resolved;
                    });

                    actionToIndicesMap.compute(itemAction, (key, resolvedIndicesSet) -> {
                        final Set<String> localSet = resolvedIndicesSet != null ? resolvedIndicesSet : new HashSet<>();
                        localSet.add(resolvedIndex);
                        return localSet;
                    });
                }

                final ActionListener<Collection<Tuple<String, IndexAuthorizationResult>>> bulkAuthzListener =
                    ActionListener.wrap(collection -> {
                        final Map<String, IndicesAccessControl> actionToIndicesAccessControl = new HashMap<>();
                        final AtomicBoolean audit = new AtomicBoolean(false);
                        collection.forEach(tuple -> {
                            final IndicesAccessControl existing =
                                actionToIndicesAccessControl.putIfAbsent(tuple.v1(), tuple.v2().getIndicesAccessControl());
                            if (existing != null) {
                                throw new IllegalStateException("a value already exists for action " + tuple.v1());
                            }
                            if (tuple.v2().isAuditable()) {
                                audit.set(true);
                            }
                        });

                        for (BulkItemRequest item : request.items()) {
                            final String resolvedIndex = resolvedIndexNames.get(item.index());
                            final String itemAction = getAction(item);
                            final IndicesAccessControl indicesAccessControl = actionToIndicesAccessControl.get(itemAction);
                            final IndicesAccessControl.IndexAccessControl indexAccessControl
                                = indicesAccessControl.getIndexPermissions(resolvedIndex);
                            if (indexAccessControl == null || indexAccessControl.isGranted() == false) {
                                auditTrail.explicitIndexAccessEvent(requestId, AuditLevel.ACCESS_DENIED, authentication, itemAction,
                                        resolvedIndex, item.getClass().getSimpleName(), request.remoteAddress(), authzInfo);
                                item.abort(resolvedIndex, denialException(authentication, itemAction, request,
                                    AuthorizationEngine.IndexAuthorizationResult.getFailureDescription(List.of(resolvedIndex)), null));
                            } else if (audit.get()) {
                                auditTrail.explicitIndexAccessEvent(requestId, AuditLevel.ACCESS_GRANTED, authentication, itemAction,
                                        resolvedIndex, item.getClass().getSimpleName(), request.remoteAddress(), authzInfo);
                            }
                        }
                        listener.onResponse(null);
                    }, listener::onFailure);
                final ActionListener<Tuple<String, IndexAuthorizationResult>> groupedActionListener = wrapPreservingContext(
                    new GroupedActionListener<>(bulkAuthzListener, actionToIndicesMap.size()), threadContext);

                actionToIndicesMap.forEach((bulkItemAction, indices) -> {
                    final RequestInfo bulkItemInfo =
                        new RequestInfo(requestInfo.getAuthentication(), requestInfo.getRequest(), bulkItemAction);
                    authzEngine.authorizeIndexAction(bulkItemInfo, authzInfo,
                        ril -> ril.onResponse(new ResolvedIndices(new ArrayList<>(indices), Collections.emptyList())),
                        metadata.getIndicesLookup(), ActionListener.wrap(indexAuthorizationResult ->
                                groupedActionListener.onResponse(new Tuple<>(bulkItemAction, indexAuthorizationResult)),
                            groupedActionListener::onFailure));
                });
            }, listener::onFailure));
        }, listener::onFailure));
    }

    private static IllegalArgumentException illegalArgument(String message) {
        assert false : message;
        return new IllegalArgumentException(message);
    }

    private static boolean isIndexAction(String action) {
        return IndexPrivilege.ACTION_MATCHER.test(action);
    }

    private static String getAction(BulkItemRequest item) {
        final DocWriteRequest<?> docWriteRequest = item.request();
        switch (docWriteRequest.opType()) {
            case INDEX:
                return IMPLIED_INDEX_ACTION;
            case CREATE:
                return IMPLIED_CREATE_ACTION;
            case UPDATE:
                return UpdateAction.NAME;
            case DELETE:
                return DeleteAction.NAME;
        }
        throw new IllegalArgumentException("No equivalent action for opType [" + docWriteRequest.opType() + "]");
    }

    private void putTransientIfNonExisting(String key, Object value) {
        Object existing = threadContext.getTransient(key);
        if (existing == null) {
            threadContext.putTransient(key, value);
        }
    }

    private ElasticsearchSecurityException denialException(Authentication authentication, String action, TransportRequest request,
                                                           Exception cause) {
        return denialException(authentication, action, request, null, cause);
    }

    private ElasticsearchSecurityException denialException(Authentication authentication, String action, TransportRequest request,
                                                           @Nullable String context, Exception cause) {
        final User authUser = authentication.getUser().authenticatedUser();
        // Special case for anonymous user
        if (isAnonymousEnabled && anonymousUser.equals(authUser)) {
            if (anonymousAuthzExceptionEnabled == false) {
                return authcFailureHandler.authenticationRequired(action, threadContext);
            }
        }

        String userText = "user [" + authUser.principal() + "]";
        // check for run as
        if (authentication.getUser().isRunAs()) {
            userText = userText + " run as [" + authentication.getUser().principal() + "]";
        }
        // check for authentication by API key
        if (AuthenticationType.API_KEY == authentication.getAuthenticationType()) {
            final String apiKeyId = (String) authentication.getMetadata().get(ApiKeyService.API_KEY_ID_KEY);
            assert apiKeyId != null : "api key id must be present in the metadata";
            userText = "API key id [" + apiKeyId + "] of " + userText;
        } else if (false == authentication.isServiceAccount()) {
            // Don't print roles for API keys because they're not meaningful
            // Also not printing roles for service accounts since they have no roles
            userText = userText + " with roles [" + Strings.arrayToCommaDelimitedString(authentication.getUser().roles()) + "]";
        }

        String message = "action [" + action + "] is unauthorized for " + userText;
        if (context != null) {
            message = message + " " + context;
        }

        if (ClusterPrivilegeResolver.isClusterAction(action)) {
            final Collection<String> privileges = ClusterPrivilegeResolver.findPrivilegesThatGrant(action, request, authentication);
            if (privileges != null && privileges.size() > 0) {
                message = message + ", this action is granted by the cluster privileges ["
                    + collectionToCommaDelimitedString(privileges) + "]";
            }
        } else if (isIndexAction(action)) {
            final Collection<String> privileges = IndexPrivilege.findPrivilegesThatGrant(action);
            if (privileges != null && privileges.size() > 0) {
                message = message + ", this action is granted by the index privileges ["
                    + collectionToCommaDelimitedString(privileges) + "]";
            }
        }

        logger.debug(message);
        return authorizationError(message, cause);
    }

    private class AuthorizationResultListener<T extends AuthorizationResult> implements ActionListener<T> {

        private final Consumer<T> responseConsumer;
        private final Consumer<Exception> failureConsumer;
        private final RequestInfo requestInfo;
        private final String requestId;
        private final AuthorizationInfo authzInfo;

        private AuthorizationResultListener(Consumer<T> responseConsumer, Consumer<Exception> failureConsumer, RequestInfo requestInfo,
                                            String requestId, AuthorizationInfo authzInfo) {
            this.responseConsumer = responseConsumer;
            this.failureConsumer = failureConsumer;
            this.requestInfo = requestInfo;
            this.requestId = requestId;
            this.authzInfo = authzInfo;
        }

        @Override
        public void onResponse(T result) {
            if (result.isGranted()) {
                if (result.isAuditable()) {
                    auditTrailService.get().accessGranted(requestId, requestInfo.getAuthentication(),
                        requestInfo.getAction(), requestInfo.getRequest(), authzInfo);
                }
                try {
                    responseConsumer.accept(result);
                } catch (Exception e) {
                    failureConsumer.accept(e);
                }
            } else {
                handleFailure(result.isAuditable(), result.getFailureContext(), null);
            }
        }

        @Override
        public void onFailure(Exception e) {
            handleFailure(true, null, e);
        }

        private void handleFailure(boolean audit, @Nullable String context, @Nullable Exception e) {
            if (audit) {
                auditTrailService.get().accessDenied(requestId, requestInfo.getAuthentication(), requestInfo.getAction(),
                    requestInfo.getRequest(), authzInfo);
            }
            failureConsumer.accept(
                denialException(requestInfo.getAuthentication(), requestInfo.getAction(), requestInfo.getRequest(), context, e));
        }
    }

    private static class CachingAsyncSupplier<V> implements AsyncSupplier<V> {

        private final AsyncSupplier<V> asyncSupplier;
        private volatile ListenableFuture<V> valueFuture = null;

        private CachingAsyncSupplier(AsyncSupplier<V> supplier) {
            this.asyncSupplier = supplier;
        }

        @Override
        public void getAsync(ActionListener<V> listener) {
            if (valueFuture == null) {
                boolean firstInvocation = false;
                synchronized (this) {
                    if (valueFuture == null) {
                        valueFuture = new ListenableFuture<>();
                        firstInvocation = true;
                    }
                }
                if (firstInvocation) {
                    asyncSupplier.getAsync(valueFuture);
                }
            }
            valueFuture.addListener(listener);
        }
    }

    public static void addSettings(List<Setting<?>> settings) {
        settings.add(ANONYMOUS_AUTHORIZATION_EXCEPTION_SETTING);
    }
}

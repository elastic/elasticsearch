/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authz;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.StepListener;
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
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesResponse;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationFailureHandler;
import org.elasticsearch.xpack.core.security.authc.esnative.ClientReservedRealm;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AsyncSupplier;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationResult;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.EmptyAuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.IndexAuthorizationResult;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.RequestInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.ResolvedIndices;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.XPackSecurityUser;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.security.audit.AuditLevel;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.audit.AuditUtil;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authz.interceptor.RequestInterceptor;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;

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
import static org.elasticsearch.xpack.core.security.SecurityField.setting;
import static org.elasticsearch.xpack.core.security.support.Exceptions.authorizationError;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.PRINCIPAL_ROLES_FIELD_NAME;

public class AuthorizationService {
    public static final Setting<Boolean> ANONYMOUS_AUTHORIZATION_EXCEPTION_SETTING =
        Setting.boolSetting(setting("authc.anonymous.authz_exception"), true, Property.NodeScope);
    public static final String ORIGINATING_ACTION_KEY = "_originating_action_name";
    public static final String AUTHORIZATION_INFO_KEY = "_authz_info";
    private static final AuthorizationInfo SYSTEM_AUTHZ_INFO =
        () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, new String[] { SystemUser.ROLE_NAME });

    private static final Logger logger = LogManager.getLogger(AuthorizationService.class);

    private final Settings settings;
    private final ClusterService clusterService;
    private final AuditTrailService auditTrail;
    private final IndicesAndAliasesResolver indicesAndAliasesResolver;
    private final AuthenticationFailureHandler authcFailureHandler;
    private final ThreadContext threadContext;
    private final AnonymousUser anonymousUser;
    private final AuthorizationEngine rbacEngine;
    private final AuthorizationEngine authorizationEngine;
    private final Set<RequestInterceptor> requestInterceptors;
    private final XPackLicenseState licenseState;
    private final boolean isAnonymousEnabled;
    private final boolean anonymousAuthzExceptionEnabled;

    public AuthorizationService(Settings settings, CompositeRolesStore rolesStore, ClusterService clusterService,
                                AuditTrailService auditTrail, AuthenticationFailureHandler authcFailureHandler,
                                ThreadPool threadPool, AnonymousUser anonymousUser, @Nullable AuthorizationEngine authorizationEngine,
                                Set<RequestInterceptor> requestInterceptors, XPackLicenseState licenseState) {
        this.clusterService = clusterService;
        this.auditTrail = auditTrail;
        this.indicesAndAliasesResolver = new IndicesAndAliasesResolver(settings, clusterService);
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
        // prior to doing any authorization lets set the originating action in the context only
        putTransientIfNonExisting(ORIGINATING_ACTION_KEY, action);

        String auditId = AuditUtil.extractRequestId(threadContext);
        if (auditId == null) {
            // We would like to assert that there is an existing request-id, but if this is a system action, then that might not be
            // true because the request-id is generated during authentication
            if (isInternalUser(authentication.getUser()) != false) {
                auditId = AuditUtil.getOrGenerateRequestId(threadContext);
            } else {
                auditTrail.tamperedRequest(null, authentication.getUser(), action, originalRequest);
                final String message = "Attempt to authorize action [" + action + "] for [" + authentication.getUser().principal()
                    + "] without an existing request-id";
                assert false : message;
                listener.onFailure(new ElasticsearchSecurityException(message));
            }
        }

        // sometimes a request might be wrapped within another, which is the case for proxied
        // requests and concrete shard requests
        final TransportRequest unwrappedRequest = maybeUnwrapRequest(authentication, originalRequest, action, auditId);
        if (SystemUser.is(authentication.getUser())) {
            // this never goes async so no need to wrap the listener
            authorizeSystemUser(authentication, action, auditId, unwrappedRequest, listener);
        } else {
            final String finalAuditId = auditId;
            final RequestInfo requestInfo = new RequestInfo(authentication, unwrappedRequest, action);
            final ActionListener<AuthorizationInfo> authzInfoListener = wrapPreservingContext(ActionListener.wrap(
                authorizationInfo -> {
                    putTransientIfNonExisting(AUTHORIZATION_INFO_KEY, authorizationInfo);
                    maybeAuthorizeRunAs(requestInfo, finalAuditId, authorizationInfo, listener);
                }, listener::onFailure), threadContext);
            getAuthorizationEngine(authentication).resolveAuthorizationInfo(requestInfo, authzInfoListener);
        }
    }

    private void maybeAuthorizeRunAs(final RequestInfo requestInfo, final String requestId, final AuthorizationInfo authzInfo,
                                     final ActionListener<Void> listener) {
        final Authentication authentication = requestInfo.getAuthentication();
        final TransportRequest request = requestInfo.getRequest();
        final String action = requestInfo.getAction();
        final boolean isRunAs = authentication.getUser().isRunAs();
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
                    listener.onFailure(denialException(authentication, action, null));
                }
            }, e -> {
                auditTrail.runAsDenied(requestId, authentication, action, request,
                    authzInfo.getAuthenticatedUserAuthorizationInfo());
                listener.onFailure(denialException(authentication, action, null));
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
        if (ClusterPrivilegeResolver.isClusterAction(action)) {
            final ActionListener<AuthorizationResult> clusterAuthzListener =
                wrapPreservingContext(new AuthorizationResultListener<>(result -> {
                        putTransientIfNonExisting(AuthorizationServiceField.INDICES_PERMISSIONS_KEY, IndicesAccessControl.ALLOW_ALL);
                        listener.onResponse(null);
                    }, listener::onFailure, requestInfo, requestId, authzInfo), threadContext);
            authzEngine.authorizeClusterAction(requestInfo, authzInfo, clusterAuthzListener);
        } else if (IndexPrivilege.ACTION_MATCHER.test(action)) {
            final MetaData metaData = clusterService.state().metaData();
            final AsyncSupplier<List<String>> authorizedIndicesSupplier = new CachingAsyncSupplier<>(authzIndicesListener ->
                authzEngine.loadAuthorizedIndices(requestInfo, authzInfo, metaData.getAliasAndIndexLookup(),
                    authzIndicesListener));
            final AsyncSupplier<ResolvedIndices> resolvedIndicesAsyncSupplier = new CachingAsyncSupplier<>((resolvedIndicesListener) -> {
                authorizedIndicesSupplier.getAsync(ActionListener.wrap(authorizedIndices -> {
                    resolveIndexNames(request, metaData, authorizedIndices, resolvedIndicesListener);
                }, e -> {
                    auditTrail.accessDenied(requestId, authentication, action, request, authzInfo);
                    if (e instanceof IndexNotFoundException) {
                        listener.onFailure(e);
                    } else {
                        listener.onFailure(denialException(authentication, action, e));
                    }
                }));
            });
            authzEngine.authorizeIndexAction(requestInfo, authzInfo, resolvedIndicesAsyncSupplier,
                metaData.getAliasAndIndexLookup(), wrapPreservingContext(new AuthorizationResultListener<>(result ->
                    handleIndexActionAuthorizationResult(result, requestInfo, requestId, authzInfo, authzEngine, authorizedIndicesSupplier,
                        resolvedIndicesAsyncSupplier, metaData, listener),
                    listener::onFailure, requestInfo, requestId, authzInfo), threadContext));
        } else {
            logger.warn("denying access as action [{}] is not an index or cluster action", action);
            auditTrail.accessDenied(requestId, authentication, action, request, authzInfo);
            listener.onFailure(denialException(authentication, action, null));
        }
    }

    private void handleIndexActionAuthorizationResult(final IndexAuthorizationResult result, final RequestInfo requestInfo,
                                                      final String requestId, final AuthorizationInfo authzInfo,
                                                      final AuthorizationEngine authzEngine,
                                                      final AsyncSupplier<List<String>> authorizedIndicesSupplier,
                                                      final AsyncSupplier<ResolvedIndices> resolvedIndicesAsyncSupplier,
                                                      final MetaData metaData,
                                                      final ActionListener<Void> listener) {
        final Authentication authentication = requestInfo.getAuthentication();
        final TransportRequest request = requestInfo.getRequest();
        final String action = requestInfo.getAction();
        if (result.getIndicesAccessControl() != null) {
            putTransientIfNonExisting(AuthorizationServiceField.INDICES_PERMISSIONS_KEY,
                result.getIndicesAccessControl());
        }
        //if we are creating an index we need to authorize potential aliases created at the same time
        if (IndexPrivilege.CREATE_INDEX_MATCHER.test(action)) {
            assert request instanceof CreateIndexRequest;
            Set<Alias> aliases = ((CreateIndexRequest) request).aliases();
            if (aliases.isEmpty()) {
                runRequestInterceptors(requestInfo, authzInfo, authorizationEngine, listener);
            } else {
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
                    metaData.getAliasAndIndexLookup(),
                    wrapPreservingContext(new AuthorizationResultListener<>(
                        authorizationResult -> runRequestInterceptors(requestInfo, authzInfo, authorizationEngine, listener),
                        listener::onFailure, aliasesRequestInfo, requestId, authzInfo), threadContext));
            }
        } else if (action.equals(TransportShardBulkAction.ACTION_NAME)) {
            // if this is performing multiple actions on the index, then check each of those actions.
            assert request instanceof BulkShardRequest
                : "Action " + action + " requires " + BulkShardRequest.class + " but was " + request.getClass();
            authorizeBulkItems(requestInfo, authzInfo, authzEngine, resolvedIndicesAsyncSupplier, authorizedIndicesSupplier, metaData,
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
            Iterator<RequestInterceptor> requestInterceptorIterator = requestInterceptors.iterator();
            final StepListener<Void> firstStepListener = new StepListener<>();
            final RequestInterceptor first = requestInterceptorIterator.next();

            StepListener<Void> prevListener = firstStepListener;
            while (requestInterceptorIterator.hasNext()) {
                final RequestInterceptor nextInterceptor = requestInterceptorIterator.next();
                final StepListener<Void> current = new StepListener<>();
                prevListener.whenComplete(v -> nextInterceptor.intercept(requestInfo, authorizationEngine, authorizationInfo, current),
                    listener::onFailure);
                prevListener = current;
            }

            prevListener.whenComplete(v -> listener.onResponse(null), listener::onFailure);
            first.intercept(requestInfo, authorizationEngine, authorizationInfo, firstStepListener);
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
        if (rbacEngine != authorizationEngine && licenseState.isAuthorizationEngineAllowed()) {
            if (ClientReservedRealm.isReserved(user.principal(), settings) || isInternalUser(user)) {
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
        if (SystemUser.isAuthorized(action)) {
            putTransientIfNonExisting(AuthorizationServiceField.INDICES_PERMISSIONS_KEY, IndicesAccessControl.ALLOW_ALL);
            putTransientIfNonExisting(AUTHORIZATION_INFO_KEY, SYSTEM_AUTHZ_INFO);
            auditTrail.accessGranted(requestId, authentication, action, request, SYSTEM_AUTHZ_INFO);
            listener.onResponse(null);
        } else {
            auditTrail.accessDenied(requestId, authentication, action, request, SYSTEM_AUTHZ_INFO);
            listener.onFailure(denialException(authentication, action, null));
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
            if (isProxyAction && isOriginalRequestProxyRequest == false) {
                IllegalStateException cause = new IllegalStateException("originalRequest is not a proxy request: [" + originalRequest +
                    "] but action: [" + action + "] is a proxy action");
                auditTrail.accessDenied(requestId, authentication, action, request, EmptyAuthorizationInfo.INSTANCE);
                throw denialException(authentication, action, cause);
            }
            if (TransportActionProxy.isProxyRequest(originalRequest) && TransportActionProxy.isProxyAction(action) == false) {
                IllegalStateException cause = new IllegalStateException("originalRequest is a proxy request for: [" + request +
                    "] but action: [" + action + "] isn't");
                auditTrail.accessDenied(requestId, authentication, action, request, EmptyAuthorizationInfo.INSTANCE);
                throw denialException(authentication, action, cause);
            }
        }
        return request;
    }

    private boolean isInternalUser(User user) {
        return SystemUser.is(user) || XPackUser.is(user) || XPackSecurityUser.is(user);
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
     * {@link #denialException(Authentication, String, Exception) access
     * denied} exception. Because a shard level request is for exactly 1 index, and
     * there are a small number of possible item {@link DocWriteRequest.OpType
     * types}, the number of distinct authorization checks that need to be performed
     * is very small, but the results must be cached, to avoid adding a high
     * overhead to each bulk request.
     */
    private void authorizeBulkItems(RequestInfo requestInfo, AuthorizationInfo authzInfo,
                                    AuthorizationEngine authzEngine, AsyncSupplier<ResolvedIndices> resolvedIndicesAsyncSupplier,
                                    AsyncSupplier<List<String>> authorizedIndicesSupplier,
                                    MetaData metaData, String requestId, ActionListener<Void> listener) {
        final Authentication authentication = requestInfo.getAuthentication();
        final BulkShardRequest request = (BulkShardRequest) requestInfo.getRequest();
        // Maps original-index -> expanded-index-name (expands date-math, but not aliases)
        final Map<String, String> resolvedIndexNames = new HashMap<>();
        // Maps action -> resolved indices set
        final Map<String, Set<String>> actionToIndicesMap = new HashMap<>();

        authorizedIndicesSupplier.getAsync(ActionListener.wrap(authorizedIndices -> {
            resolvedIndicesAsyncSupplier.getAsync(ActionListener.wrap(overallResolvedIndices -> {
                final Set<String> localIndices = new HashSet<>(overallResolvedIndices.getLocal());
                for (BulkItemRequest item : request.items()) {
                    String resolvedIndex = resolvedIndexNames.computeIfAbsent(item.index(), key -> {
                        final ResolvedIndices resolvedIndices =
                            indicesAndAliasesResolver.resolveIndicesAndAliases(item.request(), metaData, authorizedIndices);
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

                    final String itemAction = getAction(item);
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
                                item.abort(resolvedIndex, denialException(authentication, itemAction, null));
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
                        metaData.getAliasAndIndexLookup(), ActionListener.wrap(indexAuthorizationResult ->
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

    private static String getAction(BulkItemRequest item) {
        final DocWriteRequest<?> docWriteRequest = item.request();
        switch (docWriteRequest.opType()) {
            case INDEX:
            case CREATE:
                return IndexAction.NAME;
            case UPDATE:
                return UpdateAction.NAME;
            case DELETE:
                return DeleteAction.NAME;
        }
        throw new IllegalArgumentException("No equivalent action for opType [" + docWriteRequest.opType() + "]");
    }

    private void resolveIndexNames(TransportRequest request, MetaData metaData, List<String> authorizedIndices,
                                   ActionListener<ResolvedIndices> listener) {
        listener.onResponse(indicesAndAliasesResolver.resolve(request, metaData, authorizedIndices));
    }

    private void putTransientIfNonExisting(String key, Object value) {
        Object existing = threadContext.getTransient(key);
        if (existing == null) {
            threadContext.putTransient(key, value);
        }
    }

    private ElasticsearchSecurityException denialException(Authentication authentication, String action, Exception cause) {
        final User authUser = authentication.getUser().authenticatedUser();
        // Special case for anonymous user
        if (isAnonymousEnabled && anonymousUser.equals(authUser)) {
            if (anonymousAuthzExceptionEnabled == false) {
                return authcFailureHandler.authenticationRequired(action, threadContext);
            }
        }
        // check for run as
        if (authentication.getUser().isRunAs()) {
            logger.debug("action [{}] is unauthorized for user [{}] run as [{}]", action, authUser.principal(),
                    authentication.getUser().principal());
            return authorizationError("action [{}] is unauthorized for user [{}] run as [{}]", cause, action, authUser.principal(),
                    authentication.getUser().principal());
        }
        // check for authentication by API key
        if (authentication.getAuthenticatedBy().getType().equals(ApiKeyService.API_KEY_REALM_TYPE)) {
            final String apiKeyId = (String) authentication.getMetadata().get(ApiKeyService.API_KEY_ID_KEY);
            assert apiKeyId != null : "api key id must be present in the metadata";
            logger.debug("action [{}] is unauthorized for API key id [{}] of user [{}]", action, apiKeyId, authUser.principal());
            return authorizationError("action [{}] is unauthorized for API key id [{}] of user [{}]", cause, action, apiKeyId,
                authUser.principal());
        }
        logger.debug("action [{}] is unauthorized for user [{}]", action, authUser.principal());
        return authorizationError("action [{}] is unauthorized for user [{}]", cause, action, authUser.principal());
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
                    auditTrail.accessGranted(requestId, requestInfo.getAuthentication(), requestInfo.getAction(), requestInfo.getRequest(),
                        authzInfo);
                }
                try {
                    responseConsumer.accept(result);
                } catch (Exception e) {
                    failureConsumer.accept(e);
                }
            } else {
                handleFailure(result.isAuditable(), null);
            }
        }

        @Override
        public void onFailure(Exception e) {
            handleFailure(true, e);
        }

        private void handleFailure(boolean audit, @Nullable Exception e) {
            if (audit) {
                auditTrail.accessDenied(requestId, requestInfo.getAuthentication(), requestInfo.getAction(), requestInfo.getRequest(),
                    authzInfo);
            }
            failureConsumer.accept(denialException(requestInfo.getAuthentication(), requestInfo.getAction(), e));
        }
    }

    private static class CachingAsyncSupplier<V> implements AsyncSupplier<V> {

        private final AsyncSupplier<V> asyncSupplier;
        private V value = null;

        private CachingAsyncSupplier(AsyncSupplier<V> supplier) {
            this.asyncSupplier = supplier;
        }

        @Override
        public synchronized void getAsync(ActionListener<V> listener) {
            if (value == null) {
                asyncSupplier.getAsync(ActionListener.wrap(loaded -> {
                    value = loaded;
                    listener.onResponse(value);
                }, listener::onFailure));
            } else {
                listener.onResponse(value);
            }
        }
    }

    public static void addSettings(List<Setting<?>> settings) {
        settings.add(ANONYMOUS_AUTHORIZATION_EXCEPTION_SETTING);
    }
}

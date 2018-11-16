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
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.replication.TransportReplicationAction.ConcreteShardRequest;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationFailureHandler;
import org.elasticsearch.xpack.core.security.authc.esnative.ClientReservedRealm;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.XPackSecurityUser;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.authz.IndicesAndAliasesResolver.ResolvedIndices;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.action.support.ContextPreservingActionListener.*;
import static org.elasticsearch.xpack.core.security.SecurityField.setting;
import static org.elasticsearch.xpack.core.security.support.Exceptions.authorizationError;
import static org.elasticsearch.xpack.security.authz.AuthorizationEngine.*;

public class AuthorizationService {
    public static final Setting<Boolean> ANONYMOUS_AUTHORIZATION_EXCEPTION_SETTING =
            Setting.boolSetting(setting("authc.anonymous.authz_exception"), true, Property.NodeScope);
    public static final String ORIGINATING_ACTION_KEY = "_originating_action_name";
    static final String ROLE_NAMES_KEY = "_effective_role_names";

    private static final Logger logger = LogManager.getLogger(AuthorizationService.class);

    private final Settings settings;
    private final ClusterService clusterService;
    private final CompositeRolesStore rolesStore;
    private final AuditTrailService auditTrail;
    private final IndicesAndAliasesResolver indicesAndAliasesResolver;
    private final AuthenticationFailureHandler authcFailureHandler;
    private final ThreadContext threadContext;
    private final AnonymousUser anonymousUser;
    private final FieldPermissionsCache fieldPermissionsCache;
    private final AuthorizationEngine rbacEngine;
    private final boolean isAnonymousEnabled;
    private final boolean anonymousAuthzExceptionEnabled;

    public AuthorizationService(Settings settings, CompositeRolesStore rolesStore, ClusterService clusterService,
                                AuditTrailService auditTrail, AuthenticationFailureHandler authcFailureHandler,
                                ThreadPool threadPool, AnonymousUser anonymousUser) {
        this.rolesStore = rolesStore;
        this.clusterService = clusterService;
        this.auditTrail = auditTrail;
        this.indicesAndAliasesResolver = new IndicesAndAliasesResolver(settings, clusterService);
        this.authcFailureHandler = authcFailureHandler;
        this.threadContext = threadPool.getThreadContext();
        this.anonymousUser = anonymousUser;
        this.isAnonymousEnabled = AnonymousUser.isAnonymousEnabled(settings);
        this.anonymousAuthzExceptionEnabled = ANONYMOUS_AUTHORIZATION_EXCEPTION_SETTING.get(settings);
        this.fieldPermissionsCache = new FieldPermissionsCache(settings);
        this.rbacEngine = new RBACEngine(settings, rolesStore, anonymousUser, isAnonymousEnabled);
        this.settings = settings;
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

        // sometimes a request might be wrapped within another, which is the case for proxied
        // requests and concrete shard requests
        final TransportRequest unwrappedRequest = maybeUnwrapRequest(authentication, originalRequest, action);
        if (SystemUser.is(authentication.getUser())) {
            // this never goes async so no need to wrap the listener
            authorizeSystemUser(authentication, action, unwrappedRequest, listener);
        } else {
            final ActionListener<AuthorizationInfo> authzInfoListener = wrapPreservingContext(ActionListener.wrap(
                authorizationInfo -> maybeAuthorizeRunAs(authentication, action, unwrappedRequest, authorizationInfo, listener),
                listener::onFailure), threadContext);
            getAuthorizationEngine(authentication).resolveAuthorizationInfo(authentication, unwrappedRequest, action, authzInfoListener);
        }
    }

    private void maybeAuthorizeRunAs(final Authentication authentication, final String action, final TransportRequest unwrappedRequest,
                                     final AuthorizationInfo authzInfo, final ActionListener<Void> listener) {
        final boolean isRunAs = authentication.getUser().isRunAs();
        if (isRunAs) {
            ActionListener<AuthorizationResult> runAsListener = wrapPreservingContext(ActionListener.wrap(result -> {
                if (result.isGranted()) {
                    if (result.isAuditable()) {
                        // TODO get roles from result as AuthzInfo?
                        auditTrail.runAsGranted(authentication, action, unwrappedRequest,
                            authentication.getUser().authenticatedUser().roles());
                    }
                    authorizePostRunAs(authentication, action, unwrappedRequest, authzInfo, listener);
                } else {
                    listener.onFailure(denyRunAs(authentication, action, unwrappedRequest,
                        authentication.getUser().authenticatedUser().roles()));
                }
            }, e -> {
                // TODO need a failure handler better than this!
                listener.onFailure(denyRunAs(authentication, action, unwrappedRequest,
                    authentication.getUser().authenticatedUser().roles(), e));
            }), threadContext);
            authorizeRunAs(authentication, unwrappedRequest, action, authzInfo, runAsListener);
        } else {
            authorizePostRunAs(authentication, action, unwrappedRequest, authzInfo, listener);
        }
    }

    private void authorizePostRunAs(final Authentication authentication, final String action,
                                    final TransportRequest unwrappedRequest, final AuthorizationInfo authzInfo,
                                    final ActionListener<Void> listener) {
        final AuthorizationEngine authzEngine = getAuthorizationEngine(authentication);
        if (ClusterPrivilege.ACTION_MATCHER.test(action)) {
            final ActionListener<AuthorizationResult> clusterAuthzListener = wrapPreservingContext(ActionListener.wrap(result -> {
                if (result.isGranted()) {
                    if (result.isAuditable()) {
                        // TODO authzInfo
                        auditTrail.accessGranted(authentication, action, unwrappedRequest, authentication.getUser().roles());
                    }
                    putTransientIfNonExisting(AuthorizationServiceField.INDICES_PERMISSIONS_KEY, IndicesAccessControl.ALLOW_ALL);
                    listener.onResponse(null);
                } else {
                    listener.onFailure(denial(authentication, action, unwrappedRequest, authentication.getUser().roles()));
                }
            }, e -> {
                // TODO need a failure handler better than this!
                listener.onFailure(denial(authentication, action, unwrappedRequest,
                    authentication.getUser().roles(), e));
            }), threadContext);
            authzEngine.authorizeClusterAction(authentication, unwrappedRequest, action, authzInfo, clusterAuthzListener);
        } else if (IndexPrivilege.ACTION_MATCHER.test(action)) {
            // scroll is special
            // some APIs are indices requests that are not actually associated with indices. For example,
            // search scroll request, is categorized under the indices context, but doesn't hold indices names
            // (in this case, the security check on the indices was done on the search request that initialized
            // the scroll. Given that scroll is implemented using a context on the node holding the shard, we
            // piggyback on it and enhance the context with the original authentication. This serves as our method
            // to validate the scroll id only stays with the same user!
            if (unwrappedRequest instanceof IndicesRequest == false && unwrappedRequest instanceof IndicesAliasesRequest == false) {
                //note that clear scroll shard level actions can originate from a clear scroll all, which doesn't require any
                //indices permission as it's categorized under cluster. This is why the scroll check is performed
                //even before checking if the user has any indices permission.
                if (isScrollRelatedAction(action)) {
                    // if the action is a search scroll action, we first authorize that the user can execute the action for some
                    // index and if they cannot, we can fail the request early before we allow the execution of the action and in
                    // turn the shard actions
                    if (SearchScrollAction.NAME.equals(action)) {
                        authorizeIndexActionName(authentication, action, unwrappedRequest, authzInfo, authzEngine, listener);
                    } else {
                        // we store the request as a transient in the ThreadContext in case of a authorization failure at the shard
                        // level. If authorization fails we will audit a access_denied message and will use the request to retrieve
                        // information such as the index and the incoming address of the request
                        auditTrail.accessGranted(authentication, action, unwrappedRequest, authentication.getUser().roles());
                        listener.onResponse(null);
                    }
                } else {
                    assert false :
                        "only scroll related requests are known indices api that don't support retrieving the indices they relate to";
                    listener.onFailure(denial(authentication, action, unwrappedRequest, authentication.getUser().roles()));
                }
            } else if (authzEngine.shouldAuthorizeIndexActionNameOnly(action)) {
                authorizeIndexActionName(authentication, action, unwrappedRequest, authzInfo, authzEngine, listener);
            } else {
                final MetaData metaData = clusterService.state().metaData();
                final AuthorizedIndices authorizedIndices = new AuthorizedIndices(() -> authzEngine.loadAuthorizedIndices(authentication,
                    unwrappedRequest, action, authzInfo, metaData.getAliasAndIndexLookup()));
                final ResolvedIndices resolvedIndices = resolveIndexNames(authentication, action, unwrappedRequest,
                    metaData, authorizedIndices, authzInfo);
                assert !resolvedIndices.isEmpty()
                    : "every indices request needs to have its indices set thus the resolved indices must not be empty";

                //all wildcard expressions have been resolved and only the security plugin could have set '-*' here.
                //'-*' matches no indices so we allow the request to go through, which will yield an empty response
                if (resolvedIndices.isNoIndicesPlaceholder()) {
                    authorizeIndexActionName(authentication, action, unwrappedRequest, authzInfo, authzEngine,
                        ActionListener.wrap(ignore -> {
                            putTransientIfNonExisting(AuthorizationServiceField.INDICES_PERMISSIONS_KEY,
                                IndicesAccessControl.ALLOW_NO_INDICES);
                            auditTrail.accessGranted(authentication, action, unwrappedRequest, authentication.getUser().roles());
                            listener.onResponse(null);
                        }, listener::onFailure));
                } else {
                    authorizeIndexAction(authentication, action, unwrappedRequest, authzInfo, authzEngine, resolvedIndices,
                        metaData, authorizedIndices, listener);
                }
            }
        } else {
            listener.onFailure(denial(authentication, action, unwrappedRequest, authentication.getUser().roles()));
        }
    }

    private void authorizeIndexActionName(final Authentication authentication, final String action,
                                          final TransportRequest unwrappedRequest, final AuthorizationInfo authzInfo,
                                          final AuthorizationEngine authzEngine, final ActionListener<Void> listener) {
        final ActionListener<AuthorizationResult> indexActionListener = wrapPreservingContext(ActionListener.wrap(result -> {
            if (result.isGranted()) {
                if (result.isAuditable()) {
                    // TODO authzInfo
                    auditTrail.accessGranted(authentication, action, unwrappedRequest, authentication.getUser().roles());
                }
                listener.onResponse(null);
            } else {
                listener.onFailure(denial(authentication, action, unwrappedRequest, authentication.getUser().roles()));
            }
        }, e -> {
            // TODO need a failure handler better than this!
            listener.onFailure(denial(authentication, action, unwrappedRequest,
                authentication.getUser().roles(), e));
        }), threadContext);
        authzEngine.authorizeIndexActionName(authentication, unwrappedRequest, action, authzInfo, indexActionListener);
    }

    private void authorizeIndexAction(final Authentication authentication, final String action,
                                      final TransportRequest unwrappedRequest, final AuthorizationInfo authzInfo,
                                      final AuthorizationEngine authzEngine, final ResolvedIndices resolvedIndices,
                                      final MetaData metaData, final AuthorizedIndices authorizedIndices,
                                      final ActionListener<Void> listener) {
        final Set<String> localIndices = Collections.unmodifiableSet(new HashSet<>(resolvedIndices.getLocal()));
        final ActionListener<IndexAuthorizationResult> indexActionListener = wrapPreservingContext(ActionListener.wrap(indexAuthorizationResult -> {
            if (indexAuthorizationResult.isGranted()) {
                putTransientIfNonExisting(AuthorizationServiceField.INDICES_PERMISSIONS_KEY,
                    indexAuthorizationResult.getIndicesAccessControl());
                //if we are creating an index we need to authorize potential aliases created at the same time
                if (IndexPrivilege.CREATE_INDEX_MATCHER.test(action)) {
                    assert unwrappedRequest instanceof CreateIndexRequest;
                    Set<Alias> aliases = ((CreateIndexRequest) unwrappedRequest).aliases();
                    if (aliases.isEmpty() == false) {
                        List<String> aliasesAndIndices = new ArrayList<>(localIndices);
                        for (Alias alias : aliases) {
                            aliasesAndIndices.add(alias.name());
                        }
                        ResolvedIndices withAliases = new ResolvedIndices(aliasesAndIndices, Collections.emptyList());
                        authorizeIndexAction(authentication, "indices:admin/aliases", unwrappedRequest, authzInfo, authzEngine,
                            withAliases, metaData, authorizedIndices, ActionListener.wrap(ignore -> {
                                if (indexAuthorizationResult.isAuditable()) {
                                    auditTrail.accessGranted(authentication, action, unwrappedRequest,
                                        authentication.getUser().roles());
                                }
                                listener.onResponse(null);
                            }, listener::onFailure));
                    }
                } else if (action.equals(TransportShardBulkAction.ACTION_NAME)) {
                    // is this is performing multiple actions on the index, then check each of those actions.
                    assert unwrappedRequest instanceof BulkShardRequest
                        : "Action " + action + " requires " + BulkShardRequest.class + " but was " + unwrappedRequest.getClass();

                    authorizeBulkItems(authentication, (BulkShardRequest) unwrappedRequest, authzInfo, authzEngine, localIndices,
                        authorizedIndices, metaData, ActionListener.wrap(ignore -> {
                            if (indexAuthorizationResult.isAuditable()) {
                                auditTrail.accessGranted(authentication, action, unwrappedRequest, authentication.getUser().roles());
                            }
                            listener.onResponse(null);
                        }, listener::onFailure));
                } else {
                    if (indexAuthorizationResult.isAuditable()) {
                        auditTrail.accessGranted(authentication, action, unwrappedRequest, authentication.getUser().roles());
                    }
                    listener.onResponse(null);
                }
            } else {
                listener.onFailure(denial(authentication, action, unwrappedRequest, authentication.getUser().roles()));
            }
        }, e -> {
            // TODO need a failure handler better than this!
            listener.onFailure(denial(authentication, action, unwrappedRequest,
                authentication.getUser().roles(), e));
        }), threadContext);

        authzEngine.buildIndicesAccessControl(authentication, unwrappedRequest, action, authzInfo, localIndices,
            metaData.getAliasAndIndexLookup(), indexActionListener);

    }

    private AuthorizationEngine getRunAsAuthorizationEngine(final Authentication authentication) {
        return ClientReservedRealm.isReserved(authentication.getUser().authenticatedUser().principal(), settings) ?
            rbacEngine : rbacEngine;
    }

    private AuthorizationEngine getAuthorizationEngine(final Authentication authentication) {
        return ClientReservedRealm.isReserved(authentication.getUser().principal(), settings) ?
            rbacEngine : rbacEngine;
    }

    private void authorizeSystemUser(final Authentication authentication, final String action, final TransportRequest request,
                                     final ActionListener<Void> listener) {
        if (SystemUser.isAuthorized(action)) {
            putTransientIfNonExisting(AuthorizationServiceField.INDICES_PERMISSIONS_KEY, IndicesAccessControl.ALLOW_ALL);
            putTransientIfNonExisting(ROLE_NAMES_KEY, new String[]{SystemUser.ROLE_NAME});
            auditTrail.accessGranted(authentication, action, request, new String[]{SystemUser.ROLE_NAME});
            listener.onResponse(null);
        } else {
            listener.onFailure(denial(authentication, action, request, new String[] { SystemUser.ROLE_NAME }));
        }
    }

    private TransportRequest maybeUnwrapRequest(Authentication authentication, TransportRequest originalRequest, String action) {
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
                throw denial(authentication, action, request, authentication.getUser().roles(), cause);
            }
            if (TransportActionProxy.isProxyRequest(originalRequest) && TransportActionProxy.isProxyAction(action) == false) {
                IllegalStateException cause = new IllegalStateException("originalRequest is a proxy request for: [" + request +
                    "] but action: [" + action + "] isn't");
                throw denial(authentication, action, request, authentication.getUser().roles(), cause);
            }
        }
        return request;
    }

    private void authorizeRunAs(final Authentication authentication, final TransportRequest request, final String action,
                                final AuthorizationInfo authzInfo, final ActionListener<AuthorizationResult> listener) {
        if (authentication.getLookedUpBy() == null) {
            // this user did not really exist
            // TODO(jaymode) find a better way to indicate lookup failed for a user and we need to fail authz
            throw denyRunAs(authentication, action, request, authentication.getUser().authenticatedUser().roles());
        } else {
            final AuthorizationEngine runAsAuthzEngine = getRunAsAuthorizationEngine(authentication);
            runAsAuthzEngine.authorizeRunAs(authentication, request, action, authzInfo, listener);
        }
    }

    /**
     * Performs authorization checks on the items within a {@link BulkShardRequest}.
     * This inspects the {@link BulkItemRequest items} within the request, computes
     * an <em>implied</em> action for each item's {@link DocWriteRequest#opType()},
     * and then checks whether that action is allowed on the targeted index. Items
     * that fail this checks are {@link BulkItemRequest#abort(String, Exception)
     * aborted}, with an
     * {@link #denial(Authentication, String, TransportRequest, String[]) access
     * denied} exception. Because a shard level request is for exactly 1 index, and
     * there are a small number of possible item {@link DocWriteRequest.OpType
     * types}, the number of distinct authorization checks that need to be performed
     * is very small, but the results must be cached, to avoid adding a high
     * overhead to each bulk request.
     */
    private void authorizeBulkItems(Authentication authentication, BulkShardRequest request, AuthorizationInfo authzInfo,
                                    AuthorizationEngine authzEngine, Set<String> localIndices, AuthorizedIndices authorizedIndices,
                                    MetaData metaData, ActionListener<Void> listener) {
        // Maps original-index -> expanded-index-name (expands date-math, but not aliases)
        final Map<String, String> resolvedIndexNames = new HashMap<>();
        // Maps action -> resolved indices set
        final Map<String, Set<String>> actionToIndicesMap = new HashMap<>();

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
                    final IndicesAccessControl indicesAccessControl = actionToIndicesAccessControl.get(getAction(item));
                    final IndicesAccessControl.IndexAccessControl indexAccessControl
                        = indicesAccessControl.getIndexPermissions(resolvedIndex);
                    if (indexAccessControl == null || indexAccessControl.isGranted() == false) {
                        item.abort(resolvedIndex, denial(authentication, itemAction, request, authentication.getUser().roles()));
                    } else if (audit.get()) {
                        auditTrail.accessGranted(authentication, itemAction, request, authentication.getUser().roles());
                    }
                }
                listener.onResponse(null);
            }, listener::onFailure);
        final ActionListener<Tuple<String, IndexAuthorizationResult>> groupedActionListener = wrapPreservingContext(
            new GroupedActionListener<>(bulkAuthzListener, actionToIndicesMap.size(), Collections.emptyList()), threadContext);

        actionToIndicesMap.forEach((bulkItemAction, indices) -> {
            authzEngine.buildIndicesAccessControl(authentication, request, bulkItemAction, authzInfo, indices,
                metaData.getAliasAndIndexLookup(), ActionListener.wrap(
                    indexAuthorizationResult -> groupedActionListener.onResponse(new Tuple<>(bulkItemAction, indexAuthorizationResult)),
                    groupedActionListener::onFailure));
        });
    }

    private IllegalArgumentException illegalArgument(String message) {
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

    private ResolvedIndices resolveIndexNames(Authentication authentication, String action, TransportRequest request,
                                              MetaData metaData, AuthorizedIndices authorizedIndices, AuthorizationInfo info) {
        try {
            return indicesAndAliasesResolver.resolve(request, metaData, authorizedIndices);
        } catch (Exception e) {
            auditTrail.accessDenied(authentication, action, request, authentication.getUser().roles()); // nocommit
            throw e;
        }
    }

    private void putTransientIfNonExisting(String key, Object value) {
        Object existing = threadContext.getTransient(key);
        if (existing == null) {
            threadContext.putTransient(key, value);
        }
    }

    public void roles(User user, ActionListener<Role> roleActionListener) {
        // we need to special case the internal users in this method, if we apply the anonymous roles to every user including these system
        // user accounts then we run into the chance of a deadlock because then we need to get a role that we may be trying to get as the
        // internal user. The SystemUser is special cased as it has special privileges to execute internal actions and should never be
        // passed into this method. The XPackUser has the Superuser role and we can simply return that
        if (SystemUser.is(user)) {
            throw new IllegalArgumentException("the user [" + user.principal() + "] is the system user and we should never try to get its" +
                    " roles");
        }
        if (XPackUser.is(user)) {
            assert XPackUser.INSTANCE.roles().length == 1;
            roleActionListener.onResponse(XPackUser.ROLE);
            return;
        }
        if (XPackSecurityUser.is(user)) {
            roleActionListener.onResponse(ReservedRolesStore.SUPERUSER_ROLE);
            return;
        }

        Set<String> roleNames = new HashSet<>();
        Collections.addAll(roleNames, user.roles());
        if (isAnonymousEnabled && anonymousUser.equals(user) == false) {
            if (anonymousUser.roles().length == 0) {
                throw new IllegalStateException("anonymous is only enabled when the anonymous user has roles");
            }
            Collections.addAll(roleNames, anonymousUser.roles());
        }

        if (roleNames.isEmpty()) {
            roleActionListener.onResponse(Role.EMPTY);
        } else if (roleNames.contains(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName())) {
            roleActionListener.onResponse(ReservedRolesStore.SUPERUSER_ROLE);
        } else {
            rolesStore.roles(roleNames, fieldPermissionsCache, roleActionListener);
        }
    }

    ElasticsearchSecurityException denial(Authentication authentication, String action, TransportRequest request, String[] roleNames) {
        return denial(authentication, action, request, roleNames, null);
    }

    ElasticsearchSecurityException denial(Authentication authentication, String action, TransportRequest request, String[] roleNames,
                                          Exception cause) {
        auditTrail.accessDenied(authentication, action, request, roleNames);
        return denialException(authentication, action, cause);
    }

    private ElasticsearchSecurityException denyRunAs(Authentication authentication, String action, TransportRequest request,
                                                     String[] roleNames, Exception cause) {
        auditTrail.runAsDenied(authentication, action, request, roleNames);
        return denialException(authentication, action, cause);
    }

    private ElasticsearchSecurityException denyRunAs(Authentication authentication, String action, TransportRequest request,
                                                     String[] roleNames) {
        return denyRunAs(authentication, action, request, roleNames, null);
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
        logger.debug("action [{}] is unauthorized for user [{}]", action, authUser.principal());
        return authorizationError("action [{}] is unauthorized for user [{}]", cause, action, authUser.principal());
    }

    private static boolean isScrollRelatedAction(String action) {
        return action.equals(SearchScrollAction.NAME) ||
            action.equals(SearchTransportService.FETCH_ID_SCROLL_ACTION_NAME) ||
            action.equals(SearchTransportService.QUERY_FETCH_SCROLL_ACTION_NAME) ||
            action.equals(SearchTransportService.QUERY_SCROLL_ACTION_NAME) ||
            action.equals(SearchTransportService.FREE_CONTEXT_SCROLL_ACTION_NAME) ||
            action.equals(ClearScrollAction.NAME) ||
            action.equals("indices:data/read/sql/close_cursor") ||
            action.equals(SearchTransportService.CLEAR_SCROLL_CONTEXTS_ACTION_NAME);
    }

    public static void addSettings(List<Setting<?>> settings) {
        settings.add(ANONYMOUS_AUTHORIZATION_EXCEPTION_SETTING);
    }
}

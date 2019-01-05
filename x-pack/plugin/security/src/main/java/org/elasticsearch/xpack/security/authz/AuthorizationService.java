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
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.support.replication.TransportReplicationAction.ConcreteShardRequest;
import org.elasticsearch.action.termvectors.MultiTermVectorsAction;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordAction;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.UserRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationFailureHandler;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.ClusterPermission;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.XPackSecurityUser;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.audit.AuditUtil;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.authz.IndicesAndAliasesResolver.ResolvedIndices;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.core.security.SecurityField.setting;
import static org.elasticsearch.xpack.core.security.support.Exceptions.authorizationError;

public class AuthorizationService {
    public static final Setting<Boolean> ANONYMOUS_AUTHORIZATION_EXCEPTION_SETTING =
        Setting.boolSetting(setting("authc.anonymous.authz_exception"), true, Property.NodeScope);
    public static final String ORIGINATING_ACTION_KEY = "_originating_action_name";
    public static final String ROLE_NAMES_KEY = "_effective_role_names";

    private static final Predicate<String> MONITOR_INDEX_PREDICATE = IndexPrivilege.MONITOR.predicate();
    private static final Predicate<String> SAME_USER_PRIVILEGE = Automatons.predicate(
        ChangePasswordAction.NAME, AuthenticateAction.NAME, HasPrivilegesAction.NAME, GetUserPrivilegesAction.NAME);

    private static final String INDEX_SUB_REQUEST_PRIMARY = IndexAction.NAME + "[p]";
    private static final String INDEX_SUB_REQUEST_REPLICA = IndexAction.NAME + "[r]";
    private static final String DELETE_SUB_REQUEST_PRIMARY = DeleteAction.NAME + "[p]";
    private static final String DELETE_SUB_REQUEST_REPLICA = DeleteAction.NAME + "[r]";
    private static final Logger logger = LogManager.getLogger(AuthorizationService.class);

    private final ClusterService clusterService;
    private final CompositeRolesStore rolesStore;
    private final AuditTrailService auditTrail;
    private final IndicesAndAliasesResolver indicesAndAliasesResolver;
    private final AuthenticationFailureHandler authcFailureHandler;
    private final ThreadContext threadContext;
    private final AnonymousUser anonymousUser;
    private final FieldPermissionsCache fieldPermissionsCache;
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
    }

    /**
     * Verifies that the given user can execute the given request (and action). If the user doesn't
     * have the appropriate privileges for this action/request, an {@link ElasticsearchSecurityException}
     * will be thrown.
     *
     * @param authentication The authentication information
     * @param action         The action
     * @param request        The request
     * @throws ElasticsearchSecurityException If the given user is no allowed to execute the given request
     */
    public void authorize(Authentication authentication, String action, TransportRequest request, Role userRole,
                          Role runAsRole) throws ElasticsearchSecurityException {
        final TransportRequest originalRequest = request;

        String auditId = AuditUtil.extractRequestId(threadContext);
        if (auditId == null) {
            // We would like to assert that there is an existing request-id, but if this is a system action, then that might not be
            // true because the request-id is generated during authentication
            if (isInternalUser(authentication.getUser()) != false) {
                auditId = AuditUtil.getOrGenerateRequestId(threadContext);
            } else {
                auditTrail.tamperedRequest(null, authentication.getUser(), action, request);
                final String message = "Attempt to authorize action [" + action + "] for [" + authentication.getUser().principal()
                    + "] without an existing request-id";
                assert false : message;
                throw new ElasticsearchSecurityException(message);
            }
        }

        if (request instanceof ConcreteShardRequest) {
            request = ((ConcreteShardRequest<?>) request).getRequest();
            assert TransportActionProxy.isProxyRequest(request) == false : "expected non-proxy request for action: " + action;
        } else {
            request = TransportActionProxy.unwrapRequest(request);
            if (TransportActionProxy.isProxyRequest(originalRequest) && TransportActionProxy.isProxyAction(action) == false) {
                throw new IllegalStateException("originalRequest is a proxy request for: [" + request + "] but action: ["
                    + action + "] isn't");
            }
        }
        // prior to doing any authorization lets set the originating action in the context only
        putTransientIfNonExisting(ORIGINATING_ACTION_KEY, action);

        // first we need to check if the user is the system. If it is, we'll just authorize the system access
        if (SystemUser.is(authentication.getUser())) {
            if (SystemUser.isAuthorized(action)) {
                putTransientIfNonExisting(AuthorizationServiceField.INDICES_PERMISSIONS_KEY, IndicesAccessControl.ALLOW_ALL);
                putTransientIfNonExisting(ROLE_NAMES_KEY, new String[] { SystemUser.ROLE_NAME });
                auditTrail.accessGranted(auditId, authentication, action, request, new String[] { SystemUser.ROLE_NAME });
                return;
            }
            throw denial(auditId, authentication, action, request, new String[] { SystemUser.ROLE_NAME });
        }

        // get the roles of the authenticated user, which may be different than the effective
        Role permission = userRole;

        // check if the request is a run as request
        final boolean isRunAs = authentication.getUser().isRunAs();
        if (isRunAs) {
            // if we are running as a user we looked up then the authentication must contain a lookedUpBy. If it doesn't then this user
            // doesn't really exist but the authc service allowed it through to avoid leaking users that exist in the system
            if (authentication.getLookedUpBy() == null) {
                throw denyRunAs(auditId, authentication, action, request, permission.names());
            } else if (permission.runAs().check(authentication.getUser().principal())) {
                auditTrail.runAsGranted(auditId, authentication, action, request, permission.names());
                permission = runAsRole;
            } else {
                throw denyRunAs(auditId, authentication, action, request, permission.names());
            }
        }
        putTransientIfNonExisting(ROLE_NAMES_KEY, permission.names());

        // first, we'll check if the action is a cluster action. If it is, we'll only check it against the cluster permissions
        if (ClusterPrivilege.ACTION_MATCHER.test(action)) {
            final ClusterPermission cluster = permission.cluster();
            if (cluster.check(action, request) || checkSameUserPermissions(action, request, authentication)) {
                putTransientIfNonExisting(AuthorizationServiceField.INDICES_PERMISSIONS_KEY, IndicesAccessControl.ALLOW_ALL);
                auditTrail.accessGranted(auditId, authentication, action, request, permission.names());
                return;
            }
            throw denial(auditId, authentication, action, request, permission.names());
        }

        // ok... this is not a cluster action, let's verify it's an indices action
        if (!IndexPrivilege.ACTION_MATCHER.test(action)) {
            throw denial(auditId, authentication, action, request, permission.names());
        }

        //composite actions are explicitly listed and will be authorized at the sub-request / shard level
        if (isCompositeAction(action)) {
            if (request instanceof CompositeIndicesRequest == false) {
                throw new IllegalStateException("Composite actions must implement " + CompositeIndicesRequest.class.getSimpleName()
                    + ", " + request.getClass().getSimpleName() + " doesn't");
            }
            // we check if the user can execute the action, without looking at indices, which will be authorized at the shard level
            if (permission.indices().check(action)) {
                auditTrail.accessGranted(auditId, authentication, action, request, permission.names());
                return;
            }
            throw denial(auditId, authentication, action, request, permission.names());
        } else if (isTranslatedToBulkAction(action)) {
            if (request instanceof CompositeIndicesRequest == false) {
                throw new IllegalStateException("Bulk translated actions must implement " + CompositeIndicesRequest.class.getSimpleName()
                    + ", " + request.getClass().getSimpleName() + " doesn't");
            }
            // we check if the user can execute the action, without looking at indices, which will be authorized at the shard level
            if (permission.indices().check(action)) {
                auditTrail.accessGranted(auditId, authentication, action, request, permission.names());
                return;
            }
            throw denial(auditId, authentication, action, request, permission.names());
        } else if (TransportActionProxy.isProxyAction(action)) {
            // we authorize proxied actions once they are "unwrapped" on the next node
            if (TransportActionProxy.isProxyRequest(originalRequest) == false) {
                throw new IllegalStateException("originalRequest is not a proxy request: [" + originalRequest + "] but action: ["
                    + action + "] is a proxy action");
            }
            if (permission.indices().check(action)) {
                auditTrail.accessGranted(auditId, authentication, action, request, permission.names());
                return;
            } else {
                // we do this here in addition to the denial below since we might run into an assertion on scroll request below if we
                // don't have permission to read cross cluster but wrap a scroll request.
                throw denial(auditId, authentication, action, request, permission.names());
            }
        }

        // some APIs are indices requests that are not actually associated with indices. For example,
        // search scroll request, is categorized under the indices context, but doesn't hold indices names
        // (in this case, the security check on the indices was done on the search request that initialized
        // the scroll. Given that scroll is implemented using a context on the node holding the shard, we
        // piggyback on it and enhance the context with the original authentication. This serves as our method
        // to validate the scroll id only stays with the same user!
        if (request instanceof IndicesRequest == false && request instanceof IndicesAliasesRequest == false) {
            //note that clear scroll shard level actions can originate from a clear scroll all, which doesn't require any
            //indices permission as it's categorized under cluster. This is why the scroll check is performed
            //even before checking if the user has any indices permission.
            if (isScrollRelatedAction(action)) {
                // if the action is a search scroll action, we first authorize that the user can execute the action for some
                // index and if they cannot, we can fail the request early before we allow the execution of the action and in
                // turn the shard actions
                if (SearchScrollAction.NAME.equals(action) && permission.indices().check(action) == false) {
                    throw denial(auditId, authentication, action, request, permission.names());
                } else {
                    // we store the request as a transient in the ThreadContext in case of a authorization failure at the shard
                    // level. If authorization fails we will audit a access_denied message and will use the request to retrieve
                    // information such as the index and the incoming address of the request
                    auditTrail.accessGranted(auditId, authentication, action, request, permission.names());
                    return;
                }
            } else {
                assert false :
                    "only scroll related requests are known indices api that don't support retrieving the indices they relate to";
                throw denial(auditId, authentication, action, request, permission.names());
            }
        }

        final boolean allowsRemoteIndices = request instanceof IndicesRequest
            && IndicesAndAliasesResolver.allowsRemoteIndices((IndicesRequest) request);

        // If this request does not allow remote indices
        // then the user must have permission to perform this action on at least 1 local index
        if (allowsRemoteIndices == false && permission.indices().check(action) == false) {
            throw denial(auditId, authentication, action, request, permission.names());
        }

        final MetaData metaData = clusterService.state().metaData();
        final AuthorizedIndices authorizedIndices = new AuthorizedIndices(authentication.getUser(), permission, action, metaData);
        final ResolvedIndices resolvedIndices = resolveIndexNames(auditId, authentication, action, request, metaData,
            authorizedIndices, permission);
        assert !resolvedIndices.isEmpty()
            : "every indices request needs to have its indices set thus the resolved indices must not be empty";

        // If this request does reference any remote indices
        // then the user must have permission to perform this action on at least 1 local index
        if (resolvedIndices.getRemote().isEmpty() && permission.indices().check(action) == false) {
            throw denial(auditId, authentication, action, request, permission.names());
        }

        //all wildcard expressions have been resolved and only the security plugin could have set '-*' here.
        //'-*' matches no indices so we allow the request to go through, which will yield an empty response
        if (resolvedIndices.isNoIndicesPlaceholder()) {
            putTransientIfNonExisting(AuthorizationServiceField.INDICES_PERMISSIONS_KEY, IndicesAccessControl.ALLOW_NO_INDICES);
            auditTrail.accessGranted(auditId, authentication, action, request, permission.names());
            return;
        }

        final Set<String> localIndices = new HashSet<>(resolvedIndices.getLocal());
        IndicesAccessControl indicesAccessControl = permission.authorize(action, localIndices, metaData, fieldPermissionsCache);
        if (!indicesAccessControl.isGranted()) {
            throw denial(auditId, authentication, action, request, permission.names());
        } else if (hasSecurityIndexAccess(indicesAccessControl)
            && MONITOR_INDEX_PREDICATE.test(action) == false
            && isSuperuser(authentication.getUser()) == false) {
            // only the XPackUser is allowed to work with this index, but we should allow indices monitoring actions through for debugging
            // purposes. These monitor requests also sometimes resolve indices concretely and then requests them
            logger.debug("user [{}] attempted to directly perform [{}] against the security index [{}]",
                authentication.getUser().principal(), action, SecurityIndexManager.SECURITY_INDEX_NAME);
            throw denial(auditId, authentication, action, request, permission.names());
        } else {
            putTransientIfNonExisting(AuthorizationServiceField.INDICES_PERMISSIONS_KEY, indicesAccessControl);
        }

        //if we are creating an index we need to authorize potential aliases created at the same time
        if (IndexPrivilege.CREATE_INDEX_MATCHER.test(action)) {
            assert request instanceof CreateIndexRequest;
            Set<Alias> aliases = ((CreateIndexRequest) request).aliases();
            if (!aliases.isEmpty()) {
                Set<String> aliasesAndIndices = Sets.newHashSet(localIndices);
                for (Alias alias : aliases) {
                    aliasesAndIndices.add(alias.name());
                }
                indicesAccessControl = permission.authorize("indices:admin/aliases", aliasesAndIndices, metaData, fieldPermissionsCache);
                if (!indicesAccessControl.isGranted()) {
                    throw denial(auditId, authentication, "indices:admin/aliases", request, permission.names());
                }
                // no need to re-add the indicesAccessControl in the context,
                // because the create index call doesn't do anything FLS or DLS
            }
        }

        if (action.equals(TransportShardBulkAction.ACTION_NAME)) {
            // is this is performing multiple actions on the index, then check each of those actions.
            assert request instanceof BulkShardRequest
                : "Action " + action + " requires " + BulkShardRequest.class + " but was " + request.getClass();

            authorizeBulkItems(auditId, authentication, (BulkShardRequest) request, permission, metaData, localIndices, authorizedIndices);
        }

        auditTrail.accessGranted(auditId, authentication, action, request, permission.names());
    }

    private boolean isInternalUser(User user) {
        return SystemUser.is(user) || XPackUser.is(user) || XPackSecurityUser.is(user);
    }

    private boolean hasSecurityIndexAccess(IndicesAccessControl indicesAccessControl) {
        for (String index : SecurityIndexManager.indexNames()) {
            final IndicesAccessControl.IndexAccessControl indexPermissions = indicesAccessControl.getIndexPermissions(index);
            if (indexPermissions != null && indexPermissions.isGranted()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Performs authorization checks on the items within a {@link BulkShardRequest}.
     * This inspects the {@link BulkItemRequest items} within the request, computes
     * an <em>implied</em> action for each item's {@link DocWriteRequest#opType()},
     * and then checks whether that action is allowed on the targeted index. Items
     * that fail this checks are {@link BulkItemRequest#abort(String, Exception)
     * aborted}, with an
     * {@link #denial(String, Authentication, String, TransportRequest, String[]) access
     * denied} exception. Because a shard level request is for exactly 1 index, and
     * there are a small number of possible item {@link DocWriteRequest.OpType
     * types}, the number of distinct authorization checks that need to be performed
     * is very small, but the results must be cached, to avoid adding a high
     * overhead to each bulk request.
     */
    private void authorizeBulkItems(String auditRequestId, Authentication authentication, BulkShardRequest request, Role permission,
                                    MetaData metaData, Set<String> indices, AuthorizedIndices authorizedIndices) {
        // Maps original-index -> expanded-index-name (expands date-math, but not aliases)
        final Map<String, String> resolvedIndexNames = new HashMap<>();
        // Maps (resolved-index , action) -> is-granted
        final Map<Tuple<String, String>, Boolean> indexActionAuthority = new HashMap<>();
        for (BulkItemRequest item : request.items()) {
            String resolvedIndex = resolvedIndexNames.computeIfAbsent(item.index(), key -> {
                final ResolvedIndices resolvedIndices = indicesAndAliasesResolver.resolveIndicesAndAliases(item.request(), metaData,
                    authorizedIndices);
                if (resolvedIndices.getRemote().size() != 0) {
                    throw illegalArgument("Bulk item should not write to remote indices, but request writes to "
                        + String.join(",", resolvedIndices.getRemote()));
                }
                if (resolvedIndices.getLocal().size() != 1) {
                    throw illegalArgument("Bulk item should write to exactly 1 index, but request writes to "
                        + String.join(",", resolvedIndices.getLocal()));
                }
                final String resolved = resolvedIndices.getLocal().get(0);
                if (indices.contains(resolved) == false) {
                    throw illegalArgument("Found bulk item that writes to index " + resolved + " but the request writes to " + indices);
                }
                return resolved;
            });
            final String itemAction = getAction(item);
            final Tuple<String, String> indexAndAction = new Tuple<>(resolvedIndex, itemAction);
            final boolean granted = indexActionAuthority.computeIfAbsent(indexAndAction, key -> {
                final IndicesAccessControl itemAccessControl = permission.authorize(itemAction, Collections.singleton(resolvedIndex),
                    metaData, fieldPermissionsCache);
                return itemAccessControl.isGranted();
            });
            if (granted == false) {
                item.abort(resolvedIndex, denial(auditRequestId, authentication, itemAction, request, permission.names()));
            }
        }
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

    private ResolvedIndices resolveIndexNames(String auditRequestId, Authentication authentication, String action, TransportRequest request,
                                              MetaData metaData, AuthorizedIndices authorizedIndices, Role permission) {
        try {
            return indicesAndAliasesResolver.resolve(request, metaData, authorizedIndices);
        } catch (Exception e) {
            auditTrail.accessDenied(auditRequestId, authentication, action, request, permission.names());
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

    private static boolean isCompositeAction(String action) {
        return action.equals(BulkAction.NAME) ||
            action.equals(MultiGetAction.NAME) ||
            action.equals(MultiTermVectorsAction.NAME) ||
            action.equals(MultiSearchAction.NAME) ||
            action.equals("indices:data/read/mpercolate") ||
            action.equals("indices:data/read/msearch/template") ||
            action.equals("indices:data/read/search/template") ||
            action.equals("indices:data/write/reindex") ||
            action.equals("indices:data/read/sql") ||
            action.equals("indices:data/read/sql/translate");
    }

    private static boolean isTranslatedToBulkAction(String action) {
        return action.equals(IndexAction.NAME) ||
            action.equals(DeleteAction.NAME) ||
            action.equals(INDEX_SUB_REQUEST_PRIMARY) ||
            action.equals(INDEX_SUB_REQUEST_REPLICA) ||
            action.equals(DELETE_SUB_REQUEST_PRIMARY) ||
            action.equals(DELETE_SUB_REQUEST_REPLICA);
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

    static boolean checkSameUserPermissions(String action, TransportRequest request, Authentication authentication) {
        final boolean actionAllowed = SAME_USER_PRIVILEGE.test(action);
        if (actionAllowed) {
            if (request instanceof UserRequest == false) {
                assert false : "right now only a user request should be allowed";
                return false;
            }
            UserRequest userRequest = (UserRequest) request;
            String[] usernames = userRequest.usernames();
            if (usernames == null || usernames.length != 1 || usernames[0] == null) {
                assert false : "this role should only be used for actions to apply to a single user";
                return false;
            }
            final String username = usernames[0];
            final boolean sameUsername = authentication.getUser().principal().equals(username);
            if (sameUsername && ChangePasswordAction.NAME.equals(action)) {
                return checkChangePasswordAction(authentication);
            }

            assert AuthenticateAction.NAME.equals(action) || HasPrivilegesAction.NAME.equals(action)
                || GetUserPrivilegesAction.NAME.equals(action) || sameUsername == false
                : "Action '" + action + "' should not be possible when sameUsername=" + sameUsername;
            return sameUsername;
        }
        return false;
    }

    private static boolean checkChangePasswordAction(Authentication authentication) {
        // we need to verify that this user was authenticated by or looked up by a realm type that support password changes
        // otherwise we open ourselves up to issues where a user in a different realm could be created with the same username
        // and do malicious things
        final boolean isRunAs = authentication.getUser().isRunAs();
        final String realmType;
        if (isRunAs) {
            realmType = authentication.getLookedUpBy().getType();
        } else {
            realmType = authentication.getAuthenticatedBy().getType();
        }

        assert realmType != null;
        // ensure the user was authenticated by a realm that we can change a password for. The native realm is an internal realm and
        // right now only one can exist in the realm configuration - if this changes we should update this check
        return ReservedRealm.TYPE.equals(realmType) || NativeRealmSettings.TYPE.equals(realmType);
    }

    ElasticsearchSecurityException denial(String auditRequestId, Authentication authentication, String action, TransportRequest request,
                                          String[] roleNames) {
        auditTrail.accessDenied(auditRequestId, authentication, action, request, roleNames);
        return denialException(authentication, action);
    }

    private ElasticsearchSecurityException denyRunAs(String auditRequestId, Authentication authentication, String action,
                                                     TransportRequest request, String[] roleNames) {
        auditTrail.runAsDenied(auditRequestId, authentication, action, request, roleNames);
        return denialException(authentication, action);
    }

    private ElasticsearchSecurityException denialException(Authentication authentication, String action) {
        final User authUser = authentication.getUser().authenticatedUser();
        // Special case for anonymous user
        if (isAnonymousEnabled && anonymousUser.equals(authUser)) {
            if (anonymousAuthzExceptionEnabled == false) {
                throw authcFailureHandler.authenticationRequired(action, threadContext);
            }
        }
        // check for run as
        if (authentication.getUser().isRunAs()) {
            logger.debug("action [{}] is unauthorized for user [{}] run as [{}]", action, authUser.principal(),
                authentication.getUser().principal());
            return authorizationError("action [{}] is unauthorized for user [{}] run as [{}]", action, authUser.principal(),
                authentication.getUser().principal());
        }
        logger.debug("action [{}] is unauthorized for user [{}]", action, authUser.principal());
        return authorizationError("action [{}] is unauthorized for user [{}]", action, authUser.principal());
    }

    static boolean isSuperuser(User user) {
        return Arrays.stream(user.roles())
            .anyMatch(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName()::equals);
    }

    public static void addSettings(List<Setting<?>> settings) {
        settings.add(ANONYMOUS_AUTHORIZATION_EXCEPTION_SETTING);
    }
}

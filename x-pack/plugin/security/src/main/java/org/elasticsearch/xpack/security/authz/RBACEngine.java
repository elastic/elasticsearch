/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.ElasticsearchRoleRestrictionException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.AliasesRequest;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.ClosePointInTimeAction;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.termvectors.MultiTermVectorsAction;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CachedSupplier;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.async.DeleteAsyncResultAction;
import org.elasticsearch.xpack.core.eql.EqlAsyncActionNames;
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchAction;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordAction;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesResponse;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.UserRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.AuthenticationType;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.IndicesAndAliasesResolverField;
import org.elasticsearch.xpack.core.security.authz.ResolvedIndices;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition;
import org.elasticsearch.xpack.core.security.authz.permission.IndicesPermission;
import org.elasticsearch.xpack.core.security.authz.permission.IndicesPermission.IsResourceAuthorizedPredicate;
import org.elasticsearch.xpack.core.security.authz.permission.RemoteIndicesPermission;
import org.elasticsearch.xpack.core.security.authz.permission.ResourcePrivileges;
import org.elasticsearch.xpack.core.security.authz.permission.ResourcePrivilegesMap;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.permission.SimpleRole;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.NamedClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.Privilege;
import org.elasticsearch.xpack.core.security.support.StringMatcher;
import org.elasticsearch.xpack.core.sql.SqlAsyncActionNames;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.common.Strings.arrayToCommaDelimitedString;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.security.authc.Authentication.getAuthenticationFromCrossClusterAccessMetadata;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.PRINCIPAL_ROLES_FIELD_NAME;

public class RBACEngine implements AuthorizationEngine {

    private static final Predicate<String> SAME_USER_PRIVILEGE = StringMatcher.of(
        ChangePasswordAction.NAME,
        AuthenticateAction.NAME,
        HasPrivilegesAction.NAME,
        GetUserPrivilegesAction.NAME,
        GetApiKeyAction.NAME
    );
    private static final String INDEX_SUB_REQUEST_PRIMARY = IndexAction.NAME + "[p]";
    private static final String INDEX_SUB_REQUEST_REPLICA = IndexAction.NAME + "[r]";
    private static final String DELETE_SUB_REQUEST_PRIMARY = DeleteAction.NAME + "[p]";
    private static final String DELETE_SUB_REQUEST_REPLICA = DeleteAction.NAME + "[r]";

    private static final Logger logger = LogManager.getLogger(RBACEngine.class);
    private final Settings settings;
    private final CompositeRolesStore rolesStore;
    private final FieldPermissionsCache fieldPermissionsCache;
    private final LoadAuthorizedIndicesTimeChecker.Factory authzIndicesTimerFactory;

    public RBACEngine(
        Settings settings,
        CompositeRolesStore rolesStore,
        FieldPermissionsCache fieldPermissionsCache,
        LoadAuthorizedIndicesTimeChecker.Factory authzIndicesTimerFactory
    ) {
        this.settings = settings;
        this.rolesStore = rolesStore;
        this.fieldPermissionsCache = fieldPermissionsCache;
        this.authzIndicesTimerFactory = authzIndicesTimerFactory;
    }

    @Override
    public void resolveAuthorizationInfo(RequestInfo requestInfo, ActionListener<AuthorizationInfo> listener) {
        final Authentication authentication = requestInfo.getAuthentication();
        rolesStore.getRoles(authentication, listener.delegateFailureAndWrap((l, roleTuple) -> {
            if (roleTuple.v1() == Role.EMPTY_RESTRICTED_BY_WORKFLOW || roleTuple.v2() == Role.EMPTY_RESTRICTED_BY_WORKFLOW) {
                l.onFailure(new ElasticsearchRoleRestrictionException("access restricted by workflow"));
            } else {
                l.onResponse(new RBACAuthorizationInfo(roleTuple.v1(), roleTuple.v2()));
            }
        }));
    }

    @Override
    public void resolveAuthorizationInfo(Subject subject, ActionListener<AuthorizationInfo> listener) {
        // TODO: When we expand support of workflows restriction to broader use cases (other than API keys for Search Application),
        // we should revisit this method and handle workflows in a consistent way.
        rolesStore.getRole(subject, listener.map(role -> new RBACAuthorizationInfo(role, role)));
    }

    @Override
    public void authorizeRunAs(RequestInfo requestInfo, AuthorizationInfo authorizationInfo, ActionListener<AuthorizationResult> listener) {
        if (authorizationInfo instanceof RBACAuthorizationInfo) {
            final Role role = ((RBACAuthorizationInfo) authorizationInfo).getAuthenticatedUserAuthorizationInfo().getRole();
            listener.onResponse(
                new AuthorizationResult(role.checkRunAs(requestInfo.getAuthentication().getEffectiveSubject().getUser().principal()))
            );
        } else {
            listener.onFailure(
                new IllegalArgumentException("unsupported authorization info:" + authorizationInfo.getClass().getSimpleName())
            );
        }
    }

    @Override
    public void authorizeClusterAction(
        RequestInfo requestInfo,
        AuthorizationInfo authorizationInfo,
        ActionListener<AuthorizationResult> listener
    ) {
        if (authorizationInfo instanceof RBACAuthorizationInfo) {
            final Role role = ((RBACAuthorizationInfo) authorizationInfo).getRole();
            if (role.checkClusterAction(requestInfo.getAction(), requestInfo.getRequest(), requestInfo.getAuthentication())) {
                listener.onResponse(AuthorizationResult.granted());
            } else if (checkSameUserPermissions(requestInfo.getAction(), requestInfo.getRequest(), requestInfo.getAuthentication())) {
                listener.onResponse(AuthorizationResult.granted());
            } else {
                listener.onResponse(AuthorizationResult.deny());
            }
        } else {
            listener.onFailure(
                new IllegalArgumentException("unsupported authorization info:" + authorizationInfo.getClass().getSimpleName())
            );
        }
    }

    // pkg private for testing
    static boolean checkSameUserPermissions(String action, TransportRequest request, Authentication authentication) {
        final boolean actionAllowed = SAME_USER_PRIVILEGE.test(action);
        if (actionAllowed) {
            if (request instanceof AuthenticateRequest) {
                return true;
            } else if (request instanceof UserRequest userRequest) {
                String[] usernames = userRequest.usernames();
                if (usernames == null || usernames.length != 1 || usernames[0] == null) {
                    assert false : "this role should only be used for actions to apply to a single user";
                    return false;
                }
                final String username = usernames[0];
                // Cross cluster access user can perform has privilege check
                if (authentication.isCrossClusterAccess() && HasPrivilegesAction.NAME.equals(action)) {
                    assert request instanceof HasPrivilegesRequest;
                    return getAuthenticationFromCrossClusterAccessMetadata(authentication).getEffectiveSubject()
                        .getUser()
                        .principal()
                        .equals(username);
                }

                final boolean sameUsername = authentication.getEffectiveSubject().getUser().principal().equals(username);
                if (sameUsername && ChangePasswordAction.NAME.equals(action)) {
                    return checkChangePasswordAction(authentication);
                }

                assert AuthenticateAction.NAME.equals(action)
                    || HasPrivilegesAction.NAME.equals(action)
                    || GetUserPrivilegesAction.NAME.equals(action)
                    || sameUsername == false : "Action '" + action + "' should not be possible when sameUsername=" + sameUsername;
                return sameUsername;
            } else if (request instanceof GetApiKeyRequest getApiKeyRequest) {
                if (authentication.isApiKey()) {
                    // if the authentication is an API key then the request must also contain same API key id
                    String authenticatedApiKeyId = (String) authentication.getAuthenticatingSubject()
                        .getMetadata()
                        .get(AuthenticationField.API_KEY_ID_KEY);
                    if (Strings.hasText(getApiKeyRequest.getApiKeyId())) {
                        // An API key requires manage_api_key privilege or higher to view any limited-by role descriptors
                        return getApiKeyRequest.getApiKeyId().equals(authenticatedApiKeyId) && false == getApiKeyRequest.withLimitedBy();
                    } else {
                        return false;
                    }
                }
            } else {
                assert false : "right now only a user request or get api key request should be allowed";
                return false;
            }
        }
        return false;
    }

    private static boolean shouldAuthorizeIndexActionNameOnly(String action, TransportRequest request) {
        switch (action) {
            case BulkAction.NAME:
            case IndexAction.NAME:
            case DeleteAction.NAME:
            case INDEX_SUB_REQUEST_PRIMARY:
            case INDEX_SUB_REQUEST_REPLICA:
            case DELETE_SUB_REQUEST_PRIMARY:
            case DELETE_SUB_REQUEST_REPLICA:
            case MultiGetAction.NAME:
            case MultiTermVectorsAction.NAME:
            case MultiSearchAction.NAME:
            case "indices:data/read/mpercolate":
            case "indices:data/read/msearch/template":
            case "indices:data/read/search/template":
            case "indices:data/write/reindex":
            case "indices:data/read/sql":
            case "indices:data/read/sql/translate":
                if (request instanceof BulkShardRequest) {
                    return false;
                }
                if (request instanceof CompositeIndicesRequest == false) {
                    throw new IllegalStateException(
                        "Composite and bulk actions must implement "
                            + CompositeIndicesRequest.class.getSimpleName()
                            + ", "
                            + request.getClass().getSimpleName()
                            + " doesn't. Action "
                            + action
                    );
                }
                return true;
            default:
                return false;
        }
    }

    @Override
    public void authorizeIndexAction(
        RequestInfo requestInfo,
        AuthorizationInfo authorizationInfo,
        AsyncSupplier<ResolvedIndices> indicesAsyncSupplier,
        Map<String, IndexAbstraction> aliasOrIndexLookup,
        ActionListener<IndexAuthorizationResult> listener
    ) {
        final String action = requestInfo.getAction();
        final TransportRequest request = requestInfo.getRequest();
        final Role role;
        try {
            role = ensureRBAC(authorizationInfo).getRole();
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        if (TransportActionProxy.isProxyAction(action) || shouldAuthorizeIndexActionNameOnly(action, request)) {
            // we've already validated that the request is a proxy request so we can skip that but we still
            // need to validate that the action is allowed and then move on
            listener.onResponse(role.checkIndicesAction(action) ? IndexAuthorizationResult.EMPTY : IndexAuthorizationResult.DENIED);
        } else if (request instanceof IndicesRequest == false) {
            if (isScrollRelatedAction(action)) {
                // scroll is special
                // some APIs are indices requests that are not actually associated with indices. For example,
                // search scroll request, is categorized under the indices context, but doesn't hold indices names
                // (in this case, the security check on the indices was done on the search request that initialized
                // the scroll. Given that scroll is implemented using a context on the node holding the shard, we
                // piggyback on it and enhance the context with the original authentication. This serves as our method
                // to validate the scroll id only stays with the same user!
                // note that clear scroll shard level actions can originate from a clear scroll all, which doesn't require any
                // indices permission as it's categorized under cluster. This is why the scroll check is performed
                // even before checking if the user has any indices permission.

                // if the action is a search scroll action, we first authorize that the user can execute the action for some
                // index and if they cannot, we can fail the request early before we allow the execution of the action and in
                // turn the shard actions
                if (SearchScrollAction.NAME.equals(action)) {
                    ActionRunnable.supply(listener.delegateFailureAndWrap((l, parsedScrollId) -> {
                        if (parsedScrollId.hasLocalIndices()) {
                            l.onResponse(
                                role.checkIndicesAction(action) ? IndexAuthorizationResult.EMPTY : IndexAuthorizationResult.DENIED
                            );
                        } else {
                            l.onResponse(IndexAuthorizationResult.EMPTY);
                        }
                    }), ((SearchScrollRequest) request)::parseScrollId).run();
                } else {
                    // RBACEngine simply authorizes scroll related actions without filling in any DLS/FLS permissions.
                    // Scroll related actions have special security logic, where the security context of the initial search
                    // request is attached to the scroll context upon creation in {@code SecuritySearchOperationListener#onNewScrollContext}
                    // and it is then verified, before every use of the scroll, in
                    // {@code SecuritySearchOperationListener#validateSearchContext}.
                    // The DLS/FLS permissions are used inside the {@code DirectoryReader} that {@code SecurityIndexReaderWrapper}
                    // built while handling the initial search request. In addition, for consistency, the DLS/FLS permissions from
                    // the originating search request are attached to the thread context upon validating the scroll.
                    listener.onResponse(IndexAuthorizationResult.EMPTY);
                }
            } else if (isAsyncRelatedAction(action)) {
                if (SubmitAsyncSearchAction.NAME.equals(action)) {
                    // authorize submit async search but don't fill in the DLS/FLS permissions
                    // the `null` IndicesAccessControl parameter indicates that this action has *not* determined
                    // which DLS/FLS controls should be applied to this action
                    listener.onResponse(IndexAuthorizationResult.EMPTY);
                } else {
                    // async-search actions other than submit have a custom security layer that checks if the current user is
                    // the same as the user that submitted the original request so no additional checks are needed here.
                    listener.onResponse(IndexAuthorizationResult.ALLOW_NO_INDICES);
                }
            } else if (action.equals(ClosePointInTimeAction.NAME)) {
                listener.onResponse(IndexAuthorizationResult.ALLOW_NO_INDICES);
            } else {
                assert false
                    : "only scroll and async-search related requests are known indices api that don't "
                        + "support retrieving the indices they relate to";
                listener.onFailure(
                    new IllegalStateException(
                        "only scroll and async-search related requests are known indices "
                            + "api that don't support retrieving the indices they relate to"
                    )
                );
            }
        } else if (isChildActionAuthorizedByParentOnLocalNode(requestInfo, authorizationInfo)) {
            listener.onResponse(new IndexAuthorizationResult(requestInfo.getOriginatingAuthorizationContext().getIndicesAccessControl()));
        } else if (PreAuthorizationUtils.shouldPreAuthorizeChildByParentAction(requestInfo, authorizationInfo)) {
            // We only pre-authorize child actions if DLS/FLS is not configured,
            // hence we can allow here access for all requested indices.
            listener.onResponse(new IndexAuthorizationResult(IndicesAccessControl.allowAll()));
        } else if (allowsRemoteIndices(request) || role.checkIndicesAction(action)) {
            indicesAsyncSupplier.getAsync(listener.delegateFailureAndWrap((delegateListener, resolvedIndices) -> {
                assert resolvedIndices.isEmpty() == false
                    : "every indices request needs to have its indices set thus the resolved indices must not be empty";
                // all wildcard expressions have been resolved and only the security plugin could have set '-*' here.
                // '-*' matches no indices so we allow the request to go through, which will yield an empty response
                if (resolvedIndices.isNoIndicesPlaceholder()) {
                    if (allowsRemoteIndices(request) && role.checkIndicesAction(action) == false) {
                        delegateListener.onResponse(IndexAuthorizationResult.DENIED);
                    } else {
                        delegateListener.onResponse(IndexAuthorizationResult.ALLOW_NO_INDICES);
                    }
                } else {
                    assert resolvedIndices.getLocal().stream().noneMatch(Regex::isSimpleMatchPattern)
                        || ((IndicesRequest) request).indicesOptions().expandWildcardExpressions() == false
                        || (request instanceof AliasesRequest aliasesRequest && aliasesRequest.expandAliasesWildcards() == false)
                        || (request instanceof IndicesAliasesRequest indicesAliasesRequest
                            && false == indicesAliasesRequest.getAliasActions()
                                .stream()
                                .allMatch(IndicesAliasesRequest.AliasActions::expandAliasesWildcards))
                        : "expanded wildcards for local indices OR the request should not expand wildcards at all";
                    delegateListener.onResponse(buildIndicesAccessControl(action, role, resolvedIndices, aliasOrIndexLookup));
                }
            }));
        } else {
            listener.onResponse(IndexAuthorizationResult.DENIED);
        }
    }

    private static boolean allowsRemoteIndices(TransportRequest transportRequest) {
        return transportRequest instanceof IndicesRequest.Replaceable replaceable && replaceable.allowsRemoteIndices();
    }

    private static boolean isChildActionAuthorizedByParentOnLocalNode(RequestInfo requestInfo, AuthorizationInfo authorizationInfo) {
        final AuthorizationContext parent = requestInfo.getOriginatingAuthorizationContext();
        if (parent == null) {
            return false;
        }

        final IndicesAccessControl indicesAccessControl = parent.getIndicesAccessControl();
        if (indicesAccessControl == null) {
            // This can happen for is the parent request was authorized by index name only - e.g. bulk request
            // A missing IAC is not an error, but it means we can't safely tie authz of the child action to the parent authz
            return false;
        }

        if (requestInfo.getAction().startsWith(parent.getAction()) == false) {
            // Parent action is not a true parent
            // We want to treat shard level actions (those that append '[s]' and/or '[p]' & '[r]')
            // or similar (e.g. search phases) as children, but not every action that is triggered
            // within another action should be authorized this way
            return false;
        }

        if (authorizationInfo.equals(parent.getAuthorizationInfo()) == false) {
            // Authorization changed
            // This should only happen if the user's list of roles changed between requests
            // Take the safe option and perform full authorization
            return false;
        }

        final IndicesRequest indicesRequest;
        if (requestInfo.getRequest() instanceof IndicesRequest) {
            indicesRequest = (IndicesRequest) requestInfo.getRequest();
        } else {
            // Can only handle indices request here
            return false;
        }

        final String[] indices = indicesRequest.indices();
        if (indices == null || indices.length == 0) {
            // No indices to check
            return false;
        }

        if (Arrays.equals(IndicesAndAliasesResolverField.NO_INDICES_OR_ALIASES_ARRAY, indices)) {
            // Special placeholder for no indices.
            // We probably can short circuit this, but it's safer not to and just fall through to the regular authorization
            return false;
        }

        assert Arrays.stream(indices).noneMatch(Regex::isSimpleMatchPattern)
            || indicesRequest.indicesOptions().expandWildcardExpressions() == false
            || (indicesRequest instanceof AliasesRequest aliasesRequest && aliasesRequest.expandAliasesWildcards() == false)
            || (indicesRequest instanceof IndicesAliasesRequest indicesAliasesRequest
                && false == indicesAliasesRequest.getAliasActions()
                    .stream()
                    .allMatch(IndicesAliasesRequest.AliasActions::expandAliasesWildcards))
            : "child request with action ["
                + requestInfo.getAction()
                + "] contains unexpanded wildcards "
                + Arrays.stream(indices).filter(Regex::isSimpleMatchPattern).toList();

        // Check if the parent context has already successfully authorized access to the child's indices
        return Arrays.stream(indices).allMatch(indicesAccessControl::hasIndexPermissions);
    }

    @Override
    public void loadAuthorizedIndices(
        RequestInfo requestInfo,
        AuthorizationInfo authorizationInfo,
        Map<String, IndexAbstraction> indicesLookup,
        ActionListener<AuthorizationEngine.AuthorizedIndices> listener
    ) {
        if (authorizationInfo instanceof RBACAuthorizationInfo) {
            final Role role = ((RBACAuthorizationInfo) authorizationInfo).getRole();
            listener.onResponse(
                resolveAuthorizedIndicesFromRole(role, requestInfo, indicesLookup, () -> authzIndicesTimerFactory.newTimer(requestInfo))
            );
        } else {
            listener.onFailure(
                new IllegalArgumentException("unsupported authorization info:" + authorizationInfo.getClass().getSimpleName())
            );
        }
    }

    @Override
    public void validateIndexPermissionsAreSubset(
        RequestInfo requestInfo,
        AuthorizationInfo authorizationInfo,
        Map<String, List<String>> indexNameToNewNames,
        ActionListener<AuthorizationResult> listener
    ) {
        if (authorizationInfo instanceof RBACAuthorizationInfo) {
            final Role role = ((RBACAuthorizationInfo) authorizationInfo).getRole();
            Map<String, Automaton> permissionMap = new HashMap<>();
            for (Entry<String, List<String>> entry : indexNameToNewNames.entrySet()) {
                Automaton existingPermissions = permissionMap.computeIfAbsent(entry.getKey(), role::allowedActionsMatcher);
                for (String alias : entry.getValue()) {
                    Automaton newNamePermissions = permissionMap.computeIfAbsent(alias, role::allowedActionsMatcher);
                    if (Operations.subsetOf(newNamePermissions, existingPermissions) == false) {
                        listener.onResponse(AuthorizationResult.deny());
                        return;
                    }
                }
            }
            listener.onResponse(AuthorizationResult.granted());
        } else {
            listener.onFailure(
                new IllegalArgumentException("unsupported authorization info:" + authorizationInfo.getClass().getSimpleName())
            );
        }
    }

    @Override
    public void checkPrivileges(
        AuthorizationInfo authorizationInfo,
        PrivilegesToCheck privilegesToCheck,
        Collection<ApplicationPrivilegeDescriptor> applicationPrivileges,
        ActionListener<PrivilegesCheckResult> originalListener
    ) {
        if (authorizationInfo instanceof RBACAuthorizationInfo == false) {
            originalListener.onFailure(
                new IllegalArgumentException("unsupported authorization info:" + authorizationInfo.getClass().getSimpleName())
            );
            return;
        }
        final Role userRole = ((RBACAuthorizationInfo) authorizationInfo).getRole();
        logger.trace(
            () -> format(
                "Check whether role [%s] has privileges [%s]",
                Strings.arrayToCommaDelimitedString(userRole.names()),
                privilegesToCheck
            )
        );

        final ActionListener<PrivilegesCheckResult> listener;
        if (userRole instanceof SimpleRole simpleRole) {
            final PrivilegesCheckResult result = simpleRole.checkPrivilegesWithCache(privilegesToCheck);
            if (result != null) {
                logger.debug(
                    () -> format(
                        "role [%s] has privileges check result in cache for check: [%s]",
                        arrayToCommaDelimitedString(userRole.names()),
                        privilegesToCheck
                    )
                );
                originalListener.onResponse(result);
                return;
            }
            listener = originalListener.delegateFailure((delegateListener, privilegesCheckResult) -> {
                try {
                    simpleRole.cacheHasPrivileges(settings, privilegesToCheck, privilegesCheckResult);
                } catch (Exception e) {
                    logger.error("Failed to cache check result for [{}]", privilegesToCheck);
                    delegateListener.onFailure(e);
                    return;
                }
                delegateListener.onResponse(privilegesCheckResult);
            });
        } else {
            // caching of check result unsupported
            listener = originalListener;
        }

        boolean allMatch = true;

        final Map<String, Boolean> clusterPrivilegesCheckResults = new HashMap<>();
        for (String checkAction : privilegesToCheck.cluster()) {
            boolean privilegeGranted = userRole.grants(ClusterPrivilegeResolver.resolve(checkAction));
            allMatch = allMatch && privilegeGranted;
            if (privilegesToCheck.runDetailedCheck()) {
                clusterPrivilegesCheckResults.put(checkAction, privilegeGranted);
            } else if (false == allMatch) {
                listener.onResponse(PrivilegesCheckResult.SOME_CHECKS_FAILURE_NO_DETAILS);
                return;
            }
        }

        final ResourcePrivilegesMap.Builder combineIndicesResourcePrivileges = privilegesToCheck.runDetailedCheck()
            ? ResourcePrivilegesMap.builder()
            : null;
        for (RoleDescriptor.IndicesPrivileges check : privilegesToCheck.index()) {
            boolean privilegesGranted = userRole.checkIndicesPrivileges(
                Sets.newHashSet(check.getIndices()),
                check.allowRestrictedIndices(),
                Sets.newHashSet(check.getPrivileges()),
                combineIndicesResourcePrivileges
            );
            allMatch = allMatch && privilegesGranted;
            if (false == privilegesToCheck.runDetailedCheck() && false == allMatch) {
                assert combineIndicesResourcePrivileges == null;
                listener.onResponse(PrivilegesCheckResult.SOME_CHECKS_FAILURE_NO_DETAILS);
                return;
            }
        }

        final Map<String, Collection<ResourcePrivileges>> privilegesByApplication = new HashMap<>();

        final Set<String> applicationNames = Arrays.stream(privilegesToCheck.application())
            .map(RoleDescriptor.ApplicationResourcePrivileges::getApplication)
            .collect(Collectors.toSet());
        for (String applicationName : applicationNames) {
            logger.debug(() -> format("Checking privileges for application [%s]", applicationName));
            final ResourcePrivilegesMap.Builder resourcePrivilegesMapBuilder = privilegesToCheck.runDetailedCheck()
                ? ResourcePrivilegesMap.builder()
                : null;
            for (RoleDescriptor.ApplicationResourcePrivileges p : privilegesToCheck.application()) {
                if (applicationName.equals(p.getApplication())) {
                    boolean privilegesGranted = userRole.checkApplicationResourcePrivileges(
                        applicationName,
                        Sets.newHashSet(p.getResources()),
                        Sets.newHashSet(p.getPrivileges()),
                        applicationPrivileges,
                        resourcePrivilegesMapBuilder
                    );
                    allMatch = allMatch && privilegesGranted;
                    if (false == privilegesToCheck.runDetailedCheck() && false == allMatch) {
                        listener.onResponse(PrivilegesCheckResult.SOME_CHECKS_FAILURE_NO_DETAILS);
                        return;
                    }
                }
            }
            if (resourcePrivilegesMapBuilder != null) {
                privilegesByApplication.put(
                    applicationName,
                    resourcePrivilegesMapBuilder.build().getResourceToResourcePrivileges().values()
                );
            }
        }

        if (privilegesToCheck.runDetailedCheck()) {
            assert combineIndicesResourcePrivileges != null;
            listener.onResponse(
                new PrivilegesCheckResult(
                    allMatch,
                    new PrivilegesCheckResult.Details(
                        clusterPrivilegesCheckResults,
                        combineIndicesResourcePrivileges.build().getResourceToResourcePrivileges(),
                        privilegesByApplication
                    )
                )
            );
        } else {
            assert allMatch;
            listener.onResponse(PrivilegesCheckResult.ALL_CHECKS_SUCCESS_NO_DETAILS);
        }
    }

    @Override
    public void getUserPrivileges(AuthorizationInfo authorizationInfo, ActionListener<GetUserPrivilegesResponse> listener) {
        if (authorizationInfo instanceof RBACAuthorizationInfo == false) {
            listener.onFailure(
                new IllegalArgumentException("unsupported authorization info:" + authorizationInfo.getClass().getSimpleName())
            );
        } else {
            final Role role = ((RBACAuthorizationInfo) authorizationInfo).getRole();
            final GetUserPrivilegesResponse getUserPrivilegesResponse;
            try {
                getUserPrivilegesResponse = buildUserPrivilegesResponseObject(role);
            } catch (UnsupportedOperationException e) {
                listener.onFailure(
                    new IllegalArgumentException(
                        "Cannot retrieve privileges for API keys with assigned role descriptors. "
                            + "Please use the Get API key information API https://ela.st/es-api-get-api-key",
                        e
                    )
                );
                return;
            }
            listener.onResponse(getUserPrivilegesResponse);
        }
    }

    @Override
    public void getRoleDescriptorsIntersectionForRemoteCluster(
        final String remoteClusterAlias,
        final AuthorizationInfo authorizationInfo,
        final ActionListener<RoleDescriptorsIntersection> listener
    ) {
        if (authorizationInfo instanceof RBACAuthorizationInfo rbacAuthzInfo) {
            final Role role = rbacAuthzInfo.getRole();
            listener.onResponse(role.getRoleDescriptorsIntersectionForRemoteCluster(remoteClusterAlias));
        } else {
            listener.onFailure(
                new IllegalArgumentException("unsupported authorization info: " + authorizationInfo.getClass().getSimpleName())
            );
        }
    }

    static GetUserPrivilegesResponse buildUserPrivilegesResponseObject(Role userRole) {
        logger.trace(() -> "List privileges for role [" + arrayToCommaDelimitedString(userRole.names()) + "]");

        // We use sorted sets for Strings because they will typically be small, and having a predictable order allows for simpler testing
        final Set<String> cluster = new TreeSet<>();
        // But we don't have a meaningful ordering for objects like ConfigurableClusterPrivilege, so the tests work with "random" ordering
        final Set<ConfigurableClusterPrivilege> conditionalCluster = new HashSet<>();
        for (ClusterPrivilege privilege : userRole.cluster().privileges()) {
            if (privilege instanceof NamedClusterPrivilege) {
                cluster.add(((NamedClusterPrivilege) privilege).name());
            } else if (privilege instanceof ConfigurableClusterPrivilege) {
                conditionalCluster.add((ConfigurableClusterPrivilege) privilege);
            } else {
                throw new IllegalArgumentException(
                    "found unsupported cluster privilege : "
                        + privilege
                        + ((privilege != null) ? " of type " + privilege.getClass().getSimpleName() : "")
                );
            }
        }

        final Set<GetUserPrivilegesResponse.Indices> indices = new LinkedHashSet<>();
        for (IndicesPermission.Group group : userRole.indices().groups()) {
            indices.add(toIndices(group));
        }

        final Set<GetUserPrivilegesResponse.RemoteIndices> remoteIndices = new LinkedHashSet<>();
        for (RemoteIndicesPermission.RemoteIndicesGroup remoteIndicesGroup : userRole.remoteIndices().remoteIndicesGroups()) {
            for (IndicesPermission.Group group : remoteIndicesGroup.indicesPermissionGroups()) {
                remoteIndices.add(new GetUserPrivilegesResponse.RemoteIndices(toIndices(group), remoteIndicesGroup.remoteClusterAliases()));
            }
        }

        final Set<RoleDescriptor.ApplicationResourcePrivileges> application = new LinkedHashSet<>();
        for (String applicationName : userRole.application().getApplicationNames()) {
            for (ApplicationPrivilege privilege : userRole.application().getPrivileges(applicationName)) {
                final Set<String> resources = userRole.application().getResourcePatterns(privilege);
                if (resources.isEmpty()) {
                    logger.trace("No resources defined in application privilege {}", privilege);
                } else {
                    application.add(
                        RoleDescriptor.ApplicationResourcePrivileges.builder()
                            .application(applicationName)
                            .privileges(privilege.name())
                            .resources(resources)
                            .build()
                    );
                }
            }
        }

        final Privilege runAsPrivilege = userRole.runAs().getPrivilege();
        final Set<String> runAs;
        if (Operations.isEmpty(runAsPrivilege.getAutomaton())) {
            runAs = Collections.emptySet();
        } else {
            runAs = runAsPrivilege.name();
        }

        return new GetUserPrivilegesResponse(cluster, conditionalCluster, indices, application, runAs, remoteIndices);
    }

    private static GetUserPrivilegesResponse.Indices toIndices(final IndicesPermission.Group group) {
        final Set<BytesReference> queries = group.getQuery() == null ? Collections.emptySet() : group.getQuery();
        final Set<FieldPermissionsDefinition.FieldGrantExcludeGroup> fieldSecurity = getFieldGrantExcludeGroups(group);
        return new GetUserPrivilegesResponse.Indices(
            Arrays.asList(group.indices()),
            group.privilege().name(),
            fieldSecurity,
            queries,
            group.allowRestrictedIndices()
        );
    }

    private static Set<FieldPermissionsDefinition.FieldGrantExcludeGroup> getFieldGrantExcludeGroups(IndicesPermission.Group group) {
        if (group.getFieldPermissions().hasFieldLevelSecurity()) {
            final List<FieldPermissionsDefinition> fieldPermissionsDefinitions = group.getFieldPermissions()
                .getFieldPermissionsDefinitions();
            assert fieldPermissionsDefinitions.size() == 1
                : "limited-by field must not exist since we do not support reporting user privileges for limited roles";
            final FieldPermissionsDefinition definition = fieldPermissionsDefinitions.get(0);
            return definition.getFieldGrantExcludeGroups();
        } else {
            return Collections.emptySet();
        }
    }

    static AuthorizedIndices resolveAuthorizedIndicesFromRole(
        Role role,
        RequestInfo requestInfo,
        Map<String, IndexAbstraction> lookup,
        Supplier<Consumer<Collection<String>>> timerSupplier
    ) {
        IsResourceAuthorizedPredicate predicate = role.allowedIndicesMatcher(requestInfo.getAction());

        // do not include data streams for actions that do not operate on data streams
        TransportRequest request = requestInfo.getRequest();
        final boolean includeDataStreams = (request instanceof IndicesRequest) && ((IndicesRequest) request).includeDataStreams();

        return new AuthorizedIndices(() -> {
            Consumer<Collection<String>> timeChecker = timerSupplier.get();
            Set<String> indicesAndAliases = new HashSet<>();
            // TODO: can this be done smarter? I think there are usually more indices/aliases in the cluster then indices defined a roles?
            if (includeDataStreams) {
                for (IndexAbstraction indexAbstraction : lookup.values()) {
                    if (predicate.test(indexAbstraction)) {
                        indicesAndAliases.add(indexAbstraction.getName());
                        if (indexAbstraction.getType() == IndexAbstraction.Type.DATA_STREAM) {
                            // add data stream and its backing indices for any authorized data streams
                            for (Index index : indexAbstraction.getIndices()) {
                                indicesAndAliases.add(index.getName());
                            }
                        }
                    }
                }
            } else {
                for (IndexAbstraction indexAbstraction : lookup.values()) {
                    if (indexAbstraction.getType() != IndexAbstraction.Type.DATA_STREAM && predicate.test(indexAbstraction)) {
                        indicesAndAliases.add(indexAbstraction.getName());
                    }
                }
            }
            timeChecker.accept(indicesAndAliases);
            return indicesAndAliases;
        }, name -> {
            final IndexAbstraction indexAbstraction = lookup.get(name);
            if (indexAbstraction == null) {
                // test access (by name) to a resource that does not currently exist
                // the action handler must handle the case of accessing resources that do not exist
                return predicate.test(name, null);
            } else {
                // We check the parent data stream first if there is one. For testing requested indices, this is most likely
                // more efficient than checking the index name first because we recommend grant privileges over data stream
                // instead of backing indices.
                return (indexAbstraction.getParentDataStream() != null && predicate.test(indexAbstraction.getParentDataStream()))
                    || predicate.test(indexAbstraction);
            }
        });
    }

    private IndexAuthorizationResult buildIndicesAccessControl(
        String action,
        Role role,
        ResolvedIndices resolvedIndices,
        Map<String, IndexAbstraction> aliasAndIndexLookup
    ) {
        final IndicesAccessControl accessControl = role.authorize(
            action,
            Sets.newHashSet(resolvedIndices.getLocal()),
            aliasAndIndexLookup,
            fieldPermissionsCache
        );
        return new IndexAuthorizationResult(accessControl);
    }

    private static RBACAuthorizationInfo ensureRBAC(AuthorizationInfo authorizationInfo) {
        if (authorizationInfo instanceof RBACAuthorizationInfo == false) {
            throw new IllegalArgumentException("unsupported authorization info:" + authorizationInfo.getClass().getSimpleName());
        }
        return (RBACAuthorizationInfo) authorizationInfo;
    }

    public static Role maybeGetRBACEngineRole(AuthorizationInfo authorizationInfo) {
        if (authorizationInfo instanceof RBACAuthorizationInfo) {
            return ((RBACAuthorizationInfo) authorizationInfo).getRole();
        }
        return null;
    }

    private static boolean checkChangePasswordAction(Authentication authentication) {
        // we need to verify that this user was authenticated by or looked up by a realm type that support password changes
        // otherwise we open ourselves up to issues where a user in a different realm could be created with the same username
        // and do malicious things
        final boolean isRunAs = authentication.isRunAs();
        final String realmType;
        if (isRunAs) {
            realmType = authentication.getEffectiveSubject().getRealm().getType();
        } else {
            realmType = authentication.getAuthenticatingSubject().getRealm().getType();
        }

        assert realmType != null;
        // Ensure that the user is not authenticated with an access token or an API key.
        // Also ensure that the user was authenticated by a realm that we can change a password for. The native realm is an internal realm
        // and right now only one can exist in the realm configuration - if this changes we should update this check
        final AuthenticationType authType = authentication.getAuthenticationType();
        return (authType.equals(AuthenticationType.REALM)
            && (ReservedRealm.TYPE.equals(realmType) || NativeRealmSettings.TYPE.equals(realmType)));
    }

    static class RBACAuthorizationInfo implements AuthorizationInfo {

        private final Role role;
        private final Map<String, Object> info;
        private final RBACAuthorizationInfo authenticatedUserAuthorizationInfo;

        RBACAuthorizationInfo(Role role, Role authenticatedUserRole) {
            this.role = Objects.requireNonNull(role);
            this.info = Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, role.names());
            this.authenticatedUserAuthorizationInfo = authenticatedUserRole == null
                ? this
                : new RBACAuthorizationInfo(authenticatedUserRole, null);
        }

        Role getRole() {
            return role;
        }

        @Override
        public Map<String, Object> asMap() {
            return info;
        }

        @Override
        public RBACAuthorizationInfo getAuthenticatedUserAuthorizationInfo() {
            return authenticatedUserAuthorizationInfo;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RBACAuthorizationInfo that = (RBACAuthorizationInfo) o;
            if (this.role.equals(that.role) == false) {
                return false;
            }
            // Because authenticatedUserAuthorizationInfo can be reference to this, calling `equals` can result in infinite recursion.
            // But if both user-authz-info objects are references to their containing object, then they must be equal.
            if (this.authenticatedUserAuthorizationInfo == this) {
                return that.authenticatedUserAuthorizationInfo == that;
            } else {
                return this.authenticatedUserAuthorizationInfo.equals(that.authenticatedUserAuthorizationInfo);
            }
        }

        @Override
        public int hashCode() {
            // Since authenticatedUserAuthorizationInfo can self reference, we handle it specially to avoid infinite recursion.
            if (this.authenticatedUserAuthorizationInfo == this) {
                return Objects.hashCode(role);
            } else {
                return Objects.hash(role, authenticatedUserAuthorizationInfo);
            }
        }
    }

    private static boolean isScrollRelatedAction(String action) {
        return action.equals(SearchScrollAction.NAME)
            || action.equals(SearchTransportService.FETCH_ID_SCROLL_ACTION_NAME)
            || action.equals(SearchTransportService.QUERY_FETCH_SCROLL_ACTION_NAME)
            || action.equals(SearchTransportService.QUERY_SCROLL_ACTION_NAME)
            || action.equals(SearchTransportService.FREE_CONTEXT_SCROLL_ACTION_NAME)
            || action.equals(ClearScrollAction.NAME)
            || action.equals("indices:data/read/sql/close_cursor")
            || action.equals(SearchTransportService.CLEAR_SCROLL_CONTEXTS_ACTION_NAME);
    }

    private static boolean isAsyncRelatedAction(String action) {
        return action.equals(SubmitAsyncSearchAction.NAME)
            || action.equals(GetAsyncSearchAction.NAME)
            || action.equals(DeleteAsyncResultAction.NAME)
            || action.equals(EqlAsyncActionNames.EQL_ASYNC_GET_RESULT_ACTION_NAME)
            || action.equals(SqlAsyncActionNames.SQL_ASYNC_GET_RESULT_ACTION_NAME);
    }

    static final class AuthorizedIndices implements AuthorizationEngine.AuthorizedIndices {

        private final CachedSupplier<Set<String>> allAuthorizedAndAvailableSupplier;
        private final Predicate<String> isAuthorizedPredicate;

        AuthorizedIndices(Supplier<Set<String>> allAuthorizedAndAvailableSupplier, Predicate<String> isAuthorizedPredicate) {
            this.allAuthorizedAndAvailableSupplier = new CachedSupplier<>(allAuthorizedAndAvailableSupplier);
            this.isAuthorizedPredicate = Objects.requireNonNull(isAuthorizedPredicate);
        }

        @Override
        public Supplier<Set<String>> all() {
            return allAuthorizedAndAvailableSupplier;
        }

        @Override
        public boolean check(String name) {
            return this.isAuthorizedPredicate.test(name);
        }
    }
}

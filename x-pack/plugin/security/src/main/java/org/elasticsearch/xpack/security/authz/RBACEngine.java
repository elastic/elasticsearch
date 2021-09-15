/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.termvectors.MultiTermVectorsAction;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.async.DeleteAsyncResultAction;
import org.elasticsearch.xpack.core.eql.EqlAsyncActionNames;
import org.elasticsearch.action.search.ClosePointInTimeAction;
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchAction;
import org.elasticsearch.xpack.core.security.action.GetApiKeyAction;
import org.elasticsearch.xpack.core.security.action.GetApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordAction;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesResponse;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.action.user.UserRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.AuthenticationType;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.ResolvedIndices;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition;
import org.elasticsearch.xpack.core.security.authz.permission.IndicesPermission;
import org.elasticsearch.xpack.core.security.authz.permission.ResourcePrivileges;
import org.elasticsearch.xpack.core.security.authz.permission.ResourcePrivilegesMap;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.NamedClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.Privilege;
import org.elasticsearch.xpack.core.security.support.StringMatcher;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.sql.SqlAsyncActionNames;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
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
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;

import static org.elasticsearch.common.Strings.arrayToCommaDelimitedString;
import static org.elasticsearch.xpack.security.action.user.TransportHasPrivilegesAction.getApplicationNames;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.PRINCIPAL_ROLES_FIELD_NAME;

public class RBACEngine implements AuthorizationEngine {

    private static final Predicate<String> SAME_USER_PRIVILEGE = StringMatcher.of(
        ChangePasswordAction.NAME, AuthenticateAction.NAME, HasPrivilegesAction.NAME, GetUserPrivilegesAction.NAME, GetApiKeyAction.NAME);
    private static final String INDEX_SUB_REQUEST_PRIMARY = IndexAction.NAME + "[p]";
    private static final String INDEX_SUB_REQUEST_REPLICA = IndexAction.NAME + "[r]";
    private static final String DELETE_SUB_REQUEST_PRIMARY = DeleteAction.NAME + "[p]";
    private static final String DELETE_SUB_REQUEST_REPLICA = DeleteAction.NAME + "[r]";

    private static final Logger logger = LogManager.getLogger(RBACEngine.class);

    private final CompositeRolesStore rolesStore;
    private final FieldPermissionsCache fieldPermissionsCache;

    public RBACEngine(Settings settings, CompositeRolesStore rolesStore) {
        this.rolesStore = rolesStore;
        this.fieldPermissionsCache = new FieldPermissionsCache(settings);
    }

    @Override
    public void resolveAuthorizationInfo(RequestInfo requestInfo, ActionListener<AuthorizationInfo> listener) {
        final Authentication authentication = requestInfo.getAuthentication();
        getRoles(authentication.getUser(), authentication, ActionListener.wrap(role -> {
            if (authentication.getUser().isRunAs()) {
                getRoles(authentication.getUser().authenticatedUser(), authentication, ActionListener.wrap(
                    authenticatedUserRole -> listener.onResponse(new RBACAuthorizationInfo(role, authenticatedUserRole)),
                    listener::onFailure));
            } else {
                listener.onResponse(new RBACAuthorizationInfo(role, role));
            }
        }, listener::onFailure));
    }

    private void getRoles(User user, Authentication authentication, ActionListener<Role> listener) {
        rolesStore.getRoles(user, authentication, listener);
    }

    @Override
    public void authorizeRunAs(RequestInfo requestInfo, AuthorizationInfo authorizationInfo, ActionListener<AuthorizationResult> listener) {
        if (authorizationInfo instanceof RBACAuthorizationInfo) {
            final Role role = ((RBACAuthorizationInfo) authorizationInfo).getAuthenticatedUserAuthorizationInfo().getRole();
            listener.onResponse(
                new AuthorizationResult(role.checkRunAs(requestInfo.getAuthentication().getUser().principal())));
        } else {
            listener.onFailure(new IllegalArgumentException("unsupported authorization info:" +
                authorizationInfo.getClass().getSimpleName()));
        }
    }

    @Override
    public void authorizeClusterAction(RequestInfo requestInfo, AuthorizationInfo authorizationInfo,
                                       ActionListener<AuthorizationResult> listener) {
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
            listener.onFailure(new IllegalArgumentException("unsupported authorization info:" +
                authorizationInfo.getClass().getSimpleName()));
        }
    }

    // pkg private for testing
    boolean checkSameUserPermissions(String action, TransportRequest request, Authentication authentication) {
        final boolean actionAllowed = SAME_USER_PRIVILEGE.test(action);
        if (actionAllowed) {
            if (request instanceof UserRequest) {
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
            } else if (request instanceof GetApiKeyRequest) {
                GetApiKeyRequest getApiKeyRequest = (GetApiKeyRequest) request;
                if (AuthenticationType.API_KEY == authentication.getAuthenticationType()) {
                    assert authentication.getLookedUpBy() == null : "runAs not supported for api key authentication";
                    // if authenticated by API key then the request must also contain same API key id
                    String authenticatedApiKeyId = (String) authentication.getMetadata().get(ApiKeyService.API_KEY_ID_KEY);
                    if (Strings.hasText(getApiKeyRequest.getApiKeyId())) {
                        return getApiKeyRequest.getApiKeyId().equals(authenticatedApiKeyId);
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
                    throw new IllegalStateException("Composite and bulk actions must implement " +
                        CompositeIndicesRequest.class.getSimpleName() + ", " + request.getClass().getSimpleName() + " doesn't. Action " +
                        action);
                }
                return true;
            default:
                return false;
        }
    }

    @Override
    public void authorizeIndexAction(RequestInfo requestInfo, AuthorizationInfo authorizationInfo,
                                     AsyncSupplier<ResolvedIndices> indicesAsyncSupplier,
                                     Map<String, IndexAbstraction> aliasOrIndexLookup,
                                     ActionListener<IndexAuthorizationResult> listener) {
        final String action = requestInfo.getAction();
        final TransportRequest request = requestInfo.getRequest();
        if (TransportActionProxy.isProxyAction(action) || shouldAuthorizeIndexActionNameOnly(action, request)) {
            // we've already validated that the request is a proxy request so we can skip that but we still
            // need to validate that the action is allowed and then move on
            try {
                listener.onResponse(authorizeIndexActionName(action, authorizationInfo, null));
            } catch (Exception e) {
                listener.onFailure(e);
            }
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
                    ActionRunnable.supply(
                        ActionListener.wrap(parsedScrollId -> {
                            if (parsedScrollId.hasLocalIndices()) {
                                listener.onResponse(authorizeIndexActionName(action, authorizationInfo, null));
                            } else {
                                listener.onResponse(new IndexAuthorizationResult(true, null));
                            }
                        }, listener::onFailure),
                        ((SearchScrollRequest) request)::parseScrollId
                    ).run();
                } else {
                    // RBACEngine simply authorizes scroll related actions without filling in any DLS/FLS permissions.
                    // Scroll related actions have special security logic, where the security context of the initial search
                    // request is attached to the scroll context upon creation in {@code SecuritySearchOperationListener#onNewScrollContext}
                    // and it is then verified, before every use of the scroll, in
                    // {@code SecuritySearchOperationListener#validateSearchContext}.
                    // The DLS/FLS permissions are used inside the {@code DirectoryReader} that {@code SecurityIndexReaderWrapper}
                    // built while handling the initial search request. In addition, for consistency, the DLS/FLS permissions from
                    // the originating search request are attached to the thread context upon validating the scroll.
                    listener.onResponse(new IndexAuthorizationResult(true, null));
                }
            } else if (isAsyncRelatedAction(action)) {
                if (SubmitAsyncSearchAction.NAME.equals(action)) {
                    // authorize submit async search but don't fill in the DLS/FLS permissions
                    // the `null` IndicesAccessControl parameter indicates that this action has *not* determined
                    // which DLS/FLS controls should be applied to this action
                    listener.onResponse(new IndexAuthorizationResult(true, null));
                } else {
                    // async-search actions other than submit have a custom security layer that checks if the current user is
                    // the same as the user that submitted the original request so no additional checks are needed here.
                    listener.onResponse(new IndexAuthorizationResult(true, IndicesAccessControl.ALLOW_NO_INDICES));
                }
            } else if (action.equals(ClosePointInTimeAction.NAME)) {
                    listener.onResponse(new IndexAuthorizationResult(true, IndicesAccessControl.ALLOW_NO_INDICES));
            } else {
                assert false : "only scroll and async-search related requests are known indices api that don't " +
                    "support retrieving the indices they relate to";
                listener.onFailure(new IllegalStateException("only scroll and async-search related requests are known indices " +
                    "api that don't support retrieving the indices they relate to"));
            }
        } else if (((IndicesRequest) request).allowsRemoteIndices()) {
            // remote indices are allowed
            indicesAsyncSupplier.getAsync(ActionListener.wrap(resolvedIndices -> {
                assert resolvedIndices.isEmpty() == false
                    : "every indices request needs to have its indices set thus the resolved indices must not be empty";
                //all wildcard expressions have been resolved and only the security plugin could have set '-*' here.
                //'-*' matches no indices so we allow the request to go through, which will yield an empty response
                if (resolvedIndices.isNoIndicesPlaceholder()) {
                    // check action name
                    listener.onResponse(authorizeIndexActionName(action, authorizationInfo, IndicesAccessControl.ALLOW_NO_INDICES));
                } else {
                    listener.onResponse(buildIndicesAccessControl(
                        action, authorizationInfo, Sets.newHashSet(resolvedIndices.getLocal()), aliasOrIndexLookup));
                }
            }, listener::onFailure));
        } else {
            try {
                final IndexAuthorizationResult indexAuthorizationResult =
                        authorizeIndexActionName(action, authorizationInfo, IndicesAccessControl.ALLOW_NO_INDICES);
                if (indexAuthorizationResult.isGranted()) {
                    indicesAsyncSupplier.getAsync(ActionListener.wrap(resolvedIndices -> {
                        assert resolvedIndices.isEmpty() == false
                                : "every indices request needs to have its indices set thus the resolved indices must not be empty";
                        //all wildcard expressions have been resolved and only the security plugin could have set '-*' here.
                        //'-*' matches no indices so we allow the request to go through, which will yield an empty response
                        if (resolvedIndices.isNoIndicesPlaceholder()) {
                            listener.onResponse(new IndexAuthorizationResult(true, IndicesAccessControl.ALLOW_NO_INDICES));
                        } else {
                            listener.onResponse(buildIndicesAccessControl(
                                action, authorizationInfo, Sets.newHashSet(resolvedIndices.getLocal()), aliasOrIndexLookup));
                        }
                    }, listener::onFailure));
                } else {
                    listener.onResponse(indexAuthorizationResult);
                }
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }
    }

    private static IndexAuthorizationResult authorizeIndexActionName(String action,
                                                                     AuthorizationInfo authorizationInfo,
                                                                     IndicesAccessControl grantedValue) {
        final Role role = ensureRBAC(authorizationInfo).getRole();
        return new IndexAuthorizationResult(true, role.checkIndicesAction(action) ? grantedValue : IndicesAccessControl.DENIED);

    }

    @Override
    public void loadAuthorizedIndices(RequestInfo requestInfo, AuthorizationInfo authorizationInfo,
                                      Map<String, IndexAbstraction> indicesLookup, ActionListener<Set<String>> listener) {
        if (authorizationInfo instanceof RBACAuthorizationInfo) {
            final Role role = ((RBACAuthorizationInfo) authorizationInfo).getRole();
            listener.onResponse(resolveAuthorizedIndicesFromRole(role, requestInfo, indicesLookup));
        } else {
            listener.onFailure(
                new IllegalArgumentException("unsupported authorization info:" + authorizationInfo.getClass().getSimpleName()));
        }
    }

    @Override
    public void validateIndexPermissionsAreSubset(RequestInfo requestInfo, AuthorizationInfo authorizationInfo,
                                                  Map<String, List<String>> indexNameToNewNames,
                                                  ActionListener<AuthorizationResult> listener) {
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
                new IllegalArgumentException("unsupported authorization info:" + authorizationInfo.getClass().getSimpleName()));
        }
    }

    @Override
    public void checkPrivileges(Authentication authentication, AuthorizationInfo authorizationInfo,
                                HasPrivilegesRequest request,
                                Collection<ApplicationPrivilegeDescriptor> applicationPrivileges,
                                ActionListener<HasPrivilegesResponse> listener) {
        if (authorizationInfo instanceof RBACAuthorizationInfo == false) {
            listener.onFailure(
                new IllegalArgumentException("unsupported authorization info:" + authorizationInfo.getClass().getSimpleName()));
            return;
        }
        final Role userRole = ((RBACAuthorizationInfo) authorizationInfo).getRole();
        logger.trace(() -> new ParameterizedMessage("Check whether role [{}] has privileges cluster=[{}] index=[{}] application=[{}]",
            Strings.arrayToCommaDelimitedString(userRole.names()),
            Strings.arrayToCommaDelimitedString(request.clusterPrivileges()),
            Strings.arrayToCommaDelimitedString(request.indexPrivileges()),
            Strings.arrayToCommaDelimitedString(request.applicationPrivileges())
        ));

        Map<String, Boolean> cluster = new HashMap<>();
        for (String checkAction : request.clusterPrivileges()) {
            cluster.put(checkAction, userRole.grants(ClusterPrivilegeResolver.resolve(checkAction)));
        }
        boolean allMatch = cluster.values().stream().allMatch(Boolean::booleanValue);
        ResourcePrivilegesMap.Builder combineIndicesResourcePrivileges = ResourcePrivilegesMap.builder();
        for (RoleDescriptor.IndicesPrivileges check : request.indexPrivileges()) {
            ResourcePrivilegesMap resourcePrivileges = userRole.checkIndicesPrivileges(Sets.newHashSet(check.getIndices()),
                check.allowRestrictedIndices(), Sets.newHashSet(check.getPrivileges()));
            allMatch = allMatch && resourcePrivileges.allAllowed();
            combineIndicesResourcePrivileges.addResourcePrivilegesMap(resourcePrivileges);
        }
        ResourcePrivilegesMap allIndices = combineIndicesResourcePrivileges.build();
        allMatch = allMatch && allIndices.allAllowed();

        final Map<String, Collection<ResourcePrivileges>> privilegesByApplication = new HashMap<>();
        for (String applicationName : getApplicationNames(request)) {
            logger.debug("Checking privileges for application {}", applicationName);
            ResourcePrivilegesMap.Builder builder = ResourcePrivilegesMap.builder();
            for (RoleDescriptor.ApplicationResourcePrivileges p : request.applicationPrivileges()) {
                if (applicationName.equals(p.getApplication())) {
                    ResourcePrivilegesMap appPrivsByResourceMap = userRole.checkApplicationResourcePrivileges(applicationName,
                        Sets.newHashSet(p.getResources()), Sets.newHashSet(p.getPrivileges()), applicationPrivileges);
                    builder.addResourcePrivilegesMap(appPrivsByResourceMap);
                }
            }
            ResourcePrivilegesMap resourcePrivsForApplication = builder.build();
            allMatch = allMatch && resourcePrivsForApplication.allAllowed();
            privilegesByApplication.put(applicationName, resourcePrivsForApplication.getResourceToResourcePrivileges().values());
        }

        listener.onResponse(new HasPrivilegesResponse(request.username(), allMatch, cluster,
            allIndices.getResourceToResourcePrivileges().values(), privilegesByApplication));
    }


    @Override
    public void getUserPrivileges(Authentication authentication, AuthorizationInfo authorizationInfo, GetUserPrivilegesRequest request,
                                  ActionListener<GetUserPrivilegesResponse> listener) {
        if (authorizationInfo instanceof RBACAuthorizationInfo == false) {
            listener.onFailure(
                new IllegalArgumentException("unsupported authorization info:" + authorizationInfo.getClass().getSimpleName()));
        } else {
            final Role role = ((RBACAuthorizationInfo) authorizationInfo).getRole();
            listener.onResponse(buildUserPrivilegesResponseObject(role));
        }
    }

    GetUserPrivilegesResponse buildUserPrivilegesResponseObject(Role userRole) {
        logger.trace(() -> new ParameterizedMessage("List privileges for role [{}]", arrayToCommaDelimitedString(userRole.names())));

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
                    "found unsupported cluster privilege : " + privilege +
                        ((privilege != null) ? " of type " + privilege.getClass().getSimpleName() : ""));
            }
        }

        final Set<GetUserPrivilegesResponse.Indices> indices = new LinkedHashSet<>();
        for (IndicesPermission.Group group : userRole.indices().groups()) {
            final Set<BytesReference> queries = group.getQuery() == null ? Collections.emptySet() : group.getQuery();
            final Set<FieldPermissionsDefinition.FieldGrantExcludeGroup> fieldSecurity;
            if (group.getFieldPermissions().hasFieldLevelSecurity()) {
                final FieldPermissionsDefinition definition = group.getFieldPermissions().getFieldPermissionsDefinition();
                assert group.getFieldPermissions().getLimitedByFieldPermissionsDefinition() == null
                    : "limited-by field must not exist since we do not support reporting user privileges for limited roles";
                fieldSecurity = definition.getFieldGrantExcludeGroups();
            } else {
                fieldSecurity = Collections.emptySet();
            }
            indices.add(new GetUserPrivilegesResponse.Indices(
                Arrays.asList(group.indices()),
                group.privilege().name(),
                fieldSecurity,
                queries,
                group.allowRestrictedIndices()
            ));
        }

        final Set<RoleDescriptor.ApplicationResourcePrivileges> application = new LinkedHashSet<>();
        for (String applicationName : userRole.application().getApplicationNames()) {
            for (ApplicationPrivilege privilege : userRole.application().getPrivileges(applicationName)) {
                final Set<String> resources = userRole.application().getResourcePatterns(privilege);
                if (resources.isEmpty()) {
                    logger.trace("No resources defined in application privilege {}", privilege);
                } else {
                    application.add(RoleDescriptor.ApplicationResourcePrivileges.builder()
                        .application(applicationName)
                        .privileges(privilege.name())
                        .resources(resources)
                        .build());
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

        return new GetUserPrivilegesResponse(cluster, conditionalCluster, indices, application, runAs);
    }

    static Set<String> resolveAuthorizedIndicesFromRole(Role role, RequestInfo requestInfo, Map<String, IndexAbstraction> lookup) {
        Predicate<IndexAbstraction> predicate = role.allowedIndicesMatcher(requestInfo.getAction());

        // do not include data streams for actions that do not operate on data streams
        TransportRequest request = requestInfo.getRequest();
        final boolean includeDataStreams = (request instanceof IndicesRequest) && ((IndicesRequest) request).includeDataStreams();

        Set<String> indicesAndAliases = new HashSet<>();
        // TODO: can this be done smarter? I think there are usually more indices/aliases in the cluster then indices defined a roles?
        if (includeDataStreams) {
            for (IndexAbstraction indexAbstraction : lookup.values()) {
                if (predicate.test(indexAbstraction)) {
                    indicesAndAliases.add(indexAbstraction.getName());
                    if (indexAbstraction.getType() == IndexAbstraction.Type.DATA_STREAM) {
                        // add data stream and its backing indices for any authorized data streams
                        for (IndexMetadata indexMetadata : indexAbstraction.getIndices()) {
                            indicesAndAliases.add(indexMetadata.getIndex().getName());
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
        return Collections.unmodifiableSet(indicesAndAliases);
    }

    private IndexAuthorizationResult buildIndicesAccessControl(String action,
                                                               AuthorizationInfo authorizationInfo,
                                                               Set<String> indices,
                                                               Map<String, IndexAbstraction> aliasAndIndexLookup) {
        final Role role = ensureRBAC(authorizationInfo).getRole();
        final IndicesAccessControl accessControl = role.authorize(action, indices, aliasAndIndexLookup, fieldPermissionsCache);
        return new IndexAuthorizationResult(true, accessControl);
    }

    private static RBACAuthorizationInfo ensureRBAC(AuthorizationInfo authorizationInfo) {
        if (authorizationInfo instanceof RBACAuthorizationInfo == false) {
            throw new IllegalArgumentException("unsupported authorization info:" + authorizationInfo.getClass().getSimpleName());
        }
        return (RBACAuthorizationInfo) authorizationInfo;
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
            this.role = role;
            this.info = Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, role.names());
            this.authenticatedUserAuthorizationInfo =
                authenticatedUserRole == null ? this : new RBACAuthorizationInfo(authenticatedUserRole, null);
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

    private static boolean isAsyncRelatedAction(String action) {
        return action.equals(SubmitAsyncSearchAction.NAME) ||
            action.equals(GetAsyncSearchAction.NAME) ||
            action.equals(DeleteAsyncResultAction.NAME) ||
            action.equals(EqlAsyncActionNames.EQL_ASYNC_GET_RESULT_ACTION_NAME) ||
            action.equals(SqlAsyncActionNames.SQL_ASYNC_GET_RESULT_ACTION_NAME);
    }
}

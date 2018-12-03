/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authz;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.termvectors.MultiTermVectorsAction;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordAction;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.UserRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.XPackSecurityUser;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.PRINCIPAL_ROLES_FIELD_NAME;

public class RBACEngine implements AuthorizationEngine {

    private static final Predicate<String> SAME_USER_PRIVILEGE = Automatons.predicate(
        ChangePasswordAction.NAME, AuthenticateAction.NAME, HasPrivilegesAction.NAME, GetUserPrivilegesAction.NAME);
    private static final Predicate<String> MONITOR_INDEX_PREDICATE = IndexPrivilege.MONITOR.predicate();
    private static final String INDEX_SUB_REQUEST_PRIMARY = IndexAction.NAME + "[p]";
    private static final String INDEX_SUB_REQUEST_REPLICA = IndexAction.NAME + "[r]";
    private static final String DELETE_SUB_REQUEST_PRIMARY = DeleteAction.NAME + "[p]";
    private static final String DELETE_SUB_REQUEST_REPLICA = DeleteAction.NAME + "[r]";

    private static final Logger logger = LogManager.getLogger(RBACEngine.class);

    private final CompositeRolesStore rolesStore;
    private final FieldPermissionsCache fieldPermissionsCache;
    private final AnonymousUser anonymousUser;
    private final boolean isAnonymousEnabled;

    RBACEngine(Settings settings, CompositeRolesStore rolesStore, AnonymousUser anonymousUser, boolean isAnonymousEnabled) {
        this.rolesStore = rolesStore;
        this.fieldPermissionsCache = new FieldPermissionsCache(settings);
        this.anonymousUser = anonymousUser;
        this.isAnonymousEnabled = isAnonymousEnabled;
    }

    @Override
    public void resolveAuthorizationInfo(Authentication authentication, TransportRequest request, String action,
                                         ActionListener<AuthorizationInfo> listener) {
        getRoles(authentication.getUser(), ActionListener.wrap(role -> {
            if (authentication.getUser().isRunAs()) {
                getRoles(authentication.getUser().authenticatedUser(), ActionListener.wrap(
                    authenticatedUserRole -> listener.onResponse(new RBACAuthorizationInfo(role, authenticatedUserRole)),
                    listener::onFailure));
            } else {
                listener.onResponse(new RBACAuthorizationInfo(role, role));
            }
        }, listener::onFailure));
    }

    private void getRoles(User user, ActionListener<Role> roleActionListener) {
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

        Set<String> roleNames = new HashSet<>(Arrays.asList(user.roles()));
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

    @Override
    public void authorizeRunAs(Authentication authentication, TransportRequest request, String action, AuthorizationInfo authorizationInfo,
                               ActionListener<AuthorizationResult> listener) {
        if (authorizationInfo instanceof RBACAuthorizationInfo) {
            final Role role = ((RBACAuthorizationInfo) authorizationInfo).getAuthenticatedUserAuthorizationInfo().getRole();
            listener.onResponse(new AuthorizationResult(role.runAs().check(authentication.getUser().principal())));
        } else {
            listener.onFailure(new IllegalArgumentException("unsupported authorization info:" +
                authorizationInfo.getClass().getSimpleName()));
        }
    }

    @Override
    public void authorizeClusterAction(Authentication authentication, TransportRequest request, String action,
                                       AuthorizationInfo authorizationInfo, ActionListener<AuthorizationResult> listener) {
        if (authorizationInfo instanceof RBACAuthorizationInfo) {
            final Role role = ((RBACAuthorizationInfo) authorizationInfo).getRole();
            if (role.cluster().check(action, request)) {
                listener.onResponse(AuthorizationResult.granted());
            } else if (checkSameUserPermissions(action, request, authentication)) {
                listener.onResponse(AuthorizationResult.granted());
            } else {
                listener.onResponse(AuthorizationResult.deny());
            }
        } else {
            listener.onFailure(new IllegalArgumentException("unsupported authorization info:" +
                authorizationInfo.getClass().getSimpleName()));
        }
    }

    @Override
    public boolean checkSameUserPermissions(String action, TransportRequest request, Authentication authentication) {
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

    @Override
    public boolean shouldAuthorizeIndexActionNameOnly(String action, TransportRequest request) {
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
                if (request instanceof CompositeIndicesRequest == false) {
                    throw new IllegalStateException("Composite and bulk actions must implement " +
                        CompositeIndicesRequest.class.getSimpleName() + ", " + request.getClass().getSimpleName() + " doesn't");
                }
                return true;
            default:
                return false;
        }
    }

    @Override
    public void authorizeIndexActionName(Authentication authentication, TransportRequest request, String action,
                                         AuthorizationInfo authorizationInfo, ActionListener<AuthorizationResult> listener) {
        if (authorizationInfo instanceof RBACAuthorizationInfo) {
            final Role role = ((RBACAuthorizationInfo) authorizationInfo).getRole();
            if (role.indices().check(action)) {
                listener.onResponse(AuthorizationResult.granted());
            } else {
                listener.onResponse(AuthorizationResult.deny());
            }
        } else {
            listener.onFailure(new IllegalArgumentException("unsupported authorization info:" +
                authorizationInfo.getClass().getSimpleName()));
        }
    }

    @Override
    public List<String> loadAuthorizedIndices(Authentication authentication, TransportRequest request, String action,
                                              AuthorizationInfo authorizationInfo, Map<String, AliasOrIndex> aliasAndIndexLookup) {
        if (authorizationInfo instanceof RBACAuthorizationInfo) {
            final Role role = ((RBACAuthorizationInfo) authorizationInfo).getRole();
            return resolveAuthorizedIndicesFromRole(role, action, aliasAndIndexLookup);
        } else {
            throw new IllegalArgumentException("unsupported authorization info:" + authorizationInfo.getClass().getSimpleName());
        }
    }

    static List<String> resolveAuthorizedIndicesFromRole(Role role, String action, Map<String, AliasOrIndex> aliasAndIndexLookup) {
        Predicate<String> predicate = role.indices().allowedIndicesMatcher(action);

        List<String> indicesAndAliases = new ArrayList<>();
        // TODO: can this be done smarter? I think there are usually more indices/aliases in the cluster then indices defined a roles?
        for (Map.Entry<String, AliasOrIndex> entry : aliasAndIndexLookup.entrySet()) {
            String aliasOrIndex = entry.getKey();
            if (predicate.test(aliasOrIndex)) {
                indicesAndAliases.add(aliasOrIndex);
            }
        }

        if (Arrays.asList(role.names()).contains(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName()) == false) {
            // we should filter out all of the security indices from wildcards
            indicesAndAliases.removeAll(SecurityIndexManager.indexNames());
        }
        return Collections.unmodifiableList(indicesAndAliases);
    }

    @Override
    public void buildIndicesAccessControl(Authentication authentication, TransportRequest request, String action,
                                          AuthorizationInfo authorizationInfo, Set<String> indices,
                                          SortedMap<String, AliasOrIndex> aliasAndIndexLookup,
                                          ActionListener<IndexAuthorizationResult> listener) {
        if (authorizationInfo instanceof RBACAuthorizationInfo) {
            final Role role = ((RBACAuthorizationInfo) authorizationInfo).getRole();
            final IndicesAccessControl accessControl = role.authorize(action, indices, aliasAndIndexLookup, fieldPermissionsCache);
            if (accessControl.isGranted() && hasSecurityIndexAccess(accessControl) && MONITOR_INDEX_PREDICATE.test(action) == false
                && isSuperuser(authentication.getUser()) == false) {
                // only superusers are allowed to work with this index, but we should allow indices monitoring actions through
                // for debugging
                // purposes. These monitor requests also sometimes resolve indices concretely and then requests them
                logger.debug("user [{}] attempted to directly perform [{}] against the security index [{}]",
                    authentication.getUser().principal(), action, SecurityIndexManager.SECURITY_INDEX_NAME);
                listener.onResponse(new IndexAuthorizationResult(true, new IndicesAccessControl(false, Collections.emptyMap())));
            } else {
                listener.onResponse(new IndexAuthorizationResult(true, accessControl));
            }
        } else {
            listener.onFailure(new IllegalArgumentException("unsupported authorization info:" +
                authorizationInfo.getClass().getSimpleName()));
        }
    }

    private static boolean hasSecurityIndexAccess(IndicesAccessControl indicesAccessControl) {
        for (String index : SecurityIndexManager.indexNames()) {
            final IndicesAccessControl.IndexAccessControl indexPermissions = indicesAccessControl.getIndexPermissions(index);
            if (indexPermissions != null && indexPermissions.isGranted()) {
                return true;
            }
        }
        return false;
    }

    static boolean isSuperuser(User user) {
        return Arrays.asList(user.roles()).contains(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName());
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
}

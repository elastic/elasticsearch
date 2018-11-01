package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.termvectors.MultiTermVectorsAction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordAction;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.UserRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Predicate;

public class RBACEngine implements AuthorizationEngine {

    private static final Predicate<String> SAME_USER_PRIVILEGE = Automatons.predicate(
        ChangePasswordAction.NAME, AuthenticateAction.NAME, HasPrivilegesAction.NAME, GetUserPrivilegesAction.NAME);

    private final CompositeRolesStore rolesStore;
    private final FieldPermissionsCache fieldPermissionsCache;

    public RBACEngine(Settings settings, CompositeRolesStore rolesStore) {
        this.rolesStore = rolesStore;
        this.fieldPermissionsCache = new FieldPermissionsCache(settings);
    }

    @Override
    public void resolveAuthorizationInfo(Authentication authentication, TransportRequest request, String action,
                                         ActionListener<AuthorizationInfo> listener) {
        rolesStore.roles(new HashSet<>(Arrays.asList(authentication.getUser().roles())), fieldPermissionsCache,
            ActionListener.wrap(role -> {
                if (authentication.getUser().isRunAs()) {
                    rolesStore.roles(new HashSet<>(Arrays.asList(authentication.getUser().authenticatedUser().roles())),
                        fieldPermissionsCache, ActionListener.wrap(
                            authenticatedUserRole -> listener.onResponse(new RBACAuthorizationInfo(role, authenticatedUserRole)),
                            listener::onFailure));
                } else {
                    listener.onResponse(new RBACAuthorizationInfo(role, role));
                }
            }, listener::onFailure));
    }

    @Override
    public void authorizeRunAs(Authentication authentication, TransportRequest request, String action, AuthorizationInfo authorizationInfo,
                               ActionListener<AuthorizationResult> listener) {
        if (authorizationInfo instanceof RBACAuthorizationInfo) {
            final Role role = ((RBACAuthorizationInfo) authorizationInfo).getAuthenticatedUserRole();
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
            final Role role = ((RBACAuthorizationInfo) authorizationInfo).getAuthenticatedUserRole();
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
    public boolean isCompositeAction(String action) {
        switch (action) {
            case BulkAction.NAME:
            case MultiGetAction.NAME:
            case MultiTermVectorsAction.NAME:
            case MultiSearchAction.NAME:
            case "indices:data/read/mpercolate":
            case "indices:data/read/msearch/template":
            case "indices:data/read/search/template":
            case "indices:data/write/reindex":
            case "indices:data/read/sql":
            case "indices:data/read/sql/translate":
                return true;
            default:
                return false;
        }
    }

    @Override
    public void authorizeIndexActionName(Authentication authentication, TransportRequest request, String action,
                                         AuthorizationInfo authorizationInfo, ActionListener<AuthorizationResult> listener) {
        if (authorizationInfo instanceof RBACAuthorizationInfo) {
            final Role role = ((RBACAuthorizationInfo) authorizationInfo).getAuthenticatedUserRole();
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
        private final Role authenticatedUserRole;
        private final Map<String, Object> info;

        RBACAuthorizationInfo(Role role, Role authenticatedUserRole) {
            this.role = role;
            this.authenticatedUserRole = authenticatedUserRole;
            this.info = Collections.singletonMap("roles", Arrays.asList(role.names()));
        }

        Role getRole() {
            return role;
        }

        Role getAuthenticatedUserRole() {
            return authenticatedUserRole;
        }

        @Override
        public Map<String, Object> asMap() {
            return info;
        }
    }
}

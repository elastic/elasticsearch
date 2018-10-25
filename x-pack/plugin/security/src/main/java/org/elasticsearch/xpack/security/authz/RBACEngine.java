package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;

public class RBACEngine implements AuthorizationEngine {

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

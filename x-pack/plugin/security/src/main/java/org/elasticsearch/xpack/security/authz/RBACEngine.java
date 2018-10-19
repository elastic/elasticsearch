package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;

import java.util.Arrays;
import java.util.HashSet;

public class RBACEngine implements AuthorizationEngine {

    private final CompositeRolesStore rolesStore;
    private final FieldPermissionsCache fieldPermissionsCache;

    public RBACEngine(Settings settings, CompositeRolesStore rolesStore) {
        this.rolesStore = rolesStore;
        this.fieldPermissionsCache = new FieldPermissionsCache(settings);
    }

    @Override
    public void authorizeRunAs(Authentication authentication, TransportRequest request, String action,
                               ActionListener<AuthorizationResult> listener) {
        rolesStore.roles(new HashSet<>(Arrays.asList(authentication.getUser().authenticatedUser().roles())), fieldPermissionsCache,
            ActionListener.wrap(role ->
                listener.onResponse(new AuthorizationResult(role.runAs().check(authentication.getUser().principal()))),
                listener::onFailure));
    }
}

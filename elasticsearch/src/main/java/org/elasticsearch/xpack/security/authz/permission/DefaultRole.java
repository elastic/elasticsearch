/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.permission;

import org.elasticsearch.xpack.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.esnative.NativeRealm;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.security.action.user.ChangePasswordAction;
import org.elasticsearch.xpack.security.action.user.UserRequest;
import org.elasticsearch.xpack.security.authz.permission.RunAsPermission.Core;
import org.elasticsearch.xpack.security.authz.privilege.ClusterPrivilege;
import org.elasticsearch.xpack.security.authz.privilege.Privilege.Name;
import org.elasticsearch.transport.TransportRequest;

/**
 * A default role that will be applied to all users other than the internal {@link org.elasticsearch.xpack.security.user.SystemUser}. This
 * role grants access to actions that every user should be able to execute such as the ability to change their password and execute the
 * authenticate endpoint to get information about themselves
 */
public class DefaultRole extends Role {

    private static final ClusterPermission.Core CLUSTER_PERMISSION =
            new SameUserClusterPermission(ClusterPrivilege.get(new Name(ChangePasswordAction.NAME, AuthenticateAction.NAME)));
    private static final IndicesPermission.Core INDICES_PERMISSION = IndicesPermission.Core.NONE;
    private static final RunAsPermission.Core RUN_AS_PERMISSION = Core.NONE;

    public static final String NAME = "__default_role";
    public static final DefaultRole INSTANCE = new DefaultRole();

    private DefaultRole() {
        super(NAME, CLUSTER_PERMISSION, INDICES_PERMISSION, RUN_AS_PERMISSION);
    }

    private static class SameUserClusterPermission extends ClusterPermission.Core {

        private SameUserClusterPermission(ClusterPrivilege privilege) {
            super(privilege);
        }

        @Override
        public boolean check(String action, TransportRequest request, Authentication authentication) {
            final boolean actionAllowed = super.check(action, request, authentication);
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
                final boolean sameUsername = authentication.getRunAsUser().principal().equals(username);
                if (sameUsername && ChangePasswordAction.NAME.equals(action)) {
                    return checkChangePasswordAction(authentication);
                }

                assert AuthenticateAction.NAME.equals(action) || sameUsername == false;
                return sameUsername;
            }
            return false;
        }
    }

    static boolean checkChangePasswordAction(Authentication authentication) {
        // we need to verify that this user was authenticated by or looked up by a realm type that support password changes
        // otherwise we open ourselves up to issues where a user in a different realm could be created with the same username
        // and do malicious things
        final boolean isRunAs = authentication.isRunAs();
        final String realmType;
        if (isRunAs) {
            realmType = authentication.getLookedUpBy().getType();
        } else {
            realmType = authentication.getAuthenticatedBy().getType();
        }

        assert realmType != null;
        // ensure the user was authenticated by a realm that we can change a password for. The native realm is an internal realm and right
        // now only one can exist in the realm configuration - if this changes we should update this check
        return ReservedRealm.TYPE.equals(realmType) || NativeRealm.TYPE.equals(realmType);
    }
}

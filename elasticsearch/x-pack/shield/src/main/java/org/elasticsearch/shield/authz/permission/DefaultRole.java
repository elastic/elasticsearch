/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.permission;

import org.elasticsearch.shield.user.User;
import org.elasticsearch.shield.action.user.AuthenticateAction;
import org.elasticsearch.shield.action.user.ChangePasswordAction;
import org.elasticsearch.shield.action.user.UserRequest;
import org.elasticsearch.shield.authz.permission.RunAsPermission.Core;
import org.elasticsearch.shield.authz.privilege.ClusterPrivilege;
import org.elasticsearch.shield.authz.privilege.Privilege.Name;
import org.elasticsearch.transport.TransportRequest;

/**
 * A default role that will be applied to all users other than the internal {@link org.elasticsearch.shield.user.SystemUser}. This role
 * grants access to actions that every user should be able to execute such as the ability to change their password and execute the
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
        public boolean check(String action, TransportRequest request, User user) {
            final boolean actionAllowed = super.check(action, request, user);
            if (actionAllowed) {
                assert request instanceof UserRequest;
                UserRequest userRequest = (UserRequest) request;
                String[] usernames = userRequest.usernames();
                assert usernames != null && usernames.length == 1;
                final String username = usernames[0];
                assert username != null;
                return user.principal().equals(username);
            }
            return false;
        }
    }
}

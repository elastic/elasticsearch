/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.store;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.shield.SecurityContext;
import org.elasticsearch.shield.authz.RoleDescriptor;
import org.elasticsearch.shield.authz.permission.KibanaRole;
import org.elasticsearch.shield.authz.permission.Role;
import org.elasticsearch.shield.authz.permission.SuperuserRole;
import org.elasticsearch.shield.authz.permission.TransportClientRole;
import org.elasticsearch.shield.user.KibanaUser;
import org.elasticsearch.shield.user.SystemUser;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

/**
 *
 */
public class ReservedRolesStore implements RolesStore {

    private final SecurityContext securityContext;

    @Inject
    public ReservedRolesStore(SecurityContext securityContext) {
        this.securityContext = securityContext;
    }

    @Override
    public Role role(String role) {
        switch (role) {
            case SuperuserRole.NAME:
                return SuperuserRole.INSTANCE;
            case TransportClientRole.NAME:
                return TransportClientRole.INSTANCE;
            case KibanaRole.NAME:
                // The only user that should know about this role is the kibana user itself (who has this role). The reason we want to hide
                // this role is that it was created specifically for kibana, with all the permissions that the kibana user needs.
                // We don't want it to be assigned to other users.
                if (KibanaUser.is(securityContext.getUser())) {
                    return KibanaRole.INSTANCE;
                }
                return null;
            default:
                return null;
        }
    }

    public RoleDescriptor roleDescriptor(String role) {
        switch (role) {
            case SuperuserRole.NAME:
                return SuperuserRole.DESCRIPTOR;
            case TransportClientRole.NAME:
                return TransportClientRole.DESCRIPTOR;
            case KibanaRole.NAME:
                // The only user that should know about this role is the kibana user itself (who has this role). The reason we want to hide
                // this role is that it was created specifically for kibana, with all the permissions that the kibana user needs.
                // We don't want it to be assigned to other users.
                if (KibanaUser.is(securityContext.getUser())) {
                    return KibanaRole.DESCRIPTOR;
                }
                return null;
            default:
                return null;
        }
    }

    public Collection<RoleDescriptor> roleDescriptors() {
        if (KibanaUser.is(securityContext.getUser())) {
            return Arrays.asList(SuperuserRole.DESCRIPTOR, TransportClientRole.DESCRIPTOR, KibanaRole.DESCRIPTOR);
        }
        return Arrays.asList(SuperuserRole.DESCRIPTOR, TransportClientRole.DESCRIPTOR);
    }

    public static Set<String> names() {
        return Sets.newHashSet(SuperuserRole.NAME, KibanaRole.NAME, TransportClientRole.NAME);
    }

    public static boolean isReserved(String role) {
        switch (role) {
            case SuperuserRole.NAME:
            case KibanaRole.NAME:
            case TransportClientRole.NAME:
            case SystemUser.ROLE_NAME:
                return true;
            default:
                return false;
        }
    }
}

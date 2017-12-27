/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.store;

import org.elasticsearch.xpack.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.authz.permission.Role;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class ReservedRolesStore {

    public static final Role SUPERUSER_ROLE = Role.builder(ClientReservedRoles.SUPERUSER_ROLE_DESCRIPTOR, null).build();

    public Map<String, Object> usageStats() {
        return Collections.emptyMap();
    }

    public RoleDescriptor roleDescriptor(String role) {
        return ClientReservedRoles.RESERVED_ROLES.get(role);
    }

    public Collection<RoleDescriptor> roleDescriptors() {
        return ClientReservedRoles.RESERVED_ROLES.values();
    }

    public static Set<String> names() {
        return ClientReservedRoles.RESERVED_ROLES.keySet();
    }

}


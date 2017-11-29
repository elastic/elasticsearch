/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.store;

import org.elasticsearch.xpack.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.authz.permission.Role;
import org.elasticsearch.xpack.security.SecurityExtension;
import org.elasticsearch.xpack.security.support.MetadataUtils;
import org.elasticsearch.xpack.security.user.SystemUser;
import org.elasticsearch.xpack.security.user.XPackUser;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

public class ReservedRolesStore {

    public static final RoleDescriptor SUPERUSER_ROLE_DESCRIPTOR = new RoleDescriptor("superuser",
            new String[] { "all" },
            new RoleDescriptor.IndicesPrivileges[] {
                    RoleDescriptor.IndicesPrivileges.builder().indices("*").privileges("all").build()},
            new String[] { "*" },
            MetadataUtils.DEFAULT_RESERVED_METADATA);
    public static final Role SUPERUSER_ROLE = Role.builder(SUPERUSER_ROLE_DESCRIPTOR, null).build();
    private static final Map<String, RoleDescriptor> RESERVED_ROLES = initializeReservedRoles();

    private static Map<String, RoleDescriptor> initializeReservedRoles() {
        Map<String, RoleDescriptor> roles = new HashMap<>();

        roles.put("superuser", SUPERUSER_ROLE_DESCRIPTOR);

        // Services are loaded through SPI, and are defined in their META-INF/services
        for(SecurityExtension ext : ServiceLoader.load(SecurityExtension.class, SecurityExtension.class.getClassLoader())) {
            roles.putAll(ext.getReservedRoles());
        }

        return Collections.unmodifiableMap(roles);
    }

    public Map<String, Object> usageStats() {
        return Collections.emptyMap();
    }

    public RoleDescriptor roleDescriptor(String role) {
        return RESERVED_ROLES.get(role);
    }

    public Collection<RoleDescriptor> roleDescriptors() {
        return RESERVED_ROLES.values();
    }

    public static Set<String> names() {
        return RESERVED_ROLES.keySet();
    }

    public static boolean isReserved(String role) {
        return RESERVED_ROLES.containsKey(role) || SystemUser.ROLE_NAME.equals(role) || XPackUser.ROLE_NAME.equals(role);
    }

}

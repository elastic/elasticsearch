/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.store;

import org.elasticsearch.xpack.security.SecurityExtension;
import org.elasticsearch.xpack.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.support.MetadataUtils;
import org.elasticsearch.xpack.security.user.UsernamesField;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

public class ClientReservedRoles {

    public static final RoleDescriptor SUPERUSER_ROLE_DESCRIPTOR = new RoleDescriptor("superuser",
            new String[] { "all" },
            new RoleDescriptor.IndicesPrivileges[] {
                    RoleDescriptor.IndicesPrivileges.builder().indices("*").privileges("all").build()},
            new String[] { "*" },
            MetadataUtils.DEFAULT_RESERVED_METADATA);
    static final Map<String, RoleDescriptor> RESERVED_ROLES = initializeReservedRoles();

    static Map<String, RoleDescriptor> initializeReservedRoles() {
                Map<String, RoleDescriptor> roles = new HashMap<>();

                roles.put("superuser", SUPERUSER_ROLE_DESCRIPTOR);

                // Services are loaded through SPI, and are defined in their META-INF/services
                for(SecurityExtension ext : ServiceLoader.load(SecurityExtension.class, SecurityExtension.class.getClassLoader())) {
                    roles.putAll(ext.getReservedRoles());
                }

                return Collections.unmodifiableMap(roles);
            }

    public static boolean isReserved(String role) {
        return RESERVED_ROLES.containsKey(role) || UsernamesField.SYSTEM_ROLE.equals(role) || UsernamesField.XPACK_ROLE.equals(role);
    }
}

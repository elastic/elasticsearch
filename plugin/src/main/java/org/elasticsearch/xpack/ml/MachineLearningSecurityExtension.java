/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.xpack.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.SecurityExtension;
import org.elasticsearch.xpack.security.support.MetadataUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MachineLearningSecurityExtension implements SecurityExtension {
    @Override
    public Map<String, RoleDescriptor> getReservedRoles() {
        Map<String, RoleDescriptor> roles = new HashMap<>();

        roles.put("machine_learning_user",
                  new RoleDescriptor("machine_learning_user",
                                     new String[] { "monitor_ml" },
                                     new RoleDescriptor.IndicesPrivileges[] {
                                         RoleDescriptor.IndicesPrivileges.builder()
                                                 .indices(".ml-anomalies*", ".ml-notifications")
                                                 .privileges("view_index_metadata", "read")
                                                 .build()
                                     },
                                     null,
                                     MetadataUtils.DEFAULT_RESERVED_METADATA));

        roles.put("machine_learning_admin",
                  new RoleDescriptor("machine_learning_admin",
                                     new String[] { "manage_ml" },
                                     new RoleDescriptor.IndicesPrivileges[] {
                                         RoleDescriptor.IndicesPrivileges.builder()
                                                 .indices(".ml-*")
                                                 .privileges("view_index_metadata", "read")
                                                 .build()
                                     },
                                     null,
                                     MetadataUtils.DEFAULT_RESERVED_METADATA));

        return Collections.unmodifiableMap(roles);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.xpack.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.support.MetadataUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class StackSecurityExtension implements SecurityExtension {
    @Override
    public Map<String, RoleDescriptor> getReservedRoles() {
        Map<String, RoleDescriptor> roles = new HashMap<>();

        roles.put("transport_client",
                  new RoleDescriptor("transport_client",
                                     new String[] { "transport_client" },
                                     null,
                                     null,
                                     MetadataUtils.DEFAULT_RESERVED_METADATA));

        roles.put("kibana_user",
                  new RoleDescriptor("kibana_user",
                                     null,
                                     new RoleDescriptor.IndicesPrivileges[] {
                                         RoleDescriptor.IndicesPrivileges.builder()
                                                 .indices(".kibana*")
                                                 .privileges("manage", "read", "index", "delete")
                                                 .build()
                                     },
                                     null,
                                     MetadataUtils.DEFAULT_RESERVED_METADATA));

        roles.put("ingest_admin",
                  new RoleDescriptor("ingest_admin",
                                     new String[] { "manage_index_templates", "manage_pipeline" },
                                     null,
                                     null,
                                     MetadataUtils.DEFAULT_RESERVED_METADATA));

        // reporting_user doesn't have any privileges in Elasticsearch, and Kibana authorizes privileges based on this role
        roles.put("reporting_user",
                  new RoleDescriptor("reporting_user",
                                     null,
                                     null,
                                     null,
                                     MetadataUtils.DEFAULT_RESERVED_METADATA));

        roles.put("kibana_dashboard_only_user",
                  new RoleDescriptor("kibana_dashboard_only_user",
                                     null,
                                     new RoleDescriptor.IndicesPrivileges[] {
                                         RoleDescriptor.IndicesPrivileges.builder()
                                                 .indices(".kibana*")
                                                 .privileges("read", "view_index_metadata")
                                                 .build()
                                     },
                                     null,
                                     MetadataUtils.DEFAULT_RESERVED_METADATA));

        return Collections.unmodifiableMap(roles);
    }
}

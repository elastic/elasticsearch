/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.store;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.xpack.monitoring.action.MonitoringBulkAction;
import org.elasticsearch.xpack.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.authz.permission.Role;
import org.elasticsearch.xpack.security.support.MetadataUtils;

import org.elasticsearch.xpack.security.user.KibanaUser;
import org.elasticsearch.xpack.security.user.SystemUser;

public class ReservedRolesStore {

    public static final RoleDescriptor SUPERUSER_ROLE_DESCRIPTOR = new RoleDescriptor("superuser", new String[] { "all" },
            new RoleDescriptor.IndicesPrivileges[] {
                    RoleDescriptor.IndicesPrivileges.builder().indices("*").privileges("all").build()},
            new String[] { "*" },
            MetadataUtils.DEFAULT_RESERVED_METADATA);
    public static final Role SUPERUSER_ROLE = Role.builder(SUPERUSER_ROLE_DESCRIPTOR, null).build();
    private static final Map<String, RoleDescriptor> RESERVED_ROLES = initializeReservedRoles();

    private static Map<String, RoleDescriptor> initializeReservedRoles() {
        return MapBuilder.<String, RoleDescriptor>newMapBuilder()
                .put("superuser", new RoleDescriptor("superuser", new String[] { "all" },
                        new RoleDescriptor.IndicesPrivileges[] {
                                RoleDescriptor.IndicesPrivileges.builder().indices("*").privileges("all").build()},
                        new String[] { "*" },
                        MetadataUtils.DEFAULT_RESERVED_METADATA))
                .put("transport_client", new RoleDescriptor("transport_client", new String[] { "transport_client" }, null, null,
                        MetadataUtils.DEFAULT_RESERVED_METADATA))
                .put("kibana_user", new RoleDescriptor("kibana_user", new String[] { "monitor" }, new RoleDescriptor.IndicesPrivileges[] {
                        RoleDescriptor.IndicesPrivileges.builder().indices(".kibana*").privileges("manage", "read", "index", "delete")
                                .build() }, null, MetadataUtils.DEFAULT_RESERVED_METADATA))
                .put("monitoring_user", new RoleDescriptor("monitoring_user", null, new RoleDescriptor.IndicesPrivileges[] {
                        RoleDescriptor.IndicesPrivileges.builder().indices(".marvel-es-*", ".monitoring-*").privileges("read").build() },
                        null, MetadataUtils.DEFAULT_RESERVED_METADATA))
                .put("remote_monitoring_agent", new RoleDescriptor("remote_monitoring_agent",
                        new String[] { "manage_index_templates", "manage_ingest_pipelines", "monitor" },
                        new RoleDescriptor.IndicesPrivileges[] {
                            RoleDescriptor.IndicesPrivileges.builder().indices(".marvel-es-*", ".monitoring-*").privileges("all").build() },
                        null, MetadataUtils.DEFAULT_RESERVED_METADATA))
                .put("ingest_admin", new RoleDescriptor("ingest_admin", new String[] { "manage_index_templates", "manage_pipeline" },
                        null, null, MetadataUtils.DEFAULT_RESERVED_METADATA))
                .put("reporting_user", new RoleDescriptor("reporting_user", null, new RoleDescriptor.IndicesPrivileges[] {
                        RoleDescriptor.IndicesPrivileges.builder().indices(".reporting-*").privileges("read", "write").build() },
                        null, MetadataUtils.DEFAULT_RESERVED_METADATA))
                .put(KibanaUser.ROLE_NAME, new RoleDescriptor(KibanaUser.ROLE_NAME, new String[] { "monitor", MonitoringBulkAction.NAME},
                        new RoleDescriptor.IndicesPrivileges[] {
                            RoleDescriptor.IndicesPrivileges.builder().indices(".kibana*", ".reporting-*").privileges("all").build() },
                        null, MetadataUtils.DEFAULT_RESERVED_METADATA))
                .put("logstash_system", new RoleDescriptor("logstash_system", new String[] { "monitor", MonitoringBulkAction.NAME},
                        null, null, MetadataUtils.DEFAULT_RESERVED_METADATA))
                .put("beats_system", new RoleDescriptor("beats_system", new String[] { "monitor", MonitoringBulkAction.NAME},
                        null, null, MetadataUtils.DEFAULT_RESERVED_METADATA))
                .immutableMap();
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
        return RESERVED_ROLES.containsKey(role) || SystemUser.ROLE_NAME.equals(role);
    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring;

import org.elasticsearch.xpack.monitoring.action.MonitoringBulkAction;
import org.elasticsearch.xpack.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.SecurityExtension;
import org.elasticsearch.xpack.security.support.MetadataUtils;
import org.elasticsearch.xpack.security.user.KibanaUser;
import org.elasticsearch.xpack.security.user.LogstashSystemUser;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MonitoringSecurityExtension implements SecurityExtension {
    @Override
    public Map<String, RoleDescriptor> getReservedRoles() {
        Map<String, RoleDescriptor> roles = new HashMap<>();

        roles.put("monitoring_user",
                  new RoleDescriptor("monitoring_user",
                                     null,
                                     new RoleDescriptor.IndicesPrivileges[] {
                                         RoleDescriptor.IndicesPrivileges.builder()
                                                 .indices(".monitoring-*")
                                                 .privileges("read", "read_cross_cluster")
                                                 .build()
                                     },
                                     null,
                                     MetadataUtils.DEFAULT_RESERVED_METADATA));

        roles.put("remote_monitoring_agent",
                  new RoleDescriptor("remote_monitoring_agent",
                                     new String[] {
                                          "manage_index_templates", "manage_ingest_pipelines", "monitor",
                                          "cluster:monitor/xpack/watcher/watch/get",
                                          "cluster:admin/xpack/watcher/watch/put",
                                          "cluster:admin/xpack/watcher/watch/delete",
                                     },
                                     new RoleDescriptor.IndicesPrivileges[] {
                                         RoleDescriptor.IndicesPrivileges.builder()
                                                 .indices(".monitoring-*")
                                                 .privileges("all")
                                                 .build()
                                     },
                                     null,
                                     MetadataUtils.DEFAULT_RESERVED_METADATA));

        // TODO(core-infra) put KibanaUser & LogstashSystemUser into a common place for the split and use them here
        roles.put("logstash_system",
                  new RoleDescriptor(LogstashSystemUser.ROLE_NAME,
                                     new String[]{"monitor", MonitoringBulkAction.NAME},
                                     null,
                                     null,
                                     MetadataUtils.DEFAULT_RESERVED_METADATA));

        roles.put("kibana_system",
                new RoleDescriptor(KibanaUser.ROLE_NAME,
                                     new String[] { "monitor", "manage_index_templates", MonitoringBulkAction.NAME },
                                     new RoleDescriptor.IndicesPrivileges[] {
                                         RoleDescriptor.IndicesPrivileges.builder()
                                                 .indices(".kibana*", ".reporting-*")
                                                 .privileges("all")
                                                 .build(),
                                         RoleDescriptor.IndicesPrivileges.builder()
                                                 .indices(".monitoring-*")
                                                 .privileges("read", "read_cross_cluster")
                                                 .build()
                                     },
                                     null,
                                     MetadataUtils.DEFAULT_RESERVED_METADATA));

        return Collections.unmodifiableMap(roles);
    }
}

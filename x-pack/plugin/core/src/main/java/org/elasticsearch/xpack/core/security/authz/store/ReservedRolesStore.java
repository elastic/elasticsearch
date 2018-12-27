/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authz.store;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkAction;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.privilege.ConditionalClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConditionalClusterPrivileges.ManageApplicationPrivileges;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;
import org.elasticsearch.xpack.core.security.user.KibanaUser;
import org.elasticsearch.xpack.core.security.user.UsernamesField;
import org.elasticsearch.xpack.core.watcher.execution.TriggeredWatchStoreField;
import org.elasticsearch.xpack.core.watcher.history.HistoryStoreField;
import org.elasticsearch.xpack.core.watcher.watch.Watch;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class ReservedRolesStore implements BiConsumer<Set<String>, ActionListener<RoleRetrievalResult>> {

    public static final RoleDescriptor SUPERUSER_ROLE_DESCRIPTOR = new RoleDescriptor("superuser",
            new String[] { "all" },
            new RoleDescriptor.IndicesPrivileges[] {
                    RoleDescriptor.IndicesPrivileges.builder().indices("*").privileges("all").build()},
            new RoleDescriptor.ApplicationResourcePrivileges[] {
                RoleDescriptor.ApplicationResourcePrivileges.builder().application("*").privileges("*").resources("*").build()
            },
            null, new String[] { "*" },
            MetadataUtils.DEFAULT_RESERVED_METADATA, Collections.emptyMap());
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
                .put("kibana_user", new RoleDescriptor("kibana_user", null, new RoleDescriptor.IndicesPrivileges[] {
                        RoleDescriptor.IndicesPrivileges.builder().indices(".kibana*").privileges("manage", "read", "index", "delete")
                            .build() }, new RoleDescriptor.ApplicationResourcePrivileges[] {
                        RoleDescriptor.ApplicationResourcePrivileges.builder()
                            .application("kibana-.kibana").resources("*").privileges("all").build() },
                        null, null,
                        MetadataUtils.DEFAULT_RESERVED_METADATA, null))
                .put("monitoring_user", new RoleDescriptor("monitoring_user",
                        new String[] { "cluster:monitor/main" },
                        new RoleDescriptor.IndicesPrivileges[] {
                            RoleDescriptor.IndicesPrivileges.builder()
                                .indices(".monitoring-*").privileges("read", "read_cross_cluster").build()
                },
                        null, MetadataUtils.DEFAULT_RESERVED_METADATA))
                .put("remote_monitoring_agent", new RoleDescriptor("remote_monitoring_agent",
                        new String[] {
                                "manage_index_templates", "manage_ingest_pipelines", "monitor",
                                "cluster:monitor/xpack/watcher/watch/get",
                                "cluster:admin/xpack/watcher/watch/put",
                                "cluster:admin/xpack/watcher/watch/delete",
                        },
                        new RoleDescriptor.IndicesPrivileges[] {
                                RoleDescriptor.IndicesPrivileges.builder().indices(".monitoring-*").privileges("all").build(),
                                RoleDescriptor.IndicesPrivileges.builder()
                                    .indices("metricbeat-*").privileges("index", "create_index").build() },
                        null, MetadataUtils.DEFAULT_RESERVED_METADATA))
                .put("remote_monitoring_collector", new RoleDescriptor(
                        "remote_monitoring_collector",
                        new String[] {
                            "monitor"
                        },
                        new RoleDescriptor.IndicesPrivileges[] {
                            RoleDescriptor.IndicesPrivileges.builder().indices("*").privileges("monitor").build(),
                            RoleDescriptor.IndicesPrivileges.builder().indices(".kibana*").privileges("read").build()
                        },
                        null,
                        null,
                        null,
                        MetadataUtils.DEFAULT_RESERVED_METADATA,
                        null
                ))
                .put("ingest_admin", new RoleDescriptor("ingest_admin", new String[] { "manage_index_templates", "manage_pipeline" },
                        null, null, MetadataUtils.DEFAULT_RESERVED_METADATA))
                // reporting_user doesn't have any privileges in Elasticsearch, and Kibana authorizes privileges based on this role
                .put("reporting_user", new RoleDescriptor("reporting_user", null, null,
                        null, MetadataUtils.DEFAULT_RESERVED_METADATA))
                .put("kibana_dashboard_only_user", new RoleDescriptor(
                        "kibana_dashboard_only_user",
                        null,
                        new RoleDescriptor.IndicesPrivileges[] {
                            RoleDescriptor.IndicesPrivileges.builder()
                                .indices(".kibana*").privileges("read", "view_index_metadata").build()
                        },
                        new RoleDescriptor.ApplicationResourcePrivileges[] {
                            RoleDescriptor.ApplicationResourcePrivileges.builder()
                            .application("kibana-.kibana").resources("*").privileges("read").build() },
                        null, null,
                        MetadataUtils.DEFAULT_RESERVED_METADATA,
                        null))
                .put(KibanaUser.ROLE_NAME, new RoleDescriptor(KibanaUser.ROLE_NAME,
                        new String[] {
                            "monitor", "manage_index_templates", MonitoringBulkAction.NAME, "manage_saml", "manage_token"
                        },
                        new RoleDescriptor.IndicesPrivileges[] {
                                RoleDescriptor.IndicesPrivileges.builder().indices(".kibana*", ".reporting-*").privileges("all").build(),
                                RoleDescriptor.IndicesPrivileges.builder()
                                        .indices(".monitoring-*").privileges("read", "read_cross_cluster").build(),
                                RoleDescriptor.IndicesPrivileges.builder()
                                        .indices(".management-beats").privileges("create_index", "read", "write").build()
                        },
                        null,
                        new ConditionalClusterPrivilege[] { new ManageApplicationPrivileges(Collections.singleton("kibana-*")) },
                        null, MetadataUtils.DEFAULT_RESERVED_METADATA, null))
                .put("logstash_system", new RoleDescriptor("logstash_system", new String[] { "monitor", MonitoringBulkAction.NAME},
                        null, null, MetadataUtils.DEFAULT_RESERVED_METADATA))
                .put("beats_admin", new RoleDescriptor("beats_admin",
                    null,
                    new RoleDescriptor.IndicesPrivileges[] {
                        RoleDescriptor.IndicesPrivileges.builder().indices(".management-beats").privileges("all").build()
                    },
                    null, MetadataUtils.DEFAULT_RESERVED_METADATA))
                .put(UsernamesField.BEATS_ROLE, new RoleDescriptor(UsernamesField.BEATS_ROLE,
                        new String[] { "monitor", MonitoringBulkAction.NAME}, null, null, MetadataUtils.DEFAULT_RESERVED_METADATA))
                .put(UsernamesField.APM_ROLE, new RoleDescriptor(UsernamesField.APM_ROLE,
                        new String[] { "monitor", MonitoringBulkAction.NAME}, null, null, MetadataUtils.DEFAULT_RESERVED_METADATA))
                .put("machine_learning_user", new RoleDescriptor("machine_learning_user", new String[] { "monitor_ml" },
                        new RoleDescriptor.IndicesPrivileges[] {
                                RoleDescriptor.IndicesPrivileges.builder().indices(".ml-anomalies*", ".ml-notifications*")
                                        .privileges("view_index_metadata", "read").build(),
                                RoleDescriptor.IndicesPrivileges.builder().indices(".ml-annotations*")
                                        .privileges("view_index_metadata", "read", "write").build()
                        },
                        null, MetadataUtils.DEFAULT_RESERVED_METADATA))
                .put("machine_learning_admin", new RoleDescriptor("machine_learning_admin", new String[] { "manage_ml" },
                        new RoleDescriptor.IndicesPrivileges[] {
                                RoleDescriptor.IndicesPrivileges.builder()
                                        .indices(".ml-anomalies*", ".ml-notifications*", ".ml-state*", ".ml-meta*")
                                        .privileges("view_index_metadata", "read").build(),
                                RoleDescriptor.IndicesPrivileges.builder().indices(".ml-annotations*")
                                        .privileges("view_index_metadata", "read", "write").build()
                        },
                        null, MetadataUtils.DEFAULT_RESERVED_METADATA))
                .put("watcher_admin", new RoleDescriptor("watcher_admin", new String[] { "manage_watcher" },
                        new RoleDescriptor.IndicesPrivileges[] {
                                RoleDescriptor.IndicesPrivileges.builder().indices(Watch.INDEX, TriggeredWatchStoreField.INDEX_NAME,
                                        HistoryStoreField.INDEX_PREFIX + "*").privileges("read").build() },
                        null, MetadataUtils.DEFAULT_RESERVED_METADATA))
                .put("watcher_user", new RoleDescriptor("watcher_user", new String[] { "monitor_watcher" },
                        new RoleDescriptor.IndicesPrivileges[] {
                                RoleDescriptor.IndicesPrivileges.builder().indices(Watch.INDEX)
                                        .privileges("read")
                                        .build(),
                                RoleDescriptor.IndicesPrivileges.builder().indices(HistoryStoreField.INDEX_PREFIX + "*")
                                        .privileges("read")
                                        .build() }, null, MetadataUtils.DEFAULT_RESERVED_METADATA))
                .put("logstash_admin", new RoleDescriptor("logstash_admin", null, new RoleDescriptor.IndicesPrivileges[] {
                        RoleDescriptor.IndicesPrivileges.builder().indices(".logstash*")
                                .privileges("create", "delete", "index", "manage", "read").build() },
                        null, MetadataUtils.DEFAULT_RESERVED_METADATA))
                .put("rollup_user", new RoleDescriptor("rollup_user", new String[] { "monitor_rollup" },
                        null, null, MetadataUtils.DEFAULT_RESERVED_METADATA))
                .put("rollup_admin", new RoleDescriptor("rollup_admin", new String[] { "manage_rollup" },
                        null, null, MetadataUtils.DEFAULT_RESERVED_METADATA))
                .immutableMap();
    }

    public static boolean isReserved(String role) {
        return RESERVED_ROLES.containsKey(role) || UsernamesField.SYSTEM_ROLE.equals(role) || UsernamesField.XPACK_ROLE.equals(role);
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

    @Override
    public void accept(Set<String> roleNames, ActionListener<RoleRetrievalResult> listener) {
        final Set<RoleDescriptor> descriptors = roleNames.stream()
            .map(RESERVED_ROLES::get)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
        listener.onResponse(RoleRetrievalResult.success(descriptors));
    }

    @Override
    public String toString() {
        return "reserved roles store";
    }
}

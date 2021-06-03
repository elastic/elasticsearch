/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.core.ilm.action.GetLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleAction;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkAction;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.privilege.GetBuiltinPrivilegesAction;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

final class ElasticServiceAccounts {

    static final String NAMESPACE = "elastic";

    private static final ServiceAccount FLEET_ACCOUNT = new ElasticServiceAccount("fleet-server",
        new RoleDescriptor(
            NAMESPACE + "/fleet-server",
            new String[]{"monitor", "manage_own_api_key"},
            new RoleDescriptor.IndicesPrivileges[]{
                RoleDescriptor.IndicesPrivileges
                    .builder()
                    .indices("logs-*", "metrics-*", "traces-*", "synthetics-*", ".logs-endpoint.diagnostic.collection-*")
                    .privileges("write", "create_index", "auto_configure")
                    .build(),
                RoleDescriptor.IndicesPrivileges
                    .builder()
                    .indices(".fleet-*")
                    .privileges("read", "write", "monitor", "create_index", "auto_configure")
                    .build()
            },
            null,
            null,
            null,
            null,
            null
        ));

    private static final ServiceAccount KIBANA_ACCOUNT = new ElasticServiceAccount("kibana-system",
        new RoleDescriptor(
            NAMESPACE + "/kibana-system",
            new String[] {
                "monitor", "manage_index_templates", MonitoringBulkAction.NAME, "manage_saml", "manage_token", "manage_oidc",
                InvalidateApiKeyAction.NAME, "grant_api_key",
                GetBuiltinPrivilegesAction.NAME, "delegate_pki", GetLifecycleAction.NAME,  PutLifecycleAction.NAME,
                // To facilitate ML UI functionality being controlled using Kibana security privileges
                "manage_ml",
                // The symbolic constant for this one is in SecurityActionMapper, so not accessible from X-Pack core
                "cluster:admin/analyze",
                // To facilitate using the file uploader functionality
                "monitor_text_structure",
                // To cancel tasks and delete async searches
                "cancel_task"
            },
            new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".kibana*", ".reporting-*").privileges("all").build(),
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".monitoring-*").privileges("read", "read_cross_cluster").build(),
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".management-beats").privileges("create_index", "read", "write").build(),
                // To facilitate ML UI functionality being controlled using Kibana security privileges
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".ml-anomalies*", ".ml-notifications*", ".ml-stats-*")
                    .privileges("read").build(),
                RoleDescriptor.IndicesPrivileges.builder().indices(".ml-annotations*")
                    .privileges("read", "write").build(),
                // APM agent configuration
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".apm-agent-configuration").privileges("all").build(),
                // APM custom link index creation
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".apm-custom-link").privileges("all").build(),
                // APM telemetry queries APM indices in kibana task runner
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("apm-*")
                    .privileges("read", "read_cross_cluster").build(),
                // Data telemetry reads mappings, metadata and stats of indices
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("*")
                    .privileges("view_index_metadata", "monitor").build(),
                // Endpoint diagnostic information. Kibana reads from these indices to send telemetry
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".logs-endpoint.diagnostic.collection-*")
                    .privileges("read").build(),
                // Fleet Server indices. Kibana create this indice before Fleet Server use them.
                // Fleet Server indices. Kibana read and write to this indice to manage Elastic Agents
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".fleet*")
                    .privileges("all").build(),
                // Legacy "Alerts as data" index. Kibana user will create this index.
                // Kibana user will read / write to these indices
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(ReservedRolesStore.LEGACY_ALERTS_INDEX)
                    .privileges("all").build(),
                // "Alerts as data" index. Kibana user will create this index.
                // Kibana user will read / write to these indices
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(ReservedRolesStore.ALERTS_INDEX)
                    .privileges("all").build()
            },
            null,
            new ConfigurableClusterPrivilege[] { new
                ConfigurableClusterPrivileges.ManageApplicationPrivileges(Collections.singleton("kibana-*")) },
            null,
            MetadataUtils.DEFAULT_RESERVED_METADATA,
            null
        ));

    static final Map<String, ServiceAccount> ACCOUNTS = List.of(FLEET_ACCOUNT, KIBANA_ACCOUNT).stream()
        .collect(Collectors.toMap(a -> a.id().asPrincipal(), Function.identity()));;

    private ElasticServiceAccounts() {}

    static class ElasticServiceAccount implements ServiceAccount {
        private final ServiceAccountId id;
        private final RoleDescriptor roleDescriptor;
        private final User user;

        ElasticServiceAccount(String serviceName, RoleDescriptor roleDescriptor) {
            this.id = new ServiceAccountId(NAMESPACE, serviceName);
            this.roleDescriptor = Objects.requireNonNull(roleDescriptor, "Role descriptor cannot be null");
            if (roleDescriptor.getName().equals(id.asPrincipal()) == false) {
                throw new IllegalArgumentException("the provided role descriptor [" + roleDescriptor.getName()
                    + "] must have the same name as the service account [" + id.asPrincipal() + "]");
            }
            this.user = new User(id.asPrincipal(), Strings.EMPTY_ARRAY, "Service account - " + id, null,
                Map.of("_elastic_service_account", true),
                true);
        }

        @Override
        public ServiceAccountId id() {
            return id;
        }

        @Override
        public RoleDescriptor roleDescriptor() {
            return roleDescriptor;
        }

        @Override
        public User asUser() {
            return user;
        }
    }
}

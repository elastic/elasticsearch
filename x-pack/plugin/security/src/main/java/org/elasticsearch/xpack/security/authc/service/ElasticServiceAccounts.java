/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class ElasticServiceAccounts {

    static final String NAMESPACE = "elastic";

    private static final ServiceAccount ENTERPRISE_SEARCH_ACCOUNT = new ElasticServiceAccount(
        "enterprise-search-server",
        new RoleDescriptor(
            NAMESPACE + "/enterprise-search-server",
            new String[] { "manage", "manage_security", "read_connector_secrets", "write_connector_secrets" },
            new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(
                        "search-*",
                        ".search-acl-filter-*",
                        ".elastic-analytics-collections",
                        ".ent-search-*",
                        ".monitoring-ent-search-*",
                        "metricbeat-ent-search-*",
                        "enterprise-search-*",
                        "logs-app_search.analytics-default",
                        "logs-elastic_analytics.events-*",
                        "logs-enterprise_search.api-default",
                        "logs-enterprise_search.audit-default",
                        "logs-app_search.search_relevance_suggestions-default",
                        "logs-crawler-default",
                        "logs-elastic_crawler-default",
                        "logs-workplace_search.analytics-default",
                        "logs-workplace_search.content_events-default",
                        ".elastic-connectors*"
                    )
                    .privileges("manage", "read", "write")
                    .build() },
            null,
            null,
            null,
            null,
            null
        )
    );

    private static final ServiceAccount FLEET_ACCOUNT = new ElasticServiceAccount(
        "fleet-server",
        new RoleDescriptor(
            NAMESPACE + "/fleet-server",
            new String[] { "monitor", "manage_own_api_key", "read_fleet_secrets" },
            new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(
                        "logs-*",
                        "metrics-*",
                        "traces-*",
                        ".logs-endpoint.diagnostic.collection-*",
                        ".logs-endpoint.action.responses-*",
                        ".logs-endpoint.heartbeat-*"
                    )
                    .privileges("write", "create_index", "auto_configure")
                    .build(),
                RoleDescriptor.IndicesPrivileges.builder().indices("profiling-*").privileges("read", "write").build(),
                RoleDescriptor.IndicesPrivileges.builder()
                    // APM Server (and hence Fleet Server, which issues its API Keys) needs additional privileges
                    // for the non-sensitive "sampled traces" data stream:
                    // - "maintenance" privilege to refresh indices
                    // - "monitor" privilege to be able to query index stats for the global checkpoint
                    // - "read" privilege to search the documents
                    .indices("traces-apm.sampled-*")
                    .privileges("read", "monitor", "maintenance")
                    .build(),
                // Fleet secrets. Fleet Server can only read from this index.
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".fleet-secrets*")
                    .privileges("read")
                    .allowRestrictedIndices(true)
                    .build(),
                // Other Fleet indices. Fleet Server needs "maintenance" privilege to be able to perform operations with "refresh".
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".fleet-actions*")
                    .privileges("read", "write", "monitor", "create_index", "auto_configure", "maintenance")
                    .allowRestrictedIndices(true)
                    .build(),
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".fleet-agents*")
                    .privileges("read", "write", "monitor", "create_index", "auto_configure", "maintenance")
                    .allowRestrictedIndices(true)
                    .build(),
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".fleet-artifacts*")
                    .privileges("read", "write", "monitor", "create_index", "auto_configure", "maintenance")
                    .allowRestrictedIndices(true)
                    .build(),
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".fleet-enrollment-api-keys*")
                    .privileges("read", "write", "monitor", "create_index", "auto_configure", "maintenance")
                    .allowRestrictedIndices(true)
                    .build(),
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".fleet-policies*")
                    .privileges("read", "write", "monitor", "create_index", "auto_configure", "maintenance")
                    .allowRestrictedIndices(true)
                    .build(),
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".fleet-policies-leader*")
                    .privileges("read", "write", "monitor", "create_index", "auto_configure", "maintenance")
                    .allowRestrictedIndices(true)
                    .build(),
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".fleet-servers*")
                    .privileges("read", "write", "monitor", "create_index", "auto_configure", "maintenance")
                    .allowRestrictedIndices(true)
                    .build(),
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".fleet-fileds*")
                    .privileges("read", "write", "monitor", "create_index", "auto_configure", "maintenance")
                    .allowRestrictedIndices(true)
                    .build(),
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("synthetics-*")
                    // Fleet Server needs "read" privilege to be able to retrieve multi-agent docs
                    .privileges("read", "write", "create_index", "auto_configure")
                    .allowRestrictedIndices(false)
                    .build() },
            new RoleDescriptor.ApplicationResourcePrivileges[] {
                RoleDescriptor.ApplicationResourcePrivileges.builder()
                    .application("kibana-*")
                    .resources("*")
                    .privileges("reserved_fleet-setup")
                    .build() },
            null,
            null,
            null,
            null
        )
    );
    private static final ServiceAccount FLEET_REMOTE_ACCOUNT = new ElasticServiceAccount(
        "fleet-server-remote",
        new RoleDescriptor(
            NAMESPACE + "/fleet-server-remote",
            new String[] { "monitor", "manage_own_api_key" },
            new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("logs-*", "metrics-*", "traces-*")
                    .privileges("write", "create_index", "auto_configure")
                    .build(), },
            null,
            null,
            null,
            null,
            null
        )
    );
    private static final ServiceAccount KIBANA_SYSTEM_ACCOUNT = new ElasticServiceAccount(
        "kibana",
        ReservedRolesStore.kibanaSystemRoleDescriptor(NAMESPACE + "/kibana")
    );

    static final Map<String, ServiceAccount> ACCOUNTS = Stream.of(
        ENTERPRISE_SEARCH_ACCOUNT,
        FLEET_ACCOUNT,
        FLEET_REMOTE_ACCOUNT,
        KIBANA_SYSTEM_ACCOUNT
    ).collect(Collectors.toMap(a -> a.id().asPrincipal(), Function.identity()));

    private ElasticServiceAccounts() {}

    static class ElasticServiceAccount implements ServiceAccount {
        private final ServiceAccountId id;
        private final RoleDescriptor roleDescriptor;
        private final User user;

        ElasticServiceAccount(String serviceName, RoleDescriptor roleDescriptor) {
            this.id = new ServiceAccountId(NAMESPACE, serviceName);
            this.roleDescriptor = Objects.requireNonNull(roleDescriptor, "Role descriptor cannot be null");
            if (roleDescriptor.getName().equals(id.asPrincipal()) == false) {
                throw new IllegalArgumentException(
                    "the provided role descriptor ["
                        + roleDescriptor.getName()
                        + "] must have the same name as the service account ["
                        + id.asPrincipal()
                        + "]"
                );
            }
            this.user = new User(
                id.asPrincipal(),
                Strings.EMPTY_ARRAY,
                "Service account - " + id,
                null,
                Map.of("_elastic_service_account", true),
                true
            );
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

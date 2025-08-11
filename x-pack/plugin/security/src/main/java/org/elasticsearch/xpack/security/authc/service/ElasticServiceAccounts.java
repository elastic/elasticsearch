/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.List;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

final class ElasticServiceAccounts {

    static final String NAMESPACE = "elastic";

    private static final ServiceAccount FLEET_ACCOUNT = new ElasticServiceAccount(
        "fleet-server",
        new RoleDescriptor(
            NAMESPACE + "/fleet-server",
            new String[] { "monitor", "manage_own_api_key" },
            new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(
                        "logs-*",
                        "metrics-*",
                        "traces-*",
                        "synthetics-*",
                        ".logs-endpoint.diagnostic.collection-*",
                        ".logs-endpoint.action.responses-*"
                    )
                    .privileges("write", "create_index", "auto_configure")
                    .build(),
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".fleet-*")
                    .privileges("read", "write", "monitor", "create_index", "auto_configure", "maintenance")
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
    private static final ServiceAccount KIBANA_SYSTEM_ACCOUNT = new ElasticServiceAccount(
        "kibana",
        ReservedRolesStore.kibanaSystemRoleDescriptor(NAMESPACE + "/kibana")
    );

    static final Map<String, ServiceAccount> ACCOUNTS = List.of(FLEET_ACCOUNT, KIBANA_SYSTEM_ACCOUNT)
        .stream()
        .collect(Collectors.toMap(a -> a.id().asPrincipal(), Function.identity()));

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
                org.elasticsearch.core.Map.of("_elastic_service_account", true),
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

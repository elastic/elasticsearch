/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.store;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkAction;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.privilege.GetBuiltinPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.profile.ActivateProfileAction;
import org.elasticsearch.xpack.core.security.action.profile.GetProfilesAction;
import org.elasticsearch.xpack.core.security.action.profile.SuggestProfilesAction;
import org.elasticsearch.xpack.core.security.action.user.ProfileHasPrivilegesAction;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;

import java.util.Set;

import static org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore.getRemoteIndicesReadPrivileges;

/**
 * This exists in a separate file so it can be assigned to the Kibana security team in the CODEOWNERS file
 */
class KibanaOwnedReservedRoleDescriptors {
    static RoleDescriptor kibanaSystemRoleDescriptor(String name) {
        return new RoleDescriptor(
            name,
            new String[] {
                "monitor",
                "manage_index_templates",
                MonitoringBulkAction.NAME,
                "manage_saml",
                "manage_token",
                "manage_oidc",
                // For Fleet package upgrade
                "manage_pipeline",
                "manage_ilm",
                // For the endpoint package that ships a transform
                "manage_transform",
                InvalidateApiKeyAction.NAME,
                "grant_api_key",
                "manage_own_api_key",
                GetBuiltinPrivilegesAction.NAME,
                "delegate_pki",
                GetProfilesAction.NAME,
                ActivateProfileAction.NAME,
                SuggestProfilesAction.NAME,
                ProfileHasPrivilegesAction.NAME,
                // To facilitate ML UI functionality being controlled using Kibana security privileges
                "manage_ml",
                // The symbolic constant for this one is in SecurityActionMapper, so not accessible from X-Pack core
                "cluster:admin/analyze",
                // To facilitate using the file uploader functionality
                "monitor_text_structure",
                // To cancel tasks and delete async searches
                "cancel_task" },
            new RoleDescriptor.IndicesPrivileges[] {
                // System indices defined in KibanaPlugin
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".kibana*", ".reporting-*")
                    .privileges("all")
                    .allowRestrictedIndices(true)
                    .build(),
                RoleDescriptor.IndicesPrivileges.builder().indices(".monitoring-*").privileges("read", "read_cross_cluster").build(),
                RoleDescriptor.IndicesPrivileges.builder().indices(".management-beats").privileges("create_index", "read", "write").build(),
                // To facilitate ML UI functionality being controlled using Kibana security privileges
                RoleDescriptor.IndicesPrivileges.builder().indices(".ml-anomalies*", ".ml-stats-*").privileges("read").build(),
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".ml-annotations*", ".ml-notifications*")
                    .privileges("read", "write")
                    .build(),

                // APM agent configuration - system index defined in KibanaPlugin
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".apm-agent-configuration")
                    .privileges("all")
                    .allowRestrictedIndices(true)
                    .build(),

                // APM custom link index creation - system index defined in KibanaPlugin
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".apm-custom-link")
                    .privileges("all")
                    .allowRestrictedIndices(true)
                    .build(),

                // APM source map index creation - system index defined in KibanaPlugin
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".apm-source-map")
                    .privileges("all")
                    .allowRestrictedIndices(true)
                    .build(),

                // APM telemetry queries APM indices in kibana task runner
                RoleDescriptor.IndicesPrivileges.builder().indices("apm-*").privileges("read", "read_cross_cluster").build(),
                RoleDescriptor.IndicesPrivileges.builder().indices("logs-apm.*").privileges("read", "read_cross_cluster").build(),
                RoleDescriptor.IndicesPrivileges.builder().indices("metrics-apm.*").privileges("read", "read_cross_cluster").build(),
                RoleDescriptor.IndicesPrivileges.builder().indices("traces-apm.*").privileges("read", "read_cross_cluster").build(),
                RoleDescriptor.IndicesPrivileges.builder().indices("traces-apm-*").privileges("read", "read_cross_cluster").build(),

                // Data telemetry reads mappings, metadata and stats of indices
                RoleDescriptor.IndicesPrivileges.builder().indices("*").privileges("view_index_metadata", "monitor").build(),
                // Endpoint diagnostic information. Kibana reads from these indices to send telemetry
                RoleDescriptor.IndicesPrivileges.builder().indices(".logs-endpoint.diagnostic.collection-*").privileges("read").build(),
                // Fleet secrets, Kibana can only write to this index.
                // This definition must come before .fleet* below.
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".fleet-secrets*")
                    .privileges("write", "delete", "create_index")
                    .allowRestrictedIndices(true)
                    .build(),
                // Fleet Server indices. Kibana create this indice before Fleet Server use them.
                // Fleet Server indices. Kibana read and write to this indice to manage Elastic Agents
                RoleDescriptor.IndicesPrivileges.builder().indices(".fleet*").allowRestrictedIndices(true).privileges("all").build(),
                // Fleet telemetry queries Agent Logs indices in kibana task runner
                RoleDescriptor.IndicesPrivileges.builder().indices("logs-elastic_agent*").privileges("read").build(),
                // Legacy "Alerts as data" used in Security Solution.
                // Kibana user creates these indices; reads / writes to them.
                RoleDescriptor.IndicesPrivileges.builder().indices(ReservedRolesStore.ALERTS_LEGACY_INDEX).privileges("all").build(),
                // Used in Security Solution for value lists.
                // Kibana user creates these indices; reads / writes to them.
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(ReservedRolesStore.LISTS_INDEX, ReservedRolesStore.LISTS_ITEMS_INDEX)
                    .privileges("all")
                    .build(),
                // "Alerts as data" internal backing indices used in Security Solution, Observability, etc.
                // Kibana system user creates these indices; reads / writes to them via the aliases (see below).
                RoleDescriptor.IndicesPrivileges.builder().indices(ReservedRolesStore.ALERTS_BACKING_INDEX).privileges("all").build(),
                // "Alerts as data" public index aliases used in Security Solution, Observability, etc.
                // Kibana system user uses them to read / write alerts.
                RoleDescriptor.IndicesPrivileges.builder().indices(ReservedRolesStore.ALERTS_INDEX_ALIAS).privileges("all").build(),
                // "Alerts as data" public index alias used in Security Solution
                // Kibana system user uses them to read / write alerts.
                RoleDescriptor.IndicesPrivileges.builder().indices(ReservedRolesStore.PREVIEW_ALERTS_INDEX_ALIAS).privileges("all").build(),
                // "Alerts as data" internal backing indices used in Security Solution
                // Kibana system user creates these indices; reads / writes to them via the aliases (see below).
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(ReservedRolesStore.PREVIEW_ALERTS_BACKING_INDEX_ALIAS)
                    .privileges("all")
                    .build(),
                // Endpoint / Fleet policy responses. Kibana requires read access to send telemetry
                RoleDescriptor.IndicesPrivileges.builder().indices("metrics-endpoint.policy-*").privileges("read").build(),
                // Endpoint metrics. Kibana requires read access to send telemetry
                RoleDescriptor.IndicesPrivileges.builder().indices("metrics-endpoint.metrics-*").privileges("read").build(),
                // Endpoint events. Kibana reads endpoint alert lineage for building and sending telemetry
                RoleDescriptor.IndicesPrivileges.builder().indices("logs-endpoint.events.*").privileges("read").build(),
                // Fleet package install and upgrade
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(
                        "logs-*",
                        "synthetics-*",
                        "traces-*",
                        "/metrics-.*&~(metrics-endpoint\\.metadata_current_default.*)/",
                        ".logs-endpoint.action.responses-*",
                        ".logs-endpoint.diagnostic.collection-*",
                        ".logs-endpoint.actions-*",
                        ".logs-osquery_manager.actions-*",
                        ".logs-osquery_manager.action.responses-*",
                        "profiling-*"
                    )
                    .privileges(UpdateSettingsAction.NAME, PutMappingAction.NAME, RolloverAction.NAME)
                    .build(),
                // Endpoint specific action responses. Kibana reads from these to display responses to the user.
                RoleDescriptor.IndicesPrivileges.builder().indices(".logs-endpoint.action.responses-*").privileges("read").build(),
                // Endpoint specific actions. Kibana reads and writes to this index to track new actions and display them.
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".logs-endpoint.actions-*")
                    .privileges("auto_configure", "read", "write")
                    .build(),
                // Osquery manager specific action responses. Kibana reads from these to display responses to the user.
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".logs-osquery_manager.action.responses-*")
                    .privileges("auto_configure", "create_index", "read", "index", "delete")
                    .build(),
                // Osquery manager specific actions. Kibana reads and writes to this index to track new actions and display them.
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".logs-osquery_manager.actions-*")
                    .privileges("auto_configure", "create_index", "read", "index", "write", "delete")
                    .build(),
                // For ILM policy for APM, Endpoint, & Synthetics packages that have delete action
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(
                        ".logs-endpoint.diagnostic.collection-*",
                        "logs-apm-*",
                        "logs-apm.*-*",
                        "metrics-apm-*",
                        "metrics-apm.*-*",
                        "traces-apm-*",
                        "traces-apm.*-*",
                        "synthetics-http-*",
                        "synthetics-icmp-*",
                        "synthetics-tcp-*",
                        "synthetics-browser-*",
                        "synthetics-browser.network-*",
                        "synthetics-browser.screenshot-*"
                    )
                    .privileges(DeleteIndexAction.NAME)
                    .build(),
                // For src/dest indices of the Endpoint package that ships a transform
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("metrics-endpoint.metadata*")
                    .privileges("read", "view_index_metadata")
                    .build(),
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(
                        "metrics-endpoint.metadata_current_default*",
                        ".metrics-endpoint.metadata_current_default*",
                        ".metrics-endpoint.metadata_united_default*"
                    )
                    .privileges("create_index", "delete_index", "read", "index", IndicesAliasesAction.NAME, UpdateSettingsAction.NAME)
                    .build(),
                // For destination indices of the Threat Intel (ti_*) packages that ships a transform for supporting IOC expiration
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("logs-ti_*_latest.*")
                    .privileges(
                        // Require "create_index", "delete_index", "read", "index", "delete", IndicesAliasesAction.NAME, and
                        // UpdateSettingsAction.NAME for transform
                        "create_index",
                        "delete_index",
                        "read",
                        "index",
                        "delete",
                        IndicesAliasesAction.NAME,
                        UpdateSettingsAction.NAME
                    )
                    .build(),
                // For source indices of the Threat Intel (ti_*) packages that ships a transform for supporting IOC expiration
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("logs-ti_*.*-*")
                    .privileges(
                        // Require "delete_index" to perform ILM policy actions
                        DeleteIndexAction.NAME,
                        // Require "read" and "view_index_metadata" for transform
                        "read",
                        "view_index_metadata"
                    )
                    .build(),
                // For src/dest indices of the example transform package
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("kibana_sample_data_*")
                    .privileges(
                        "create_index",
                        "delete_index",
                        "read",
                        "index",
                        "view_index_metadata",
                        IndicesAliasesAction.NAME,
                        UpdateSettingsAction.NAME
                    )
                    .build(),
                // For src/dest indices of the Cloud Security Posture packages that ships a transform
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("logs-cloud_security_posture.findings-*", "logs-cloud_security_posture.vulnerabilities-*")
                    .privileges("read", "view_index_metadata")
                    .build(),
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(
                        "logs-cloud_security_posture.findings_latest-default*",
                        "logs-cloud_security_posture.scores-default*",
                        "logs-cloud_security_posture.vulnerabilities_latest-default*"
                    )
                    .privileges("create_index", "read", "index", "delete", IndicesAliasesAction.NAME, UpdateSettingsAction.NAME)
                    .build(),
                RoleDescriptor.IndicesPrivileges.builder().indices("risk-score.risk-*").privileges("all").build(),
                // For cloud_defend usageCollection
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("logs-cloud_defend.*", "metrics-cloud_defend.*")
                    .privileges("read", "view_index_metadata")
                    .build() },
            null,
            new ConfigurableClusterPrivilege[] {
                new ConfigurableClusterPrivileges.ManageApplicationPrivileges(Set.of("kibana-*")),
                new ConfigurableClusterPrivileges.WriteProfileDataPrivileges(Set.of("kibana*")) },
            null,
            MetadataUtils.DEFAULT_RESERVED_METADATA,
            null,
            TcpTransport.isUntrustedRemoteClusterEnabled()
                ? new RoleDescriptor.RemoteIndicesPrivileges[] {
                    getRemoteIndicesReadPrivileges(".monitoring-*"),
                    getRemoteIndicesReadPrivileges("apm-*"),
                    getRemoteIndicesReadPrivileges("logs-apm.*"),
                    getRemoteIndicesReadPrivileges("metrics-apm.*"),
                    getRemoteIndicesReadPrivileges("traces-apm.*"),
                    getRemoteIndicesReadPrivileges("traces-apm-*") }
                : null,
            null
        );
    }
}

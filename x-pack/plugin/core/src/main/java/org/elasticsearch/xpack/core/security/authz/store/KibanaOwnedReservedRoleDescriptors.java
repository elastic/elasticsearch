/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.store;

import org.elasticsearch.action.admin.indices.alias.TransportIndicesAliasesAction;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.settings.put.TransportUpdateSettingsAction;
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

import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore.getRemoteIndicesReadPrivileges;

/**
 * This exists in a separate file so it can be assigned to the Kibana security team in the CODEOWNERS file
 */
class KibanaOwnedReservedRoleDescriptors {

    // package-private to expose to ReservedRoleStore
    static RoleDescriptor kibanaAdminUser(String name, Map<String, Object> metadata) {
        return new RoleDescriptor(
            name,
            null,
            null,
            new RoleDescriptor.ApplicationResourcePrivileges[] {
                RoleDescriptor.ApplicationResourcePrivileges.builder()
                    .application("kibana-.kibana")
                    .resources("*")
                    .privileges("all")
                    .build() },
            null,
            null,
            metadata,
            null
        );
    }

    // package-private to expose to ReservedRoleStore
    static RoleDescriptor kibanaSystem(String name) {
        return new RoleDescriptor(
            name,
            new String[] {
                "monitor",
                "manage_index_templates",
                MonitoringBulkAction.NAME,
                "manage_saml",
                "manage_token",
                "manage_oidc",
                // For SLO to install enrich policy
                "manage_enrich",
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
                "write_fleet_secrets",
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
                // Fleet secrets. Kibana can only write to this index.
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".fleet-secrets*")
                    .privileges("write", "delete", "create_index")
                    .allowRestrictedIndices(true)
                    .build(),
                // Other Fleet indices. Kibana reads and writes to these indices to manage Elastic Agents.
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".fleet-actions*")
                    .privileges("all")
                    .allowRestrictedIndices(true)
                    .build(),
                RoleDescriptor.IndicesPrivileges.builder().indices(".fleet-agents*").privileges("all").allowRestrictedIndices(true).build(),
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".fleet-artifacts*")
                    .privileges("all")
                    .allowRestrictedIndices(true)
                    .build(),
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".fleet-enrollment-api-keys*")
                    .privileges("all")
                    .allowRestrictedIndices(true)
                    .build(),
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".fleet-policies*")
                    .privileges("all")
                    .allowRestrictedIndices(true)
                    .build(),
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".fleet-policies-leader*")
                    .privileges("all")
                    .allowRestrictedIndices(true)
                    .build(),
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".fleet-servers*")
                    .privileges("all")
                    .allowRestrictedIndices(true)
                    .build(),
                RoleDescriptor.IndicesPrivileges.builder().indices(".fleet-fileds*").privileges("all").allowRestrictedIndices(true).build(),
                // 8.9 BWC
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".fleet-file-data-*")
                    .privileges("all")
                    .allowRestrictedIndices(true)
                    .build(),
                RoleDescriptor.IndicesPrivileges.builder().indices(".fleet-files-*").privileges("all").allowRestrictedIndices(true).build(),
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".fleet-filedelivery-data-*")
                    .privileges("all")
                    .allowRestrictedIndices(true)
                    .build(),
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".fleet-filedelivery-meta-*")
                    .privileges("all")
                    .allowRestrictedIndices(true)
                    .build(),
                // Fleet telemetry queries Agent Logs indices in kibana task runner
                RoleDescriptor.IndicesPrivileges.builder().indices("logs-elastic_agent*").privileges("read").build(),
                // Fleet publishes Agent metrics in kibana task runner
                RoleDescriptor.IndicesPrivileges.builder().indices("metrics-fleet_server*").privileges("all").build(),
                // Fleet reads output health from this index pattern
                RoleDescriptor.IndicesPrivileges.builder().indices("logs-fleet_server*").privileges("read", "delete_index").build(),
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
                        ".logs-endpoint.heartbeat-*",
                        ".logs-osquery_manager.actions-*",
                        ".logs-osquery_manager.action.responses-*",
                        "profiling-*"
                    )
                    .privileges(
                        TransportUpdateSettingsAction.TYPE.name(),
                        TransportPutMappingAction.TYPE.name(),
                        RolloverAction.NAME,
                        "indices:admin/data_stream/lifecycle/put"
                    )
                    .build(),
                // Endpoint specific action responses. Kibana reads and writes (for third party agents) to the index
                // to display action responses to the user.
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".logs-endpoint.action.responses-*")
                    .privileges("auto_configure", "read", "write")
                    .build(),
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

                // Third party agent (that use non-Elastic Defend integrations) info logs indices.
                // Kibana reads from these to display agent status/info to the user.
                // These are indices that filebeat writes to, and the data in these indices are ingested by Fleet integrations
                // in order to provide support for response actions related to malicious events for such agents.
                RoleDescriptor.IndicesPrivileges.builder().indices("logs-sentinel_one.*", "logs-crowdstrike.*").privileges("read").build(),
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
                    .privileges(TransportDeleteIndexAction.TYPE.name())
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
                    .privileges(
                        "create_index",
                        "delete_index",
                        "read",
                        "index",
                        TransportIndicesAliasesAction.NAME,
                        TransportUpdateSettingsAction.TYPE.name()
                    )
                    .build(),
                // For destination indices of the Threat Intel (ti_*) packages that ships a transform for supporting IOC expiration
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("logs-ti_*_latest.*")
                    .privileges(
                        "create_index",
                        "delete_index",
                        "read",
                        "index",
                        "delete",
                        "manage",
                        TransportIndicesAliasesAction.NAME,
                        TransportUpdateSettingsAction.TYPE.name()
                    )
                    .build(),
                // For source indices of the Threat Intel (ti_*) packages that ships a transform for supporting IOC expiration
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("logs-ti_*.*-*")
                    .privileges(
                        // Require "delete_index" to perform ILM policy actions
                        TransportDeleteIndexAction.TYPE.name(),
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
                        TransportIndicesAliasesAction.NAME,
                        TransportUpdateSettingsAction.TYPE.name()
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
                    .privileges(
                        "create_index",
                        "read",
                        "index",
                        "delete",
                        TransportIndicesAliasesAction.NAME,
                        TransportUpdateSettingsAction.TYPE.name()
                    )
                    .build(),
                RoleDescriptor.IndicesPrivileges.builder().indices("risk-score.risk-*").privileges("all").build(),
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".asset-criticality.asset-criticality-*")
                    .privileges("create_index", "manage", "read")
                    .build(),
                // For cloud_defend usageCollection
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("logs-cloud_defend.*", "metrics-cloud_defend.*")
                    .privileges("read", "view_index_metadata")
                    .build(),
                // SLO observability solution internal indices
                // Kibana system user uses them to read / write slo data.
                RoleDescriptor.IndicesPrivileges.builder().indices(".slo-observability.*").privileges("all").build(),
                // Endpoint heartbeat. Kibana reads from these to determine metering/billing for endpoints.
                RoleDescriptor.IndicesPrivileges.builder().indices(".logs-endpoint.heartbeat-*").privileges("read").build(),
                // For connectors telemetry. Will be removed once we switched to connectors API
                RoleDescriptor.IndicesPrivileges.builder().indices(".elastic-connectors*").privileges("read").build() },
            null,
            new ConfigurableClusterPrivilege[] {
                new ConfigurableClusterPrivileges.ManageApplicationPrivileges(Set.of("kibana-*")),
                new ConfigurableClusterPrivileges.WriteProfileDataPrivileges(Set.of("kibana*")) },
            null,
            MetadataUtils.DEFAULT_RESERVED_METADATA,
            null,
            new RoleDescriptor.RemoteIndicesPrivileges[] {
                getRemoteIndicesReadPrivileges(".monitoring-*"),
                getRemoteIndicesReadPrivileges("apm-*"),
                getRemoteIndicesReadPrivileges("logs-apm.*"),
                getRemoteIndicesReadPrivileges("metrics-apm.*"),
                getRemoteIndicesReadPrivileges("traces-apm.*"),
                getRemoteIndicesReadPrivileges("traces-apm-*") },
            null
        );
    }
}

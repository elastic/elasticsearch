/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz.store;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.remote.RemoteInfoAction;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.xpack.core.ilm.action.GetLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleAction;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkAction;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.privilege.GetBuiltinPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.profile.ActivateProfileAction;
import org.elasticsearch.xpack.core.security.action.profile.GetProfilesAction;
import org.elasticsearch.xpack.core.security.action.profile.SuggestProfilesAction;
import org.elasticsearch.xpack.core.security.action.user.ProfileHasPrivilegesAction;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges.ManageApplicationPrivileges;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges.WriteProfileDataPrivileges;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;
import org.elasticsearch.xpack.core.security.user.KibanaSystemUser;
import org.elasticsearch.xpack.core.security.user.UsernamesField;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;
import org.elasticsearch.xpack.core.watcher.execution.TriggeredWatchStoreField;
import org.elasticsearch.xpack.core.watcher.history.HistoryStoreField;
import org.elasticsearch.xpack.core.watcher.watch.Watch;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Map.entry;

public class ReservedRolesStore implements BiConsumer<Set<String>, ActionListener<RoleRetrievalResult>> {
    /** "Security Solutions" only legacy signals index */
    public static final String ALERTS_LEGACY_INDEX = ".siem-signals*";

    /** Alerts, Rules, Cases (RAC) index used by multiple solutions */
    public static final String ALERTS_BACKING_INDEX = ".internal.alerts*";

    /** Alerts, Rules, Cases (RAC) index used by multiple solutions */
    public static final String ALERTS_INDEX_ALIAS = ".alerts*";

    /** Alerts, Rules, Cases (RAC) preview index used by multiple solutions */
    public static final String PREVIEW_ALERTS_INDEX_ALIAS = ".preview.alerts*";

    /** Alerts, Rules, Cases (RAC) preview index used by multiple solutions */
    public static final String PREVIEW_ALERTS_BACKING_INDEX_ALIAS = ".internal.preview.alerts*";

    /** "Security Solutions" only lists index for value lists for detections */
    public static final String LISTS_INDEX = ".lists-*";

    /** "Security Solutions" only lists index for value list items for detections */
    public static final String LISTS_ITEMS_INDEX = ".items-*";

    public static final RoleDescriptor SUPERUSER_ROLE_DESCRIPTOR = new RoleDescriptor(
        "superuser",
        new String[] { "all" },
        new RoleDescriptor.IndicesPrivileges[] {
            RoleDescriptor.IndicesPrivileges.builder().indices("*").privileges("all").allowRestrictedIndices(false).build(),
            RoleDescriptor.IndicesPrivileges.builder()
                .indices("*")
                .privileges("monitor", "read", "view_index_metadata", "read_cross_cluster")
                .allowRestrictedIndices(true)
                .build() },
        new RoleDescriptor.ApplicationResourcePrivileges[] {
            RoleDescriptor.ApplicationResourcePrivileges.builder().application("*").privileges("*").resources("*").build() },
        null,
        new String[] { "*" },
        MetadataUtils.DEFAULT_RESERVED_METADATA,
        Collections.emptyMap(),
        TcpTransport.isUntrustedRemoteClusterEnabled()
            ? new RoleDescriptor.RemoteIndicesPrivileges[] {
                new RoleDescriptor.RemoteIndicesPrivileges(
                    RoleDescriptor.IndicesPrivileges.builder().indices("*").privileges("all").allowRestrictedIndices(false).build(),
                    "*"
                ),
                new RoleDescriptor.RemoteIndicesPrivileges(
                    RoleDescriptor.IndicesPrivileges.builder()
                        .indices("*")
                        .privileges("monitor", "read", "view_index_metadata", "read_cross_cluster")
                        .allowRestrictedIndices(true)
                        .build(),
                    "*"
                ) }
            : null,
        null
    );

    private static final Map<String, RoleDescriptor> ALL_RESERVED_ROLES = initializeReservedRoles();

    public static final Setting<List<String>> INCLUDED_RESERVED_ROLES_SETTING = Setting.listSetting(
        SecurityField.setting("reserved_roles.include"),
        List.copyOf(ALL_RESERVED_ROLES.keySet()),
        Function.identity(),
        value -> {
            final Set<String> valueSet = Set.copyOf(value);
            if (false == valueSet.contains("superuser")) {
                throw new IllegalArgumentException("the [superuser] reserved role must be included");
            }
            final Set<String> unknownRoles = Sets.sortedDifference(valueSet, ALL_RESERVED_ROLES.keySet());
            if (false == unknownRoles.isEmpty()) {
                throw new IllegalArgumentException(
                    "unknown reserved roles to include [" + Strings.collectionToCommaDelimitedString(unknownRoles)
                );
            }
        },
        Setting.Property.NodeScope
    );
    private static Map<String, RoleDescriptor> RESERVED_ROLES = null;

    public ReservedRolesStore() {
        this(ALL_RESERVED_ROLES.keySet());
    }

    public ReservedRolesStore(Set<String> includes) {
        assert includes.contains("superuser") : "superuser must be included";
        RESERVED_ROLES = ALL_RESERVED_ROLES.entrySet()
            .stream()
            .filter(entry -> includes.contains(entry.getKey()))
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static RoleDescriptor.RemoteIndicesPrivileges getRemoteIndicesReadPrivileges(String indexPattern) {
        return new RoleDescriptor.RemoteIndicesPrivileges(
            RoleDescriptor.IndicesPrivileges.builder()
                .indices(indexPattern)
                .privileges("read", "read_cross_cluster")
                .allowRestrictedIndices(false)
                .build(),
            "*"
        );
    }

    private static Map<String, RoleDescriptor> initializeReservedRoles() {
        return Map.ofEntries(
            entry("superuser", SUPERUSER_ROLE_DESCRIPTOR),
            entry(
                "transport_client",
                new RoleDescriptor(
                    "transport_client",
                    new String[] { "transport_client" },
                    null,
                    null,
                    MetadataUtils.DEFAULT_RESERVED_METADATA
                )
            ),
            entry("kibana_admin", kibanaAdminUser("kibana_admin", MetadataUtils.DEFAULT_RESERVED_METADATA)),
            entry(
                "kibana_user",
                kibanaAdminUser("kibana_user", MetadataUtils.getDeprecatedReservedMetadata("Please use the [kibana_admin] role instead"))
            ),
            entry(
                "monitoring_user",
                new RoleDescriptor(
                    "monitoring_user",
                    new String[] { "cluster:monitor/main", "cluster:monitor/xpack/info", RemoteInfoAction.NAME },
                    new RoleDescriptor.IndicesPrivileges[] {
                        RoleDescriptor.IndicesPrivileges.builder()
                            .indices(".monitoring-*")
                            .privileges("read", "read_cross_cluster")
                            .build(),
                        RoleDescriptor.IndicesPrivileges.builder()
                            .indices("/metrics-(beats|elasticsearch|enterprisesearch|kibana|logstash).*/")
                            .privileges("read", "read_cross_cluster")
                            .build(),
                        RoleDescriptor.IndicesPrivileges.builder()
                            .indices("metricbeat-*")
                            .privileges("read", "read_cross_cluster")
                            .build() },
                    new RoleDescriptor.ApplicationResourcePrivileges[] {
                        RoleDescriptor.ApplicationResourcePrivileges.builder()
                            .application("kibana-*")
                            .resources("*")
                            .privileges("reserved_monitoring")
                            .build() },
                    null,
                    null,
                    MetadataUtils.DEFAULT_RESERVED_METADATA,
                    null,
                    TcpTransport.isUntrustedRemoteClusterEnabled()
                        ? new RoleDescriptor.RemoteIndicesPrivileges[] {
                            getRemoteIndicesReadPrivileges(".monitoring-*"),
                            getRemoteIndicesReadPrivileges("/metrics-(beats|elasticsearch|enterprisesearch|kibana|logstash).*/"),
                            getRemoteIndicesReadPrivileges("metricbeat-*") }
                        : null,
                    null
                )
            ),
            entry(
                "remote_monitoring_agent",
                new RoleDescriptor(
                    "remote_monitoring_agent",
                    new String[] {
                        "manage_index_templates",
                        "manage_ingest_pipelines",
                        "monitor",
                        GetLifecycleAction.NAME,
                        PutLifecycleAction.NAME,
                        "cluster:monitor/xpack/watcher/watch/get",
                        "cluster:admin/xpack/watcher/watch/put",
                        "cluster:admin/xpack/watcher/watch/delete" },
                    new RoleDescriptor.IndicesPrivileges[] {
                        RoleDescriptor.IndicesPrivileges.builder().indices(".monitoring-*").privileges("all").build(),
                        RoleDescriptor.IndicesPrivileges.builder()
                            .indices("metricbeat-*")
                            .privileges("index", "create_index", "view_index_metadata", IndicesAliasesAction.NAME, RolloverAction.NAME)
                            .build() },
                    null,
                    MetadataUtils.DEFAULT_RESERVED_METADATA
                )
            ),
            entry(
                "remote_monitoring_collector",
                new RoleDescriptor(
                    "remote_monitoring_collector",
                    new String[] { "monitor" },
                    new RoleDescriptor.IndicesPrivileges[] {
                        RoleDescriptor.IndicesPrivileges.builder().indices("*").privileges("monitor").allowRestrictedIndices(true).build(),
                        RoleDescriptor.IndicesPrivileges.builder()
                            .indices(".kibana*")
                            .privileges("read")
                            .allowRestrictedIndices(true)
                            .build() },
                    null,
                    null,
                    null,
                    MetadataUtils.DEFAULT_RESERVED_METADATA,
                    null
                )
            ),
            entry(
                "ingest_admin",
                new RoleDescriptor(
                    "ingest_admin",
                    new String[] { "manage_index_templates", "manage_pipeline" },
                    null,
                    null,
                    MetadataUtils.DEFAULT_RESERVED_METADATA
                )
            ),
            // reporting_user doesn't have any privileges in Elasticsearch, and Kibana authorizes privileges based on this role
            entry(
                "reporting_user",
                new RoleDescriptor(
                    "reporting_user",
                    null,
                    null,
                    null,
                    null,
                    null,
                    MetadataUtils.getDeprecatedReservedMetadata("Please use Kibana feature privileges instead"),
                    null
                )
            ),
            entry(KibanaSystemUser.ROLE_NAME, kibanaSystemRoleDescriptor(KibanaSystemUser.ROLE_NAME)),
            entry(
                "logstash_system",
                new RoleDescriptor(
                    "logstash_system",
                    new String[] { "monitor", MonitoringBulkAction.NAME },
                    null,
                    null,
                    MetadataUtils.DEFAULT_RESERVED_METADATA
                )
            ),
            entry(
                "beats_admin",
                new RoleDescriptor(
                    "beats_admin",
                    null,
                    new RoleDescriptor.IndicesPrivileges[] {
                        RoleDescriptor.IndicesPrivileges.builder().indices(".management-beats").privileges("all").build() },
                    null,
                    MetadataUtils.DEFAULT_RESERVED_METADATA
                )
            ),
            entry(
                UsernamesField.BEATS_ROLE,
                new RoleDescriptor(
                    UsernamesField.BEATS_ROLE,
                    new String[] { "monitor", MonitoringBulkAction.NAME },
                    new RoleDescriptor.IndicesPrivileges[] {
                        RoleDescriptor.IndicesPrivileges.builder()
                            .indices(".monitoring-beats-*")
                            .privileges("create_index", "create")
                            .build() },
                    null,
                    MetadataUtils.DEFAULT_RESERVED_METADATA
                )
            ),
            entry(
                UsernamesField.APM_ROLE,
                new RoleDescriptor(
                    UsernamesField.APM_ROLE,
                    new String[] { "monitor", MonitoringBulkAction.NAME },
                    new RoleDescriptor.IndicesPrivileges[] {
                        RoleDescriptor.IndicesPrivileges.builder()
                            .indices(".monitoring-beats-*")
                            .privileges("create_index", "create_doc")
                            .build() },
                    null,
                    MetadataUtils.DEFAULT_RESERVED_METADATA
                )
            ),
            entry(
                "apm_user",
                new RoleDescriptor(
                    "apm_user",
                    null,
                    new RoleDescriptor.IndicesPrivileges[] {
                        // Self managed APM Server
                        // Can be removed in 8.0
                        RoleDescriptor.IndicesPrivileges.builder().indices("apm-*").privileges("read", "view_index_metadata").build(),

                        // APM Server under fleet (data streams)
                        RoleDescriptor.IndicesPrivileges.builder().indices("logs-apm.*").privileges("read", "view_index_metadata").build(),
                        RoleDescriptor.IndicesPrivileges.builder().indices("logs-apm-*").privileges("read", "view_index_metadata").build(),
                        RoleDescriptor.IndicesPrivileges.builder()
                            .indices("metrics-apm.*")
                            .privileges("read", "view_index_metadata")
                            .build(),
                        RoleDescriptor.IndicesPrivileges.builder()
                            .indices("metrics-apm-*")
                            .privileges("read", "view_index_metadata")
                            .build(),
                        RoleDescriptor.IndicesPrivileges.builder()
                            .indices("traces-apm.*")
                            .privileges("read", "view_index_metadata")
                            .build(),
                        RoleDescriptor.IndicesPrivileges.builder()
                            .indices("traces-apm-*")
                            .privileges("read", "view_index_metadata")
                            .build(),

                        // Machine Learning indices. Only needed for legacy reasons
                        // Can be removed in 8.0
                        RoleDescriptor.IndicesPrivileges.builder()
                            .indices(".ml-anomalies*")
                            .privileges("read", "view_index_metadata")
                            .build(),

                        // Annotations
                        RoleDescriptor.IndicesPrivileges.builder()
                            .indices("observability-annotations")
                            .privileges("read", "view_index_metadata")
                            .build() },
                    new RoleDescriptor.ApplicationResourcePrivileges[] {
                        RoleDescriptor.ApplicationResourcePrivileges.builder()
                            .application("kibana-*")
                            .resources("*")
                            .privileges("reserved_ml_apm_user")
                            .build() },
                    null,
                    null,
                    MetadataUtils.getDeprecatedReservedMetadata(
                        "This role will be removed in a future major release. Please use editor and viewer roles instead"
                    ),
                    null
                )
            ),
            entry(
                "machine_learning_user",
                new RoleDescriptor(
                    "machine_learning_user",
                    new String[] { "monitor_ml" },
                    new RoleDescriptor.IndicesPrivileges[] {
                        RoleDescriptor.IndicesPrivileges.builder()
                            .indices(".ml-anomalies*", ".ml-notifications*")
                            .privileges("view_index_metadata", "read")
                            .build(),
                        RoleDescriptor.IndicesPrivileges.builder()
                            .indices(".ml-annotations*")
                            .privileges("view_index_metadata", "read", "write")
                            .build() },
                    // This role also grants Kibana privileges related to ML.
                    // This makes it completely clear to UI administrators that
                    // if they grant the Elasticsearch backend role to a user then
                    // they cannot expect Kibana privileges to stop that user from
                    // accessing ML functionality - the user could switch to curl
                    // or even Kibana dev console and call the ES endpoints directly
                    // bypassing the Kibana privileges layer entirely.
                    new RoleDescriptor.ApplicationResourcePrivileges[] {
                        RoleDescriptor.ApplicationResourcePrivileges.builder()
                            .application("kibana-*")
                            .resources("*")
                            .privileges("reserved_ml_user")
                            .build() },
                    null,
                    null,
                    MetadataUtils.DEFAULT_RESERVED_METADATA,
                    null
                )
            ),
            entry(
                "machine_learning_admin",
                new RoleDescriptor(
                    "machine_learning_admin",
                    new String[] { "manage_ml" },
                    new RoleDescriptor.IndicesPrivileges[] {
                        RoleDescriptor.IndicesPrivileges.builder()
                            .indices(".ml-anomalies*", ".ml-notifications*", ".ml-state*", ".ml-meta*", ".ml-stats-*")
                            .allowRestrictedIndices(true) // .ml-meta is a restricted index
                            .privileges("view_index_metadata", "read")
                            .build(),
                        RoleDescriptor.IndicesPrivileges.builder()
                            .indices(".ml-annotations*")
                            .privileges("view_index_metadata", "read", "write")
                            .build() },
                    // This role also grants Kibana privileges related to ML.
                    // This makes it completely clear to UI administrators that
                    // if they grant the Elasticsearch backend role to a user then
                    // they cannot expect Kibana privileges to stop that user from
                    // accessing ML functionality - the user could switch to curl
                    // or even Kibana dev console and call the ES endpoints directly
                    // bypassing the Kibana privileges layer entirely.
                    new RoleDescriptor.ApplicationResourcePrivileges[] {
                        RoleDescriptor.ApplicationResourcePrivileges.builder()
                            .application("kibana-*")
                            .resources("*")
                            .privileges("reserved_ml_admin")
                            .build() },
                    null,
                    null,
                    MetadataUtils.DEFAULT_RESERVED_METADATA,
                    null
                )
            ),
            // DEPRECATED: to be removed in 9.0.0
            entry(
                "data_frame_transforms_admin",
                new RoleDescriptor(
                    "data_frame_transforms_admin",
                    new String[] { "manage_data_frame_transforms" },
                    new RoleDescriptor.IndicesPrivileges[] {
                        RoleDescriptor.IndicesPrivileges.builder()
                            .indices(
                                TransformInternalIndexConstants.AUDIT_INDEX_PATTERN,
                                TransformInternalIndexConstants.AUDIT_INDEX_PATTERN_DEPRECATED,
                                TransformInternalIndexConstants.AUDIT_INDEX_READ_ALIAS
                            )
                            .privileges("view_index_metadata", "read")
                            .build() },
                    new RoleDescriptor.ApplicationResourcePrivileges[] {
                        RoleDescriptor.ApplicationResourcePrivileges.builder()
                            .application("kibana-*")
                            .resources("*")
                            .privileges("reserved_ml_user")
                            .build() },
                    null,
                    null,
                    MetadataUtils.getDeprecatedReservedMetadata("Please use the [transform_admin] role instead"),
                    null
                )
            ),
            // DEPRECATED: to be removed in 9.0.0
            entry(
                "data_frame_transforms_user",
                new RoleDescriptor(
                    "data_frame_transforms_user",
                    new String[] { "monitor_data_frame_transforms" },
                    new RoleDescriptor.IndicesPrivileges[] {
                        RoleDescriptor.IndicesPrivileges.builder()
                            .indices(
                                TransformInternalIndexConstants.AUDIT_INDEX_PATTERN,
                                TransformInternalIndexConstants.AUDIT_INDEX_PATTERN_DEPRECATED,
                                TransformInternalIndexConstants.AUDIT_INDEX_READ_ALIAS
                            )
                            .privileges("view_index_metadata", "read")
                            .build() },
                    new RoleDescriptor.ApplicationResourcePrivileges[] {
                        RoleDescriptor.ApplicationResourcePrivileges.builder()
                            .application("kibana-*")
                            .resources("*")
                            .privileges("reserved_ml_user")
                            .build() },
                    null,
                    null,
                    MetadataUtils.getDeprecatedReservedMetadata("Please use the [transform_user] role instead"),
                    null
                )
            ),
            entry(
                "transform_admin",
                new RoleDescriptor(
                    "transform_admin",
                    new String[] { "manage_transform" },
                    new RoleDescriptor.IndicesPrivileges[] {
                        RoleDescriptor.IndicesPrivileges.builder()
                            .indices(
                                TransformInternalIndexConstants.AUDIT_INDEX_PATTERN,
                                TransformInternalIndexConstants.AUDIT_INDEX_PATTERN_DEPRECATED,
                                TransformInternalIndexConstants.AUDIT_INDEX_READ_ALIAS
                            )
                            .privileges("view_index_metadata", "read")
                            .build() },
                    null,
                    null,
                    null,
                    MetadataUtils.DEFAULT_RESERVED_METADATA,
                    null
                )
            ),
            entry(
                "transform_user",
                new RoleDescriptor(
                    "transform_user",
                    new String[] { "monitor_transform" },
                    new RoleDescriptor.IndicesPrivileges[] {
                        RoleDescriptor.IndicesPrivileges.builder()
                            .indices(
                                TransformInternalIndexConstants.AUDIT_INDEX_PATTERN,
                                TransformInternalIndexConstants.AUDIT_INDEX_PATTERN_DEPRECATED,
                                TransformInternalIndexConstants.AUDIT_INDEX_READ_ALIAS
                            )
                            .privileges("view_index_metadata", "read")
                            .build() },
                    null,
                    null,
                    null,
                    MetadataUtils.DEFAULT_RESERVED_METADATA,
                    null
                )
            ),
            entry(
                "watcher_admin",
                new RoleDescriptor(
                    "watcher_admin",
                    new String[] { "manage_watcher" },
                    new RoleDescriptor.IndicesPrivileges[] {
                        RoleDescriptor.IndicesPrivileges.builder()
                            .indices(Watch.INDEX, TriggeredWatchStoreField.INDEX_NAME, HistoryStoreField.INDEX_PREFIX + "*")
                            .privileges("read")
                            .allowRestrictedIndices(true)
                            .build() },
                    null,
                    MetadataUtils.DEFAULT_RESERVED_METADATA
                )
            ),
            entry(
                "watcher_user",
                new RoleDescriptor(
                    "watcher_user",
                    new String[] { "monitor_watcher" },
                    new RoleDescriptor.IndicesPrivileges[] {
                        RoleDescriptor.IndicesPrivileges.builder()
                            .indices(Watch.INDEX)
                            .privileges("read")
                            .allowRestrictedIndices(true)
                            .build(),
                        RoleDescriptor.IndicesPrivileges.builder()
                            .indices(HistoryStoreField.INDEX_PREFIX + "*")
                            .privileges("read")
                            .build() },
                    null,
                    MetadataUtils.DEFAULT_RESERVED_METADATA
                )
            ),
            entry(
                "logstash_admin",
                new RoleDescriptor(
                    "logstash_admin",
                    new String[] { "manage_logstash_pipelines" },
                    new RoleDescriptor.IndicesPrivileges[] {
                        RoleDescriptor.IndicesPrivileges.builder()
                            .indices(".logstash*")
                            .privileges("create", "delete", "index", "manage", "read")
                            .allowRestrictedIndices(true)
                            .build() },
                    null,
                    MetadataUtils.DEFAULT_RESERVED_METADATA
                )
            ),
            entry(
                "rollup_user",
                new RoleDescriptor("rollup_user", new String[] { "monitor_rollup" }, null, null, MetadataUtils.DEFAULT_RESERVED_METADATA)
            ),
            entry(
                "rollup_admin",
                new RoleDescriptor("rollup_admin", new String[] { "manage_rollup" }, null, null, MetadataUtils.DEFAULT_RESERVED_METADATA)
            ),
            entry(
                "snapshot_user",
                new RoleDescriptor(
                    "snapshot_user",
                    new String[] { "create_snapshot", GetRepositoriesAction.NAME },
                    new RoleDescriptor.IndicesPrivileges[] {
                        RoleDescriptor.IndicesPrivileges.builder()
                            .indices("*")
                            .privileges("view_index_metadata")
                            .allowRestrictedIndices(true)
                            .build() },
                    null,
                    null,
                    null,
                    MetadataUtils.DEFAULT_RESERVED_METADATA,
                    null
                )
            ),
            entry(
                "enrich_user",
                new RoleDescriptor(
                    "enrich_user",
                    new String[] { "manage_enrich", "manage_ingest_pipelines", "monitor" },
                    new RoleDescriptor.IndicesPrivileges[] {
                        RoleDescriptor.IndicesPrivileges.builder().indices(".enrich-*").privileges("manage", "read", "write").build() },
                    null,
                    MetadataUtils.DEFAULT_RESERVED_METADATA
                )
            ),
            entry("viewer", buildViewerRoleDescriptor()),
            entry("editor", buildEditorRoleDescriptor())
        );
    }

    private static RoleDescriptor buildViewerRoleDescriptor() {
        return new RoleDescriptor(
            "viewer",
            new String[] {},
            new RoleDescriptor.IndicesPrivileges[] {
                // Stack
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("/~(([.]|ilm-history-).*)/")
                    .privileges("read", "view_index_metadata")
                    .build(),
                // Security
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(ReservedRolesStore.ALERTS_LEGACY_INDEX, ReservedRolesStore.LISTS_INDEX, ReservedRolesStore.LISTS_ITEMS_INDEX)
                    .privileges("read", "view_index_metadata")
                    .build(),
                // Alerts-as-data
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(ReservedRolesStore.ALERTS_INDEX_ALIAS, ReservedRolesStore.PREVIEW_ALERTS_INDEX_ALIAS)
                    .privileges("read", "view_index_metadata")
                    .build() },
            new RoleDescriptor.ApplicationResourcePrivileges[] {
                RoleDescriptor.ApplicationResourcePrivileges.builder()
                    .application("kibana-.kibana")
                    .resources("*")
                    .privileges("read")
                    .build() },
            null,
            null,
            MetadataUtils.DEFAULT_RESERVED_METADATA,
            null
        );
    }

    private static RoleDescriptor buildEditorRoleDescriptor() {
        return new RoleDescriptor(
            "editor",
            new String[] {},
            new RoleDescriptor.IndicesPrivileges[] {
                // Stack
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("/~(([.]|ilm-history-).*)/")
                    .privileges("read", "view_index_metadata")
                    .build(),
                // Observability
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("observability-annotations")
                    .privileges("read", "view_index_metadata", "write")
                    .build(),
                // Security
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(ReservedRolesStore.ALERTS_LEGACY_INDEX, ReservedRolesStore.LISTS_INDEX, ReservedRolesStore.LISTS_ITEMS_INDEX)
                    .privileges("read", "view_index_metadata", "write", "maintenance")
                    .build(),
                // Alerts-as-data
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(
                        ReservedRolesStore.ALERTS_BACKING_INDEX,
                        ReservedRolesStore.ALERTS_INDEX_ALIAS,
                        ReservedRolesStore.PREVIEW_ALERTS_BACKING_INDEX_ALIAS,
                        ReservedRolesStore.PREVIEW_ALERTS_INDEX_ALIAS
                    )
                    .privileges("read", "view_index_metadata", "write", "maintenance")
                    .build() },
            new RoleDescriptor.ApplicationResourcePrivileges[] {
                RoleDescriptor.ApplicationResourcePrivileges.builder()
                    .application("kibana-.kibana")
                    .resources("*")
                    .privileges("all")
                    .build() },
            null,
            null,
            MetadataUtils.DEFAULT_RESERVED_METADATA,
            null
        );
    }

    private static RoleDescriptor kibanaAdminUser(String name, Map<String, Object> metadata) {
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

    public static RoleDescriptor kibanaSystemRoleDescriptor(String name) {
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

                // Synthetics API key for synthetics saas service
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("synthetics-http-*", "synthetics-icmp-*", "synthetics-tcp-*", "synthetics-browser*")
                    .privileges("auto_configure", "read", "write")
                    .build(),

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
                new ManageApplicationPrivileges(Set.of("kibana-*")),
                new WriteProfileDataPrivileges(Set.of("kibana*")) },
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

    public static boolean isReserved(String role) {
        if (RESERVED_ROLES == null) {
            throw new IllegalStateException("ReserveRolesStore is not initialized properly");
        }
        return RESERVED_ROLES.containsKey(role);
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
        if (RESERVED_ROLES == null) {
            throw new IllegalStateException("ReserveRolesStore is not initialized properly");
        }
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

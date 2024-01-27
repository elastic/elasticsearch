/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Class containing all the cluster, node, and index deprecation checks that will be served
 * by the {@link DeprecationInfoAction}.
 */
public class DeprecationChecks {

    public static final Setting<List<String>> SKIP_DEPRECATIONS_SETTING = Setting.stringListSetting(
        "deprecation.skip_deprecated_settings",
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private DeprecationChecks() {}

    static List<Function<ClusterState, DeprecationIssue>> CLUSTER_SETTINGS_CHECKS = List.of();

    static final List<
        NodeDeprecationCheck<Settings, PluginsAndModules, ClusterState, XPackLicenseState, DeprecationIssue>> NODE_SETTINGS_CHECKS = List
            .of(
                NodeDeprecationChecks::checkMultipleDataPaths,
                NodeDeprecationChecks::checkDataPathsList,
                NodeDeprecationChecks::checkSharedDataPathSetting,
                NodeDeprecationChecks::checkReservedPrefixedRealmNames,
                NodeDeprecationChecks::checkSingleDataNodeWatermarkSetting,
                NodeDeprecationChecks::checkExporterUseIngestPipelineSettings,
                NodeDeprecationChecks::checkExporterPipelineMasterTimeoutSetting,
                NodeDeprecationChecks::checkExporterCreateLegacyTemplateSetting,
                NodeDeprecationChecks::checkMonitoringSettingHistoryDuration,
                NodeDeprecationChecks::checkMonitoringSettingHistoryDuration,
                NodeDeprecationChecks::checkMonitoringSettingCollectIndexRecovery,
                NodeDeprecationChecks::checkMonitoringSettingCollectIndices,
                NodeDeprecationChecks::checkMonitoringSettingCollectCcrTimeout,
                NodeDeprecationChecks::checkMonitoringSettingCollectEnrichStatsTimeout,
                NodeDeprecationChecks::checkMonitoringSettingCollectIndexRecoveryStatsTimeout,
                NodeDeprecationChecks::checkMonitoringSettingCollectIndexStatsTimeout,
                NodeDeprecationChecks::checkMonitoringSettingCollectMlJobStatsTimeout,
                NodeDeprecationChecks::checkMonitoringSettingCollectNodeStatsTimeout,
                NodeDeprecationChecks::checkMonitoringSettingCollectClusterStatsTimeout,
                NodeDeprecationChecks::checkMonitoringSettingExportersHost,
                NodeDeprecationChecks::checkMonitoringSettingExportersBulkTimeout,
                NodeDeprecationChecks::checkMonitoringSettingExportersConnectionTimeout,
                NodeDeprecationChecks::checkMonitoringSettingExportersConnectionReadTimeout,
                NodeDeprecationChecks::checkMonitoringSettingExportersAuthUsername,
                NodeDeprecationChecks::checkMonitoringSettingExportersAuthPass,
                NodeDeprecationChecks::checkMonitoringSettingExportersSSL,
                NodeDeprecationChecks::checkMonitoringSettingExportersProxyBase,
                NodeDeprecationChecks::checkMonitoringSettingExportersSniffEnabled,
                NodeDeprecationChecks::checkMonitoringSettingExportersHeaders,
                NodeDeprecationChecks::checkMonitoringSettingExportersTemplateTimeout,
                NodeDeprecationChecks::checkMonitoringSettingExportersMasterTimeout,
                NodeDeprecationChecks::checkMonitoringSettingExportersEnabled,
                NodeDeprecationChecks::checkMonitoringSettingExportersType,
                NodeDeprecationChecks::checkMonitoringSettingExportersAlertsEnabled,
                NodeDeprecationChecks::checkMonitoringSettingExportersAlertsBlacklist,
                NodeDeprecationChecks::checkMonitoringSettingExportersIndexNameTimeFormat,
                NodeDeprecationChecks::checkMonitoringSettingDecommissionAlerts,
                NodeDeprecationChecks::checkMonitoringSettingEsCollectionEnabled,
                NodeDeprecationChecks::checkMonitoringSettingCollectionEnabled,
                NodeDeprecationChecks::checkMonitoringSettingCollectionInterval,
                NodeDeprecationChecks::checkScriptContextCache,
                NodeDeprecationChecks::checkScriptContextCompilationsRateLimitSetting,
                NodeDeprecationChecks::checkScriptContextCacheSizeSetting,
                NodeDeprecationChecks::checkScriptContextCacheExpirationSetting,
                NodeDeprecationChecks::checkEnforceDefaultTierPreferenceSetting,
                NodeDeprecationChecks::checkLifecyleStepMasterTimeoutSetting,
                NodeDeprecationChecks::checkEqlEnabledSetting,
                NodeDeprecationChecks::checkNodeAttrData,
                NodeDeprecationChecks::checkWatcherBulkConcurrentRequestsSetting
            );

    static List<Function<IndexMetadata, DeprecationIssue>> INDEX_SETTINGS_CHECKS = List.of(
        IndexDeprecationChecks::oldIndicesCheck,
        IndexDeprecationChecks::translogRetentionSettingCheck,
        IndexDeprecationChecks::checkIndexDataPath,
        IndexDeprecationChecks::storeTypeSettingCheck,
        IndexDeprecationChecks::frozenIndexSettingCheck,
        IndexDeprecationChecks::deprecatedCamelCasePattern
    );

    /**
     * helper utility function to reduce repeat of running a specific {@link List} of checks.
     *
     * @param checks The functional checks to execute using the mapper function
     * @param mapper The function that executes the lambda check with the appropriate arguments
     * @param <T> The signature of the check (BiFunction, Function, including the appropriate arguments)
     * @return The list of {@link DeprecationIssue} that were found in the cluster
     */
    static <T> List<DeprecationIssue> filterChecks(List<T> checks, Function<T, DeprecationIssue> mapper) {
        return checks.stream().map(mapper).filter(Objects::nonNull).collect(Collectors.toList());
    }

    @FunctionalInterface
    public interface NodeDeprecationCheck<A, B, C, D, R> {
        R apply(A first, B second, C third, D fourth);
    }
}

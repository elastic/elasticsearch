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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Class containing all the cluster, node, and index deprecation checks that will be served
 * by the {@link DeprecationInfoAction}.
 */
public class DeprecationChecks {

    public static final Setting<List<String>> SKIP_DEPRECATIONS_SETTING = Setting.listSetting(
        "deprecation.skip_deprecated_settings",
        Collections.emptyList(),
        Function.identity(),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private DeprecationChecks() {}

    static List<Function<ClusterState, DeprecationIssue>> CLUSTER_SETTINGS_CHECKS = Collections.unmodifiableList(
        Arrays.asList(
            ClusterDeprecationChecks::checkUserAgentPipelines,
            ClusterDeprecationChecks::checkTemplatesWithTooManyFields,
            ClusterDeprecationChecks::checkTemplatesWithFieldNamesDisabled,
            ClusterDeprecationChecks::checkTemplatesWithCustomAndMultipleTypes,
            ClusterDeprecationChecks::checkTemplatesWithChainedMultiFields,
            ClusterDeprecationChecks::checkComponentTemplatesWithChainedMultiFields,
            ClusterDeprecationChecks::checkTemplatesWithChainedMultiFieldsInDynamicTemplates,
            ClusterDeprecationChecks::checkComponentTemplatesWithChainedMultiFieldsInDynamicTemplates,
            ClusterDeprecationChecks::checkTemplatesWithBoostedFields,
            ClusterDeprecationChecks::checkComponentTemplatesWithBoostedFields,
            ClusterDeprecationChecks::checkTemplatesWithBoostFieldsInDynamicTemplates,
            ClusterDeprecationChecks::checkComponentTemplatesWithBoostedFieldsInDynamicTemplates,
            ClusterDeprecationChecks::checkGeoShapeTemplates,
            ClusterDeprecationChecks::checkSparseVectorTemplates,
            ClusterDeprecationChecks::checkILMFreezeActions,
            ClusterDeprecationChecks::emptyDataTierPreferenceCheck,
            ClusterDeprecationChecks::checkShards
        )
    );

    static final List<
        NodeDeprecationCheck<Settings, PluginsAndModules, ClusterState, XPackLicenseState, DeprecationIssue>> NODE_SETTINGS_CHECKS;

    private static Set<Setting<Boolean>> getAllDeprecatedNodeRolesSettings() {
        Set<Setting<Boolean>> deprecatedNodeRolesSettings = DiscoveryNode.getPossibleRoles()
            .stream()
            .map(r -> r.legacySetting())
            .filter(s -> s != null)
            .collect(Collectors.toSet());
        deprecatedNodeRolesSettings.add(
            Setting.boolSetting("node.voting_only", false, Setting.Property.Deprecated, Setting.Property.NodeScope)
        );
        deprecatedNodeRolesSettings.add(Setting.boolSetting("node.ml", true, Setting.Property.Deprecated, Setting.Property.NodeScope));
        deprecatedNodeRolesSettings.add(
            Setting.boolSetting("node.transform", true, Setting.Property.Deprecated, Setting.Property.NodeScope)
        );
        return deprecatedNodeRolesSettings;
    }

    static {
        final Stream<
            NodeDeprecationCheck<Settings, PluginsAndModules, ClusterState, XPackLicenseState, DeprecationIssue>> legacyRoleSettings =
                getAllDeprecatedNodeRolesSettings().stream()
                    .map(setting -> (s, p, t, c) -> NodeDeprecationChecks.checkLegacyRoleSettings(setting, s, p));
        NODE_SETTINGS_CHECKS = Stream.concat(
            legacyRoleSettings,
            Stream.of(
                NodeDeprecationChecks::javaVersionCheck,
                NodeDeprecationChecks::checkPidfile,
                NodeDeprecationChecks::checkProcessors,
                NodeDeprecationChecks::checkMissingRealmOrders,
                NodeDeprecationChecks::checkUniqueRealmOrders,
                NodeDeprecationChecks::checkImplicitlyDisabledBasicRealms,
                NodeDeprecationChecks::checkReservedPrefixedRealmNames,
                (settings, pluginsAndModules, clusterState, licenseState) -> NodeDeprecationChecks.checkThreadPoolListenerQueueSize(
                    settings,
                    clusterState
                ),
                (settings, pluginsAndModules, clusterState, licenseState) -> NodeDeprecationChecks.checkThreadPoolListenerSize(
                    settings,
                    clusterState
                ),
                NodeDeprecationChecks::checkClusterRemoteConnectSetting,
                NodeDeprecationChecks::checkNodeLocalStorageSetting,
                (settings, pluginsAndModules, clusterState, licenseState) -> NodeDeprecationChecks
                    .checkNodeBasicLicenseFeatureEnabledSetting(settings, XPackSettings.ENRICH_ENABLED_SETTING, clusterState),
                (settings, pluginsAndModules, clusterState, licenseState) -> NodeDeprecationChecks
                    .checkNodeBasicLicenseFeatureEnabledSetting(settings, XPackSettings.FLATTENED_ENABLED, clusterState),
                (settings, pluginsAndModules, clusterState, licenseState) -> NodeDeprecationChecks
                    .checkNodeBasicLicenseFeatureEnabledSetting(settings, XPackSettings.INDEX_LIFECYCLE_ENABLED, clusterState),
                (settings, pluginsAndModules, clusterState, licenseState) -> NodeDeprecationChecks
                    .checkNodeBasicLicenseFeatureEnabledSetting(settings, XPackSettings.MONITORING_ENABLED, clusterState),
                (settings, pluginsAndModules, clusterState, licenseState) -> NodeDeprecationChecks
                    .checkNodeBasicLicenseFeatureEnabledSetting(settings, XPackSettings.ROLLUP_ENABLED, clusterState),
                (settings, pluginsAndModules, clusterState, licenseState) -> NodeDeprecationChecks
                    .checkNodeBasicLicenseFeatureEnabledSetting(settings, XPackSettings.SNAPSHOT_LIFECYCLE_ENABLED, clusterState),
                (settings, pluginsAndModules, clusterState, licenseState) -> NodeDeprecationChecks
                    .checkNodeBasicLicenseFeatureEnabledSetting(settings, XPackSettings.SQL_ENABLED, clusterState),
                (settings, pluginsAndModules, clusterState, licenseState) -> NodeDeprecationChecks
                    .checkNodeBasicLicenseFeatureEnabledSetting(settings, XPackSettings.TRANSFORM_ENABLED, clusterState),
                (settings, pluginsAndModules, clusterState, licenseState) -> NodeDeprecationChecks
                    .checkNodeBasicLicenseFeatureEnabledSetting(settings, XPackSettings.VECTORS_ENABLED, clusterState),
                NodeDeprecationChecks::checkMultipleDataPaths,
                NodeDeprecationChecks::checkDataPathsList,
                NodeDeprecationChecks::checkBootstrapSystemCallFilterSetting,
                NodeDeprecationChecks::checkSharedDataPathSetting,
                NodeDeprecationChecks::checkSingleDataNodeWatermarkSetting,
                NodeDeprecationChecks::checkImplicitlyDisabledSecurityOnBasicAndTrial,
                NodeDeprecationChecks::checkSearchRemoteSettings,
                NodeDeprecationChecks::checkMonitoringExporterPassword,
                NodeDeprecationChecks::checkFractionalByteValueSettings,
                NodeDeprecationChecks::checkFrozenCacheLeniency,
                NodeDeprecationChecks::checkSslServerEnabled,
                NodeDeprecationChecks::checkSslCertConfiguration,
                NodeDeprecationChecks::checkClusterRoutingAllocationIncludeRelocationsSetting,
                (settings, pluginsAndModules, clusterState, licenseState) -> NodeDeprecationChecks
                    .checkNoPermitHandshakeFromIncompatibleBuilds(
                        settings,
                        pluginsAndModules,
                        clusterState,
                        licenseState,
                        () -> System.getProperty(TransportService.PERMIT_HANDSHAKES_FROM_INCOMPATIBLE_BUILDS_KEY)
                    ),
                NodeDeprecationChecks::checkTransportClientProfilesFilterSetting,
                NodeDeprecationChecks::checkDelayClusterStateRecoverySettings,
                NodeDeprecationChecks::checkFixedAutoQueueSizeThreadpool,
                NodeDeprecationChecks::checkJoinTimeoutSetting,
                NodeDeprecationChecks::checkClusterRoutingAllocationIncludeRelocationsSetting,
                NodeDeprecationChecks::checkClusterRoutingRequireSetting,
                NodeDeprecationChecks::checkClusterRoutingIncludeSetting,
                NodeDeprecationChecks::checkClusterRoutingExcludeSetting,
                NodeDeprecationChecks::checkAcceptDefaultPasswordSetting,
                NodeDeprecationChecks::checkAcceptRolesCacheMaxSizeSetting,
                NodeDeprecationChecks::checkRolesCacheTTLSizeSetting,
                NodeDeprecationChecks::checkMaxLocalStorageNodesSetting,
                NodeDeprecationChecks::checkSamlNameIdFormatSetting,
                NodeDeprecationChecks::checkClusterRoutingAllocationIncludeRelocationsSetting,
                NodeDeprecationChecks::checkSingleDataNodeWatermarkSetting,
                NodeDeprecationChecks::checkExporterUseIngestPipelineSettings,
                NodeDeprecationChecks::checkExporterPipelineMasterTimeoutSetting,
                NodeDeprecationChecks::checkExporterCreateLegacyTemplateSetting,
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
                NodeDeprecationChecks::checkClusterRoutingAllocationIncludeRelocationsSetting,
                NodeDeprecationChecks::checkScriptContextCache,
                NodeDeprecationChecks::checkScriptContextCompilationsRateLimitSetting,
                NodeDeprecationChecks::checkScriptContextCacheSizeSetting,
                NodeDeprecationChecks::checkScriptContextCacheExpirationSetting,
                NodeDeprecationChecks::checkReroutePrioritySetting,
                NodeDeprecationChecks::checkZenBwcPingTimeoutSetting,
                NodeDeprecationChecks::checkZenUnsafeBootstrappingOnUpgradeSetting,
                NodeDeprecationChecks::checkZenCommitTimeoutSetting,
                NodeDeprecationChecks::checkZenPublishDiffEnableSetting,
                NodeDeprecationChecks::checkZenConnectOnNetworkDisconnectSetting,
                NodeDeprecationChecks::checkZenPingIntervalSetting,
                NodeDeprecationChecks::checkZenFDPingTimeoutSetting,
                NodeDeprecationChecks::checkZenPingRetriesSetting,
                NodeDeprecationChecks::checkZenRegisterConnectionListenerSetting,
                NodeDeprecationChecks::checkZenMinimumMasterNodesSetting,
                NodeDeprecationChecks::checkZenHostsProviderSetting,
                NodeDeprecationChecks::checkZenJoinRetryAttemptsSetting,
                NodeDeprecationChecks::checkZenJoinRetryDelaySetting,
                NodeDeprecationChecks::checkZenJoinTimeoutSetting,
                NodeDeprecationChecks::checkZenMasterElectionIgnoreNonMasterPingsSetting,
                NodeDeprecationChecks::checkZenMasterElectionWaitForJoinsTimeoutSetting,
                NodeDeprecationChecks::checkZenMaxPingsFromAnotherMasterSetting,
                NodeDeprecationChecks::checkZenNoMasterBlockSetting,
                NodeDeprecationChecks::checkZenPintUnicastConcurrentConnectssSetting,
                NodeDeprecationChecks::checkZenPingUnicastHostsSetting,
                NodeDeprecationChecks::checkZenPingUnicastHostsResolveTimeoutSetting,
                NodeDeprecationChecks::checkZenPingTimeoutSetting,
                NodeDeprecationChecks::checkZenPublishMaxPendingClusterStatesSetting,
                NodeDeprecationChecks::checkZenPublishTimeoutSetting,
                NodeDeprecationChecks::checkZenPingSendLeaveRequestSetting,
                NodeDeprecationChecks::checkAutoImportDanglingIndicesSetting,
                NodeDeprecationChecks::checkHttpContentTypeRequiredSetting,
                NodeDeprecationChecks::checkFsRepositoryCompressionSetting,
                NodeDeprecationChecks::checkHttpTcpNoDelaySetting,
                NodeDeprecationChecks::checkNetworkTcpConnectTimeoutSetting,
                NodeDeprecationChecks::checkTransportTcpConnectTimeoutSetting,
                NodeDeprecationChecks::checkTransportPortSetting,
                NodeDeprecationChecks::checkTcpNoDelaySetting,
                NodeDeprecationChecks::checkTransportCompressSetting,
                NodeDeprecationChecks::checkXpackDataFrameEnabledSetting,
                NodeDeprecationChecks::checkWatcherHistoryCleanerServiceSetting,
                NodeDeprecationChecks::checkLifecyleStepMasterTimeoutSetting,
                NodeDeprecationChecks::checkEqlEnabledSetting,
                NodeDeprecationChecks::checkNodeAttrData,
                NodeDeprecationChecks::checkPollIntervalTooLow
            )
        ).collect(Collectors.toList());
    }

    static List<BiFunction<ClusterState, IndexMetadata, DeprecationIssue>> INDEX_SETTINGS_CHECKS = Collections.unmodifiableList(
        Arrays.asList(
            (clusterState, indexMetadata) -> IndexDeprecationChecks.oldIndicesCheck(indexMetadata),
            (clusterState, indexMetadata) -> IndexDeprecationChecks.chainedMultiFieldsCheck(indexMetadata),
            (clusterState, indexMetadata) -> IndexDeprecationChecks.chainedMultiFieldsDynamicTemplateCheck(indexMetadata),
            (clusterState, indexMetadata) -> IndexDeprecationChecks.boostMappingCheck(indexMetadata),
            (clusterState, indexMetadata) -> IndexDeprecationChecks.boostDynamicTemplateCheck(indexMetadata),
            (clusterState, indexMetadata) -> IndexDeprecationChecks.deprecatedJodaDateTimeFormat(indexMetadata),
            (clusterState, indexMetadata) -> IndexDeprecationChecks.deprecatedCamelCasePattern(indexMetadata),
            (clusterState, indexMetadata) -> IndexDeprecationChecks.translogRetentionSettingCheck(indexMetadata),
            (clusterState, indexMetadata) -> IndexDeprecationChecks.fieldNamesDisabledCheck(indexMetadata),
            (clusterState, indexMetadata) -> IndexDeprecationChecks.checkIndexDataPath(indexMetadata),
            (clusterState, indexMetadata) -> IndexDeprecationChecks.indexingSlowLogLevelSettingCheck(indexMetadata),
            (clusterState, indexMetadata) -> IndexDeprecationChecks.searchSlowLogLevelSettingCheck(indexMetadata),
            (clusterState, indexMetadata) -> IndexDeprecationChecks.storeTypeSettingCheck(indexMetadata),
            (clusterState, indexMetadata) -> IndexDeprecationChecks.checkIndexRoutingRequireSetting(indexMetadata),
            (clusterState, indexMetadata) -> IndexDeprecationChecks.checkIndexRoutingIncludeSetting(indexMetadata),
            (clusterState, indexMetadata) -> IndexDeprecationChecks.checkIndexRoutingExcludeSetting(indexMetadata),
            (clusterState, indexMetadata) -> IndexDeprecationChecks.checkIndexMatrixFiltersSetting(indexMetadata),
            (clusterState, indexMetadata) -> IndexDeprecationChecks.checkGeoShapeMappings(indexMetadata),
            (clusterState, indexMetadata) -> IndexDeprecationChecks.frozenIndexSettingCheck(indexMetadata),
            (clusterState, indexMetadata) -> IndexDeprecationChecks.httpContentTypeRequiredSettingCheck(indexMetadata),
            (clusterState, indexMetadata) -> IndexDeprecationChecks.mapperDyamicSettingCheck(indexMetadata)
        )
    );

    /**
     * helper utility function to reduce repeat of running a specific {@link List} of checks.
     *
     * @param checks The functional checks to execute using the mapper function
     * @param mapper The function that executes the lambda check with the appropriate arguments
     * @param <T> The signature of the check (TriFunction, BiFunction, Function, including the appropriate arguments)
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

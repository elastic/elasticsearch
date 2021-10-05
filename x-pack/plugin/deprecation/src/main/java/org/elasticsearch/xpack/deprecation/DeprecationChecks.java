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
import org.elasticsearch.common.regex.Regex;
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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Class containing all the cluster, node, and index deprecation checks that will be served
 * by the {@link DeprecationInfoAction}.
 */
public class DeprecationChecks {

    public static final Setting<List<String>> HIDE_DEPRECATIONS_SETTING =
        Setting.listSetting(
            "deprecation.hide_deprecated_settings",
            Collections.emptyList(),
            Function.identity(),
            new Setting.Property[] {Setting.Property.NodeScope, Setting.Property.Dynamic}
        );

    private DeprecationChecks() {
    }

    static List<ClusterDeprecationCheck<ClusterState, DeprecatedSettingsChecker, DeprecationIssue>> CLUSTER_SETTINGS_CHECKS =
        Collections.unmodifiableList(Arrays.asList(
            ClusterDeprecationChecks::checkUserAgentPipelines,
            ClusterDeprecationChecks::checkTemplatesWithTooManyFields,
            ClusterDeprecationChecks::checkPollIntervalTooLow,
            ClusterDeprecationChecks::checkTemplatesWithFieldNamesDisabled,
            ClusterDeprecationChecks::checkTemplatesWithMultipleTypes,
            ClusterDeprecationChecks::checkClusterRoutingAllocationIncludeRelocationsSetting,
            ClusterDeprecationChecks::checkGeoShapeTemplates,
            ClusterDeprecationChecks::checkSparseVectorTemplates,
            ClusterDeprecationChecks::checkILMFreezeActions
        ));

    static final List<NodeDeprecationCheck<Settings, PluginsAndModules, ClusterState, XPackLicenseState, DeprecatedSettingsChecker,
        DeprecationIssue>>
        NODE_SETTINGS_CHECKS;

        static {
            final Stream<NodeDeprecationCheck<Settings, PluginsAndModules, ClusterState, XPackLicenseState,
            DeprecatedSettingsChecker, DeprecationIssue>>
                legacyRoleSettings =
                DiscoveryNode.getPossibleRoles().stream()
                .filter(r -> r.legacySetting() != null)
                .map(r -> (s, p, t, f, c) -> NodeDeprecationChecks.checkLegacyRoleSettings(r.legacySetting(), s, p, c));
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
                    (settings, pluginsAndModules, clusterState, licenseState, deprecatedSettingsChecker) ->
                        NodeDeprecationChecks.checkThreadPoolListenerQueueSize(settings, deprecatedSettingsChecker),
                    (settings, pluginsAndModules, clusterState, licenseState, deprecatedSettingsChecker) ->
                        NodeDeprecationChecks.checkThreadPoolListenerSize(settings, deprecatedSettingsChecker),
                    NodeDeprecationChecks::checkClusterRemoteConnectSetting,
                    NodeDeprecationChecks::checkNodeLocalStorageSetting,
                    NodeDeprecationChecks::checkGeneralScriptSizeSetting,
                    NodeDeprecationChecks::checkGeneralScriptExpireSetting,
                    NodeDeprecationChecks::checkGeneralScriptCompileSettings,
                    (settings, pluginsAndModules, clusterState, licenseState, deprecatedSettingsChecker) ->
                        NodeDeprecationChecks.checkNodeBasicLicenseFeatureEnabledSetting(settings, XPackSettings.ENRICH_ENABLED_SETTING,
                            deprecatedSettingsChecker),
                    (settings, pluginsAndModules, clusterState, licenseState, deprecatedSettingsChecker) ->
                        NodeDeprecationChecks.checkNodeBasicLicenseFeatureEnabledSetting(settings, XPackSettings.FLATTENED_ENABLED,
                            deprecatedSettingsChecker),
                    (settings, pluginsAndModules, clusterState, licenseState, deprecatedSettingsChecker) ->
                        NodeDeprecationChecks.checkNodeBasicLicenseFeatureEnabledSetting(settings, XPackSettings.INDEX_LIFECYCLE_ENABLED,
                            deprecatedSettingsChecker),
                    (settings, pluginsAndModules, clusterState, licenseState, deprecatedSettingsChecker) ->
                        NodeDeprecationChecks.checkNodeBasicLicenseFeatureEnabledSetting(settings, XPackSettings.MONITORING_ENABLED,
                            deprecatedSettingsChecker),
                    (settings, pluginsAndModules, clusterState, licenseState, deprecatedSettingsChecker) ->
                        NodeDeprecationChecks.checkNodeBasicLicenseFeatureEnabledSetting(settings, XPackSettings.ROLLUP_ENABLED,
                            deprecatedSettingsChecker),
                    (settings, pluginsAndModules, clusterState, licenseState, deprecatedSettingsChecker) ->
                        NodeDeprecationChecks.checkNodeBasicLicenseFeatureEnabledSetting(settings,
                            XPackSettings.SNAPSHOT_LIFECYCLE_ENABLED, deprecatedSettingsChecker),
                    (settings, pluginsAndModules, clusterState, licenseState, deprecatedSettingsChecker) ->
                        NodeDeprecationChecks.checkNodeBasicLicenseFeatureEnabledSetting(settings, XPackSettings.SQL_ENABLED,
                            deprecatedSettingsChecker),
                    (settings, pluginsAndModules, clusterState, licenseState, deprecatedSettingsChecker) ->
                        NodeDeprecationChecks.checkNodeBasicLicenseFeatureEnabledSetting(settings, XPackSettings.TRANSFORM_ENABLED,
                            deprecatedSettingsChecker),
                    (settings, pluginsAndModules, clusterState, licenseState, deprecatedSettingsChecker) ->
                        NodeDeprecationChecks.checkNodeBasicLicenseFeatureEnabledSetting(settings, XPackSettings.VECTORS_ENABLED,
                            deprecatedSettingsChecker),
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
                    (settings, pluginsAndModules, clusterState, licenseState, deprecatedSettingsChecker) ->
                        NodeDeprecationChecks.checkNoPermitHandshakeFromIncompatibleBuilds(settings, pluginsAndModules, clusterState,
                            licenseState, () -> System.getProperty(TransportService.PERMIT_HANDSHAKES_FROM_INCOMPATIBLE_BUILDS_KEY)),
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
                    NodeDeprecationChecks::checkClusterRoutingAllocationIncludeRelocationsSetting
                )
            ).collect(Collectors.toList());
        }

    static List<IndexDeprecationCheck<IndexMetadata, DeprecatedSettingsChecker, DeprecationIssue>> INDEX_SETTINGS_CHECKS =
        Collections.unmodifiableList(Arrays.asList(
            IndexDeprecationChecks::oldIndicesCheck,
            IndexDeprecationChecks::tooManyFieldsCheck,
            IndexDeprecationChecks::chainedMultiFieldsCheck,
            IndexDeprecationChecks::deprecatedDateTimeFormat,
            IndexDeprecationChecks::translogRetentionSettingCheck,
            IndexDeprecationChecks::fieldNamesDisabledCheck,
            IndexDeprecationChecks::checkIndexDataPath,
            IndexDeprecationChecks::indexingSlowLogLevelSettingCheck,
            IndexDeprecationChecks::searchSlowLogLevelSettingCheck,
            IndexDeprecationChecks::storeTypeSettingCheck,
            IndexDeprecationChecks::checkIndexRoutingRequireSetting,
            IndexDeprecationChecks::checkIndexRoutingIncludeSetting,
            IndexDeprecationChecks::checkIndexRoutingExcludeSetting,
            IndexDeprecationChecks::checkIndexMatrixFiltersSetting,
            IndexDeprecationChecks::checkGeoShapeMappings
        ));

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

    static boolean shouldHideDeprecation(List<String> hideDeprecationsSetting, String deprecatedSettingKey) {
        return Regex.simpleMatch(hideDeprecationsSetting, deprecatedSettingKey);
    }

    @FunctionalInterface
    public interface ClusterDeprecationCheck<A, B, R> {
        R apply(A first, B second);
    }

    @FunctionalInterface
    public interface NodeDeprecationCheck<A, B, C, D, E, R> {
        R apply(A first, B second, C third, D fourth, E fifth);
    }

    @FunctionalInterface
    public interface IndexDeprecationCheck<A, B, R> {
        R apply(A first, B second);
    }

    @FunctionalInterface
    public interface DeprecatedSettingsChecker {
        boolean shouldHideDeprecation(String setting);
    }
}

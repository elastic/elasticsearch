/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.monitoring.MonitoringDeprecatedSettings;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.settings.Setting.Property.Deprecated;
import static org.elasticsearch.common.settings.Setting.Property.NodeScope;
import static org.elasticsearch.common.settings.Setting.Property.OperatorDynamic;
import static org.elasticsearch.xpack.core.security.authc.RealmSettings.RESERVED_REALM_AND_DOMAIN_NAME_PREFIX;

public class NodeDeprecationChecks {

    // Visible for testing
    static final List<
        NodeDeprecationCheck<Settings, PluginsAndModules, ClusterState, XPackLicenseState, DeprecationIssue>> SINGLE_NODE_CHECKS = List.of(
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
            NodeDeprecationChecks::checkWatcherBulkConcurrentRequestsSetting,
            NodeDeprecationChecks::checkTracingApmSettings
        );

    static DeprecationIssue checkDeprecatedSetting(
        final Settings clusterSettings,
        final Settings nodeSettings,
        final Setting<?> deprecatedSetting,
        final String url,
        final String whenRemoved
    ) {
        if (deprecatedSetting.exists(clusterSettings) == false && deprecatedSetting.exists(nodeSettings) == false) {
            return null;
        }
        final String deprecatedSettingKey = deprecatedSetting.getKey();
        final String value = deprecatedSetting.exists(clusterSettings)
            ? deprecatedSetting.get(clusterSettings).toString()
            : deprecatedSetting.get(nodeSettings).toString();
        final String message = String.format(
            Locale.ROOT,
            "setting [%s] is deprecated and will be removed " + whenRemoved,
            deprecatedSettingKey
        );
        final String details = String.format(
            Locale.ROOT,
            "the setting [%s] is currently set to [%s], remove this setting",
            deprecatedSettingKey,
            value
        );
        return new DeprecationIssue(DeprecationIssue.Level.WARNING, message, url, details, false, null);
    }

    private static Map<String, Object> createMetaMapForRemovableSettings(boolean canAutoRemoveSetting, String removableSetting) {
        return createMetaMapForRemovableSettings(canAutoRemoveSetting, Collections.singletonList(removableSetting));
    }

    private static Map<String, Object> createMetaMapForRemovableSettings(boolean canAutoRemoveSetting, List<String> removableSettings) {
        return canAutoRemoveSetting ? DeprecationIssue.createMetaMapForRemovableSettings(removableSettings) : null;
    }

    static DeprecationIssue checkRemovedSetting(
        final Settings clusterSettings,
        final Settings nodeSettings,
        final Setting<?> removedSetting,
        final String url,
        String additionalDetailMessage,
        DeprecationIssue.Level deprecationLevel
    ) {
        if (removedSetting.exists(clusterSettings) == false && removedSetting.exists(nodeSettings) == false) {
            return null;
        }
        final String removedSettingKey = removedSetting.getKey();
        // read setting to force the deprecation warning
        if (removedSetting.exists(clusterSettings)) {
            removedSetting.get(clusterSettings);
        } else {
            removedSetting.get(nodeSettings);
        }

        final String message = String.format(Locale.ROOT, "Setting [%s] is deprecated", removedSettingKey);
        final String details = additionalDetailMessage == null
            ? String.format(Locale.ROOT, "Remove the [%s] setting.", removedSettingKey)
            : String.format(Locale.ROOT, "Remove the [%s] setting. %s", removedSettingKey, additionalDetailMessage);
        boolean canAutoRemoveSetting = removedSetting.exists(clusterSettings) && removedSetting.exists(nodeSettings) == false;
        Map<String, Object> meta = createMetaMapForRemovableSettings(canAutoRemoveSetting, removedSettingKey);
        return new DeprecationIssue(deprecationLevel, message, url, details, false, meta);
    }

    static DeprecationIssue checkMultipleRemovedSettings(
        final Settings clusterSettings,
        final Settings nodeSettings,
        final List<Setting<?>> removedSettings,
        final String url,
        String additionalDetailMessage,
        DeprecationIssue.Level deprecationLevel
    ) {

        var removedSettingsRemaining = removedSettings.stream().filter(s -> s.exists(clusterSettings) || s.exists(nodeSettings)).toList();
        if (removedSettingsRemaining.isEmpty()) {
            return null;
        }
        if (removedSettingsRemaining.size() == 1) {
            Setting<?> removedSetting = removedSettingsRemaining.get(0);
            return checkRemovedSetting(clusterSettings, nodeSettings, removedSetting, url, additionalDetailMessage, deprecationLevel);
        }

        // read settings to force the deprecation warning
        removedSettingsRemaining.forEach(s -> {
            if (s.exists(clusterSettings)) {
                s.get(clusterSettings);
            } else {
                s.get(nodeSettings);
            }
        });

        var removedSettingKeysRemaining = removedSettingsRemaining.stream().map(Setting::getKey).sorted().toList();
        final String message = String.format(Locale.ROOT, "Settings %s are deprecated", removedSettingKeysRemaining);
        final String details = additionalDetailMessage == null
            ? String.format(Locale.ROOT, "Remove each setting in %s.", removedSettingKeysRemaining)
            : String.format(Locale.ROOT, "Remove each setting in %s. %s", removedSettingKeysRemaining, additionalDetailMessage);

        var canAutoRemoveSettings = removedSettingsRemaining.stream()
            .allMatch(s -> s.exists(clusterSettings) && s.exists(nodeSettings) == false);
        var meta = createMetaMapForRemovableSettings(canAutoRemoveSettings, removedSettingKeysRemaining);
        return new DeprecationIssue(deprecationLevel, message, url, details, false, meta);
    }

    static DeprecationIssue checkMultipleDataPaths(
        Settings nodeSettings,
        PluginsAndModules plugins,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        List<String> dataPaths = Environment.PATH_DATA_SETTING.get(nodeSettings);
        if (dataPaths.size() > 1) {
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                "Specifying multiple data paths is deprecated",
                "https://ela.st/es-deprecation-7-multiple-paths",
                "The [path.data] setting contains a list of paths. Specify a single path as a string. Use RAID or other system level "
                    + "features to utilize multiple disks. If multiple data paths are configured, the node will fail to start in 8.0.",
                false,
                null
            );
        }
        return null;
    }

    static DeprecationIssue checkDataPathsList(
        Settings nodeSettings,
        PluginsAndModules plugins,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        if (Environment.dataPathUsesList(nodeSettings)) {
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                "Multiple data paths are not supported",
                "https://ela.st/es-deprecation-7-multiple-paths",
                "The [path.data] setting contains a list of paths. Specify a single path as a string. Use RAID or other system level "
                    + "features to utilize multiple disks. If multiple data paths are configured, the node will fail to start in 8.0.",
                false,
                null
            );
        }
        return null;
    }

    static DeprecationIssue checkSharedDataPathSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        if (Environment.PATH_SHARED_DATA_SETTING.exists(settings)) {
            final String message = String.format(
                Locale.ROOT,
                "setting [%s] is deprecated and will be removed in a future version",
                Environment.PATH_SHARED_DATA_SETTING.getKey()
            );
            final String url = "https://ela.st/es-deprecation-7-shared-data-path";
            final String details = "Found shared data path configured. Discontinue use of this setting.";
            return new DeprecationIssue(DeprecationIssue.Level.WARNING, message, url, details, false, null);
        }
        return null;
    }

    static DeprecationIssue checkReservedPrefixedRealmNames(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        final Map<RealmConfig.RealmIdentifier, Settings> realmSettings = RealmSettings.getRealmSettings(settings);
        if (realmSettings.isEmpty()) {
            return null;
        }
        List<RealmConfig.RealmIdentifier> reservedPrefixedRealmIdentifiers = new ArrayList<>();
        for (RealmConfig.RealmIdentifier realmIdentifier : realmSettings.keySet()) {
            if (realmIdentifier.getName().startsWith(RESERVED_REALM_AND_DOMAIN_NAME_PREFIX)) {
                reservedPrefixedRealmIdentifiers.add(realmIdentifier);
            }
        }
        if (reservedPrefixedRealmIdentifiers.isEmpty()) {
            return null;
        } else {
            return new DeprecationIssue(
                DeprecationIssue.Level.CRITICAL,
                "Realm that start with [" + RESERVED_REALM_AND_DOMAIN_NAME_PREFIX + "] will not be permitted in a future major release.",
                "https://ela.st/es-deprecation-7-realm-prefix",
                String.format(
                    Locale.ROOT,
                    "Found realm "
                        + (reservedPrefixedRealmIdentifiers.size() == 1 ? "name" : "names")
                        + " with reserved prefix [%s]: [%s]. "
                        + "In a future major release, node will fail to start if any realm names start with reserved prefix.",
                    RESERVED_REALM_AND_DOMAIN_NAME_PREFIX,
                    reservedPrefixedRealmIdentifiers.stream()
                        .map(rid -> RealmSettings.PREFIX + rid.getType() + "." + rid.getName())
                        .sorted()
                        .collect(Collectors.joining("; "))
                ),
                false,
                null
            );
        }
    }

    static DeprecationIssue checkSingleDataNodeWatermarkSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        if (DiskThresholdDecider.ENABLE_FOR_SINGLE_DATA_NODE.exists(settings)) {
            String key = DiskThresholdDecider.ENABLE_FOR_SINGLE_DATA_NODE.getKey();
            return new DeprecationIssue(
                DeprecationIssue.Level.CRITICAL,
                String.format(Locale.ROOT, "setting [%s] is deprecated and will not be available in a future version", key),
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.14/"
                    + "breaking-changes-7.14.html#deprecate-single-data-node-watermark",
                String.format(Locale.ROOT, "found [%s] configured. Discontinue use of this setting.", key),
                false,
                null
            );
        }

        return null;
    }

    private static DeprecationIssue deprecatedAffixSetting(
        Setting.AffixSetting<?> deprecatedAffixSetting,
        String detailPattern,
        String url,
        DeprecationIssue.Level warningLevel,
        Settings clusterSettings,
        Settings nodeSettings
    ) {
        var deprecatedConcreteNodeSettings = deprecatedAffixSetting.getAllConcreteSettings(nodeSettings)
            .sorted(Comparator.comparing(Setting::getKey))
            .toList();
        var deprecatedConcreteClusterSettings = deprecatedAffixSetting.getAllConcreteSettings(clusterSettings)
            .sorted(Comparator.comparing(Setting::getKey))
            .toList();

        if (deprecatedConcreteNodeSettings.isEmpty() && deprecatedConcreteClusterSettings.isEmpty()) {
            return null;
        }

        List<String> deprecatedNodeSettingKeys = deprecatedConcreteNodeSettings.stream().map(Setting::getKey).toList();
        List<String> deprecatedClusterSettingKeys = deprecatedConcreteClusterSettings.stream().map(Setting::getKey).toList();

        final String concatSettingNames = Stream.concat(deprecatedNodeSettingKeys.stream(), deprecatedClusterSettingKeys.stream())
            .distinct()
            .collect(Collectors.joining(","));
        final String message = String.format(
            Locale.ROOT,
            "The [%s] settings are deprecated and will be removed after 8.0",
            concatSettingNames
        );
        final String details = String.format(Locale.ROOT, detailPattern, concatSettingNames);

        /* Removing affix settings can cause more problems than it's worth, so always make meta null even if the settings are only set
         * dynamically
         */
        final Map<String, Object> meta = null;
        return new DeprecationIssue(warningLevel, message, url, details, false, meta);
    }

    private static DeprecationIssue deprecatedAffixGroupedSetting(
        Setting.AffixSetting<Settings> deprecatedAffixSetting,
        String detailPattern,
        String url,
        DeprecationIssue.Level warningLevel,
        Settings clusterSettings,
        Settings nodeSettings
    ) {
        List<Setting<Settings>> deprecatedConcreteNodeSettings = deprecatedAffixSetting.getAllConcreteSettings(nodeSettings)
            .sorted(Comparator.comparing(Setting::getKey))
            .toList();
        List<Setting<Settings>> deprecatedConcreteClusterSettings = deprecatedAffixSetting.getAllConcreteSettings(clusterSettings)
            .sorted(Comparator.comparing(Setting::getKey))
            .toList();

        if (deprecatedConcreteNodeSettings.isEmpty() && deprecatedConcreteClusterSettings.isEmpty()) {
            return null;
        }

        // The concrete setting names that are the root of the grouped settings (with asterisk appended for display)
        final String groupSettingNames = Stream.concat(deprecatedConcreteNodeSettings.stream(), deprecatedConcreteClusterSettings.stream())
            .map(Setting::getKey)
            .distinct()
            .map(key -> key + "*")
            .collect(Collectors.joining(","));
        // The actual group setting that are present in the settings objects, with full setting name prepended.
        List<String> allNodeSubSettingKeys = deprecatedConcreteNodeSettings.stream().flatMap(affixSetting -> {
            String groupPrefix = affixSetting.getKey();
            Settings groupSettings = affixSetting.get(nodeSettings);
            Set<String> subSettings = groupSettings.keySet();
            return subSettings.stream().map(key -> groupPrefix + key);
        }).sorted().toList();

        List<String> allClusterSubSettingKeys = deprecatedConcreteClusterSettings.stream().flatMap(affixSetting -> {
            String groupPrefix = affixSetting.getKey();
            Settings groupSettings = affixSetting.get(clusterSettings);
            Set<String> subSettings = groupSettings.keySet();
            return subSettings.stream().map(key -> groupPrefix + key);
        }).sorted().toList();

        final String allSubSettings = Stream.concat(allNodeSubSettingKeys.stream(), allClusterSubSettingKeys.stream())
            .distinct()
            .sorted()
            .collect(Collectors.joining(","));

        final String message = String.format(
            Locale.ROOT,
            "The [%s] settings are deprecated and will be removed after 8.0",
            groupSettingNames
        );
        final String details = String.format(Locale.ROOT, detailPattern, allSubSettings);
        /* Removing affix settings can cause more problems than it's worth, so always make meta null even if the settings are only set
         * dynamically
         */
        final Map<String, Object> meta = null;
        return new DeprecationIssue(warningLevel, message, url, details, false, meta);
    }

    private static final String MONITORING_SETTING_DEPRECATION_LINK = "https://ela.st/es-deprecation-7-monitoring-settings";
    private static final String MONITORING_SETTING_REMOVAL_TIME = "after 8.0";

    static DeprecationIssue genericMonitoringSetting(
        final ClusterState clusterState,
        final Settings nodeSettings,
        final Setting<?> deprecated
    ) {
        return checkDeprecatedSetting(
            clusterState.metadata().settings(),
            nodeSettings,
            deprecated,
            MONITORING_SETTING_DEPRECATION_LINK,
            MONITORING_SETTING_REMOVAL_TIME
        );
    }

    static DeprecationIssue genericMonitoringAffixSetting(
        final Settings clusterSettings,
        final Settings nodeSettings,
        final String deprecatedSuffix
    ) {
        return deprecatedAffixSetting(
            Setting.affixKeySetting(
                "xpack.monitoring.exporters.",
                deprecatedSuffix,
                (Function<String, Setting<String>>) Setting::simpleString
            ),
            "Remove the following settings: [%s]",
            MONITORING_SETTING_DEPRECATION_LINK,
            DeprecationIssue.Level.WARNING,
            clusterSettings,
            nodeSettings
        );
    }

    static DeprecationIssue genericMonitoringAffixSecureSetting(
        final Settings clusterSettings,
        final Settings nodeSettings,
        final String deprecatedSuffix
    ) {
        return deprecatedAffixSetting(
            Setting.affixKeySetting("xpack.monitoring.exporters.", deprecatedSuffix, k -> SecureSetting.secureString(k, null)),
            "Remove the following settings from the keystore: [%s]",
            MONITORING_SETTING_DEPRECATION_LINK,
            DeprecationIssue.Level.WARNING,
            clusterSettings,
            nodeSettings
        );
    }

    static DeprecationIssue genericMonitoringAffixGroupedSetting(
        final Settings clusterSettings,
        final Settings nodeSettings,
        final String deprecatedSuffix
    ) {
        return deprecatedAffixGroupedSetting(
            Setting.affixKeySetting("xpack.monitoring.exporters.", deprecatedSuffix, k -> Setting.groupSetting(k + ".")),
            "Remove the following settings: [%s]",
            MONITORING_SETTING_DEPRECATION_LINK,
            DeprecationIssue.Level.WARNING,
            clusterSettings,
            nodeSettings
        );
    }

    static DeprecationIssue checkMonitoringSettingHistoryDuration(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringSetting(clusterState, settings, Setting.simpleString("xpack.monitoring.history.duration"));
    }

    static DeprecationIssue checkMonitoringSettingCollectIndexRecovery(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringSetting(
            clusterState,
            settings,
            Setting.simpleString("xpack.monitoring.collection.index.recovery.active_only")
        );
    }

    static DeprecationIssue checkMonitoringSettingCollectIndices(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringSetting(clusterState, settings, Setting.simpleString("xpack.monitoring.collection.indices"));
    }

    static DeprecationIssue checkMonitoringSettingCollectCcrTimeout(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringSetting(clusterState, settings, Setting.simpleString("xpack.monitoring.collection.ccr.stats.timeout"));
    }

    static DeprecationIssue checkMonitoringSettingCollectEnrichStatsTimeout(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringSetting(clusterState, settings, Setting.simpleString("xpack.monitoring.collection.enrich.stats.timeout"));
    }

    static DeprecationIssue checkMonitoringSettingCollectIndexRecoveryStatsTimeout(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringSetting(clusterState, settings, Setting.simpleString("xpack.monitoring.collection.index.recovery.timeout"));
    }

    static DeprecationIssue checkMonitoringSettingCollectIndexStatsTimeout(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringSetting(clusterState, settings, Setting.simpleString("xpack.monitoring.collection.index.stats.timeout"));
    }

    static DeprecationIssue checkMonitoringSettingCollectMlJobStatsTimeout(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringSetting(clusterState, settings, Setting.simpleString("xpack.monitoring.collection.ml.job.stats.timeout"));
    }

    static DeprecationIssue checkMonitoringSettingCollectNodeStatsTimeout(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringSetting(clusterState, settings, Setting.simpleString("xpack.monitoring.collection.node.stats.timeout"));
    }

    static DeprecationIssue checkMonitoringSettingCollectClusterStatsTimeout(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringSetting(clusterState, settings, Setting.simpleString("xpack.monitoring.collection.cluster.stats.timeout"));
    }

    static DeprecationIssue checkMonitoringSettingExportersHost(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringAffixSetting(clusterState.metadata().settings(), settings, "host");
    }

    static DeprecationIssue checkMonitoringSettingExportersBulkTimeout(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringAffixSetting(clusterState.metadata().settings(), settings, "bulk.timeout");
    }

    static DeprecationIssue checkMonitoringSettingExportersConnectionTimeout(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringAffixSetting(clusterState.metadata().settings(), settings, "connection.timeout");
    }

    static DeprecationIssue checkMonitoringSettingExportersConnectionReadTimeout(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringAffixSetting(clusterState.metadata().settings(), settings, "connection.read_timeout");
    }

    static DeprecationIssue checkMonitoringSettingExportersAuthUsername(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringAffixSetting(clusterState.metadata().settings(), settings, "auth.username");
    }

    static DeprecationIssue checkMonitoringSettingExportersAuthPass(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringAffixSecureSetting(clusterState.metadata().settings(), settings, "auth.secure_password");
    }

    static DeprecationIssue checkMonitoringSettingExportersSSL(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringAffixGroupedSetting(clusterState.metadata().settings(), settings, "ssl");
    }

    static DeprecationIssue checkMonitoringSettingExportersProxyBase(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringAffixSetting(clusterState.metadata().settings(), settings, "proxy.base_path");
    }

    static DeprecationIssue checkMonitoringSettingExportersSniffEnabled(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringAffixSetting(clusterState.metadata().settings(), settings, "sniff.enabled");
    }

    static DeprecationIssue checkMonitoringSettingExportersHeaders(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringAffixGroupedSetting(clusterState.metadata().settings(), settings, "headers");
    }

    static DeprecationIssue checkMonitoringSettingExportersTemplateTimeout(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringAffixSetting(clusterState.metadata().settings(), settings, "index.template.master_timeout");
    }

    static DeprecationIssue checkMonitoringSettingExportersMasterTimeout(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringAffixSetting(clusterState.metadata().settings(), settings, "wait_master.timeout");
    }

    static DeprecationIssue checkMonitoringSettingExportersEnabled(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringAffixSetting(clusterState.metadata().settings(), settings, "enabled");
    }

    static DeprecationIssue checkMonitoringSettingExportersType(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringAffixSetting(clusterState.metadata().settings(), settings, "type");
    }

    static DeprecationIssue checkMonitoringSettingExportersAlertsEnabled(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringAffixSetting(clusterState.metadata().settings(), settings, "cluster_alerts.management.enabled");
    }

    static DeprecationIssue checkMonitoringSettingExportersAlertsBlacklist(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringAffixSetting(clusterState.metadata().settings(), settings, "cluster_alerts.management.blacklist");
    }

    static DeprecationIssue checkMonitoringSettingExportersIndexNameTimeFormat(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringAffixSetting(clusterState.metadata().settings(), settings, "index.name.time_format");
    }

    static DeprecationIssue checkMonitoringSettingDecommissionAlerts(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringSetting(clusterState, settings, Setting.simpleString("xpack.monitoring.migration.decommission_alerts"));
    }

    static DeprecationIssue checkMonitoringSettingEsCollectionEnabled(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringSetting(clusterState, settings, Setting.simpleString("xpack.monitoring.elasticsearch.collection.enabled"));
    }

    static DeprecationIssue checkMonitoringSettingCollectionEnabled(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringSetting(clusterState, settings, Setting.simpleString("xpack.monitoring.collection.enabled"));
    }

    static DeprecationIssue checkMonitoringSettingCollectionInterval(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringSetting(clusterState, settings, Setting.simpleString("xpack.monitoring.collection.interval"));
    }

    static DeprecationIssue checkExporterUseIngestPipelineSettings(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return deprecatedAffixSetting(
            MonitoringDeprecatedSettings.USE_INGEST_PIPELINE_SETTING,
            "Remove the following settings: [%s]",
            "https://ela.st/es-deprecation-7-monitoring-exporter-use-ingest-setting",
            DeprecationIssue.Level.WARNING,
            clusterState.metadata().settings(),
            settings
        );
    }

    static DeprecationIssue checkExporterPipelineMasterTimeoutSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return deprecatedAffixSetting(
            MonitoringDeprecatedSettings.PIPELINE_CHECK_TIMEOUT_SETTING,
            "Remove the following settings: [%s]",
            "https://ela.st/es-deprecation-7-monitoring-exporter-pipeline-timeout-setting",
            DeprecationIssue.Level.WARNING,
            clusterState.metadata().settings(),
            settings
        );
    }

    static DeprecationIssue checkExporterCreateLegacyTemplateSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return deprecatedAffixSetting(
            MonitoringDeprecatedSettings.TEMPLATE_CREATE_LEGACY_VERSIONS_SETTING,
            "Remove the following settings: [%s]",
            "https://ela.st/es-deprecation-7-monitoring-exporter-create-legacy-template-setting",
            DeprecationIssue.Level.WARNING,
            clusterState.metadata().settings(),
            settings
        );
    }

    static DeprecationIssue checkScriptContextCache(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        if (ScriptService.isUseContextCacheSet(settings)) {
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                ScriptService.USE_CONTEXT_RATE_KEY_DEPRECATION_MESSAGE,
                "https://ela.st/es-deprecation-7-script-context-cache",
                "found deprecated script context caches in use, change setting to compilation rate or remove "
                    + "setting to use the default",
                false,
                null
            );
        }
        return null;
    }

    static DeprecationIssue checkScriptContextCompilationsRateLimitSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting.AffixSetting<?> maxSetting = ScriptService.SCRIPT_MAX_COMPILATIONS_RATE_SETTING;
        Set<String> contextCompilationRates = maxSetting.getAsMap(settings).keySet();
        if (ScriptService.isImplicitContextCacheSet(settings)) {
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                ScriptService.contextDeprecationMessage(settings),
                "https://ela.st/es-deprecation-7-script-context-cache",
                "Remove the context-specific cache settings and set [script.max_compilations_rate] to configure the rate limit for the "
                    + "general cache. If no limit is set, the rate defaults to 150 compilations per five minutes: 150/5m. Context-specific "
                    + "caches are no longer needed to prevent system scripts from triggering rate limits.",
                false,
                null
            );
        } else if (contextCompilationRates.isEmpty() == false) {
            String maxSettings = contextCompilationRates.stream()
                .sorted()
                .map(c -> maxSetting.getConcreteSettingForNamespace(c).getKey())
                .collect(Collectors.joining(","));
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                String.format(
                    Locale.ROOT,
                    "Setting context-specific rate limits [%s] is deprecated."
                        + " Use [%s] to rate limit the compilation of user scripts."
                        + " Context-specific caches are no longer needed to prevent system scripts from triggering rate limits.",
                    maxSettings,
                    ScriptService.SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey()
                ),
                "https://ela.st/es-deprecation-7-script-context-cache",
                String.format(Locale.ROOT, "[%s] is deprecated and will be removed in a future release", maxSettings),
                false,
                null
            );
        }
        return null;
    }

    static DeprecationIssue checkScriptContextCacheSizeSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting.AffixSetting<?> cacheSizeSetting = ScriptService.SCRIPT_CACHE_SIZE_SETTING;
        Set<String> contextCacheSizes = cacheSizeSetting.getAsMap(settings).keySet();
        if (contextCacheSizes.isEmpty() == false) {
            String cacheSizeSettings = contextCacheSizes.stream()
                .sorted()
                .map(c -> cacheSizeSetting.getConcreteSettingForNamespace(c).getKey())
                .collect(Collectors.joining(","));
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                String.format(
                    Locale.ROOT,
                    "Setting a context-specific cache size [%s] is deprecated."
                        + " Use [%s] to configure the size of the general cache for scripts."
                        + " Context-specific caches are no longer needed to prevent system scripts from triggering rate limits.",
                    cacheSizeSettings,
                    ScriptService.SCRIPT_GENERAL_CACHE_SIZE_SETTING.getKey()
                ),
                "https://ela.st/es-deprecation-7-script-context-cache",
                String.format(Locale.ROOT, "[%s] is deprecated and will be removed in a future release", cacheSizeSettings),
                false,
                null
            );
        }
        return null;
    }

    static DeprecationIssue checkScriptContextCacheExpirationSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting.AffixSetting<?> cacheExpireSetting = ScriptService.SCRIPT_CACHE_EXPIRE_SETTING;
        Set<String> contextCacheExpires = cacheExpireSetting.getAsMap(settings).keySet();
        if (contextCacheExpires.isEmpty() == false) {
            String cacheExpireSettings = contextCacheExpires.stream()
                .sorted()
                .map(c -> cacheExpireSetting.getConcreteSettingForNamespace(c).getKey())
                .collect(Collectors.joining(","));
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                String.format(
                    Locale.ROOT,
                    "Setting a context-specific cache expiration [%s] is deprecated."
                        + " Use [%s] to configure the expiration of the general cache."
                        + " Context-specific caches are no longer needed to prevent system scripts from triggering rate limits.",
                    cacheExpireSettings,
                    ScriptService.SCRIPT_GENERAL_CACHE_EXPIRE_SETTING.getKey()
                ),
                "https://ela.st/es-deprecation-7-script-context-cache",
                String.format(Locale.ROOT, "[%s] is deprecated and will be removed in a future release", cacheExpireSettings),
                false,
                null
            );
        }
        return null;
    }

    static DeprecationIssue checkEnforceDefaultTierPreferenceSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        if (DataTier.ENFORCE_DEFAULT_TIER_PREFERENCE_SETTING.exists(settings)) {
            String key = DataTier.ENFORCE_DEFAULT_TIER_PREFERENCE_SETTING.getKey();
            return new DeprecationIssue(
                DeprecationIssue.Level.CRITICAL,
                String.format(Locale.ROOT, "setting [%s] is deprecated and will not be available in a future version", key),
                "https://www.elastic.co/guide/en/elasticsearch/reference/current/data-tiers.html",
                String.format(Locale.ROOT, "found [%s] configured. Discontinue use of this setting.", key),
                false,
                null
            );
        }

        return null;
    }

    static DeprecationIssue checkLifecyleStepMasterTimeoutSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<TimeValue> deprecatedSetting = LifecycleSettings.LIFECYCLE_STEP_MASTER_TIMEOUT_SETTING;
        String url = "https://ela.st/es-deprecation-8-lifecycle-master-timeout-setting";
        return checkRemovedSetting(
            clusterState.metadata().settings(),
            settings,
            deprecatedSetting,
            url,
            "As of 7.16 the timeout is always infinite.",
            DeprecationIssue.Level.WARNING
        );
    }

    static DeprecationIssue checkEqlEnabledSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<Boolean> deprecatedSetting = Setting.boolSetting(
            "xpack.eql.enabled",
            true,
            Setting.Property.NodeScope,
            Setting.Property.DeprecatedWarning
        );
        String url = "https://ela.st/es-deprecation-7-eql-enabled-setting";
        return checkRemovedSetting(
            clusterState.metadata().settings(),
            settings,
            deprecatedSetting,
            url,
            "As of 7.9.2 basic license level features are always enabled.",
            DeprecationIssue.Level.WARNING
        );
    }

    static DeprecationIssue checkNodeAttrData(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        String nodeAttrDataValue = settings.get("node.attr.data");
        if (nodeAttrDataValue == null) {
            return null;
        }
        return new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Setting node.attributes.data is not recommended",
            "https://ela.st/es-deprecation-7-node-attr-data-setting",
            "One or more of your nodes is configured with node.attributes.data settings. This is typically used to create a "
                + "hot/warm or tiered architecture, based on legacy guidelines. Data tiers are a recommended replacement for tiered "
                + "architecture clusters.",
            false,
            null
        );
    }

    static DeprecationIssue checkWatcherBulkConcurrentRequestsSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<Integer> deprecatedSetting = Setting.intSetting(
            "xpack.watcher.bulk.concurrent_requests",
            0,
            0,
            20,
            Setting.Property.NodeScope,
            Setting.Property.Deprecated
        );
        String url = "https://ela.st/es-deprecation-8-watcher-settings";
        return checkRemovedSetting(
            clusterState.metadata().settings(),
            settings,
            deprecatedSetting,
            url,
            "As of 8.8.0 this setting is ignored.",
            DeprecationIssue.Level.WARNING
        );
    }

    static DeprecationIssue checkTracingApmSettings(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        String url = "https://ela.st/es-deprecation-9-tracing-apm-settings";
        Setting.Property[] properties = { NodeScope, OperatorDynamic, Deprecated };
        List<Setting<?>> tracingApmSettings = List.of(
            Setting.prefixKeySetting("tracing.apm.agent.", key -> Setting.simpleString(key, properties)),
            Setting.stringListSetting("tracing.apm.names.include", properties),
            Setting.stringListSetting("tracing.apm.names.exclude", properties),
            Setting.stringListSetting("tracing.apm.sanitize_field_names", properties),
            Setting.boolSetting("tracing.apm.enabled", false, properties),
            SecureSetting.secureString("tracing.apm.api_key", null, Deprecated),
            SecureSetting.secureString("tracing.apm.secret_token", null, Deprecated)
        );
        return checkMultipleRemovedSettings(
            clusterState.metadata().settings(),
            settings,
            tracingApmSettings,
            url,
            "[tracing.apm.*] settings are no longer accepted as of 9.0.0"
                + " and should be replaced by [telemetry.*] or [telemetry.tracing.*] settings.",
            DeprecationIssue.Level.CRITICAL
        );
    }

    @FunctionalInterface
    public interface NodeDeprecationCheck<A, B, C, D, R> {
        R apply(A first, B second, C third, D fourth);
    }
}

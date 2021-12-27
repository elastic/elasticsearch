/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.monitoring.MonitoringDeprecatedSettings;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.security.authc.RealmSettings.RESERVED_REALM_NAME_PREFIX;

public class NodeDeprecationChecks {

    static DeprecationIssue checkDeprecatedSetting(
        final Settings settings,
        final Setting<?> deprecatedSetting,
        final String url,
        final String whenRemoved
    ) {
        if (deprecatedSetting.exists(settings) == false) {
            return null;
        }
        final String deprecatedSettingKey = deprecatedSetting.getKey();
        final String value = deprecatedSetting.get(settings).toString();
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

    static DeprecationIssue checkRemovedSetting(final Settings settings, final Setting<?> removedSetting, final String url) {
        return checkRemovedSetting(settings, removedSetting, url, null, DeprecationIssue.Level.CRITICAL);
    }

    static DeprecationIssue checkRemovedSetting(
        final Settings settings,
        final Setting<?> removedSetting,
        final String url,
        String additionalDetailMessage,
        DeprecationIssue.Level deprecationLevel
    ) {
        if (removedSetting.exists(settings) == false) {
            return null;
        }
        final String removedSettingKey = removedSetting.getKey();
        Object removedSettingValue = removedSetting.get(settings);
        String value;
        if (removedSettingValue instanceof TimeValue) {
            value = ((TimeValue) removedSettingValue).getStringRep();
        } else {
            value = removedSettingValue.toString();
        }
        final String message = String.format(Locale.ROOT, "Setting [%s] is deprecated", removedSettingKey);
        final String details = additionalDetailMessage == null
            ? String.format(Locale.ROOT, "Remove the [%s] setting.", removedSettingKey)
            : String.format(Locale.ROOT, "Remove the [%s] setting. %s", removedSettingKey, additionalDetailMessage);
        return new DeprecationIssue(deprecationLevel, message, url, details, false, null);
    }

    static DeprecationIssue checkSharedDataPathSetting(final Settings settings, final PluginsAndModules pluginsAndModules) {
        if (Environment.PATH_SHARED_DATA_SETTING.exists(settings)) {
            final String message = String.format(
                Locale.ROOT,
                "setting [%s] is deprecated and will be removed in a future version",
                Environment.PATH_SHARED_DATA_SETTING.getKey()
            );
            final String url = "https://www.elastic.co/guide/en/elasticsearch/reference/7.13/"
                + "breaking-changes-7.13.html#deprecate-shared-data-path-setting";
            final String details = "Found shared data path configured. Discontinue use of this setting.";
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, details, false, null);
        }
        return null;
    }

    static DeprecationIssue checkReservedPrefixedRealmNames(final Settings settings, final PluginsAndModules pluginsAndModules) {
        final Map<RealmConfig.RealmIdentifier, Settings> realmSettings = RealmSettings.getRealmSettings(settings);
        if (realmSettings.isEmpty()) {
            return null;
        }
        List<RealmConfig.RealmIdentifier> reservedPrefixedRealmIdentifiers = new ArrayList<>();
        for (RealmConfig.RealmIdentifier realmIdentifier : realmSettings.keySet()) {
            if (realmIdentifier.getName().startsWith(RESERVED_REALM_NAME_PREFIX)) {
                reservedPrefixedRealmIdentifiers.add(realmIdentifier);
            }
        }
        if (reservedPrefixedRealmIdentifiers.isEmpty()) {
            return null;
        } else {
            return new DeprecationIssue(
                DeprecationIssue.Level.CRITICAL,
                "Realm that start with [" + RESERVED_REALM_NAME_PREFIX + "] will not be permitted in a future major release.",
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.14/deprecated-7.14.html#reserved-prefixed-realm-names",
                String.format(
                    Locale.ROOT,
                    "Found realm "
                        + (reservedPrefixedRealmIdentifiers.size() == 1 ? "name" : "names")
                        + " with reserved prefix [%s]: [%s]. "
                        + "In a future major release, node will fail to start if any realm names start with reserved prefix.",
                    RESERVED_REALM_NAME_PREFIX,
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

    static DeprecationIssue checkSingleDataNodeWatermarkSetting(final Settings settings, final PluginsAndModules pluginsAndModules) {
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
        Settings settings
    ) {
        List<Setting<?>> deprecatedConcreteSettings = deprecatedAffixSetting.getAllConcreteSettings(settings)
            .sorted(Comparator.comparing(Setting::getKey))
            .collect(Collectors.toList());

        if (deprecatedConcreteSettings.isEmpty()) {
            return null;
        }

        final String concatSettingNames = deprecatedConcreteSettings.stream().map(Setting::getKey).collect(Collectors.joining(","));
        final String message = String.format(
            Locale.ROOT,
            "The [%s] settings are deprecated and will be removed after 8.0",
            concatSettingNames
        );
        final String details = String.format(Locale.ROOT, detailPattern, concatSettingNames);

        return new DeprecationIssue(warningLevel, message, url, details, false, null);
    }

    private static DeprecationIssue deprecatedAffixGroupedSetting(
        Setting.AffixSetting<Settings> deprecatedAffixSetting,
        String detailPattern,
        String url,
        DeprecationIssue.Level warningLevel,
        Settings settings
    ) {
        List<Setting<Settings>> deprecatedConcreteSettings = deprecatedAffixSetting.getAllConcreteSettings(settings)
            .sorted(Comparator.comparing(Setting::getKey))
            .collect(Collectors.toList());

        if (deprecatedConcreteSettings.isEmpty()) {
            return null;
        }

        // The concrete setting names that are the root of the grouped settings (with asterisk appended for display)
        final String groupSettingNames = deprecatedConcreteSettings.stream()
            .map(Setting::getKey)
            .map(key -> key + "*")
            .collect(Collectors.joining(","));
        // The actual group setting that are present in the settings object, with full setting name prepended.
        String allSubSettings = deprecatedConcreteSettings.stream().map(affixSetting -> {
            String groupPrefix = affixSetting.getKey();
            Settings groupSettings = affixSetting.get(settings);
            Set<String> subSettings = groupSettings.keySet();
            return subSettings.stream().map(key -> groupPrefix + key).collect(Collectors.joining(","));
        }).collect(Collectors.joining(";"));

        final String message = String.format(
            Locale.ROOT,
            "The [%s] settings are deprecated and will be removed after 8.0",
            groupSettingNames
        );
        final String details = String.format(Locale.ROOT, detailPattern, allSubSettings);

        return new DeprecationIssue(warningLevel, message, url, details, false, null);
    }

    private static final String MONITORING_SETTING_DEPRECATION_LINK = "https://ela.st/es-deprecation-7-monitoring-settings";
    private static final String MONITORING_SETTING_REMOVAL_TIME = "after 8.0";

    static DeprecationIssue genericMonitoringSetting(final Settings settings, final Setting<?> deprecated) {
        return checkDeprecatedSetting(settings, deprecated, MONITORING_SETTING_DEPRECATION_LINK, MONITORING_SETTING_REMOVAL_TIME);
    }

    static DeprecationIssue genericMonitoringAffixSetting(final Settings settings, final String deprecatedSuffix) {
        return deprecatedAffixSetting(
            Setting.affixKeySetting(
                "xpack.monitoring.exporters.",
                deprecatedSuffix,
                (Function<String, Setting<String>>) Setting::simpleString
            ),
            "Remove the following settings from elasticsearch.yml: [%s]",
            MONITORING_SETTING_DEPRECATION_LINK,
            DeprecationIssue.Level.WARNING,
            settings
        );
    }

    static DeprecationIssue genericMonitoringAffixSecureSetting(final Settings settings, final String deprecatedSuffix) {
        return deprecatedAffixSetting(
            Setting.affixKeySetting("xpack.monitoring.exporters.", deprecatedSuffix, k -> SecureSetting.secureString(k, null)),
            "Remove the following settings from the keystore: [%s]",
            MONITORING_SETTING_DEPRECATION_LINK,
            DeprecationIssue.Level.WARNING,
            settings
        );
    }

    static DeprecationIssue genericMonitoringAffixGroupedSetting(final Settings settings, final String deprecatedSuffix) {
        return deprecatedAffixGroupedSetting(
            Setting.affixKeySetting("xpack.monitoring.exporters.", deprecatedSuffix, k -> Setting.groupSetting(k + ".")),
            "Remove the following settings from elasticsearch.yml: [%s]",
            MONITORING_SETTING_DEPRECATION_LINK,
            DeprecationIssue.Level.WARNING,
            settings
        );
    }

    static DeprecationIssue checkMonitoringSettingHistoryDuration(final Settings settings, final PluginsAndModules pluginsAndModules) {
        return genericMonitoringSetting(settings, Setting.simpleString("xpack.monitoring.history.duration"));
    }

    static DeprecationIssue checkMonitoringSettingCollectIndexRecovery(final Settings settings, final PluginsAndModules pluginsAndModules) {
        return genericMonitoringSetting(settings, Setting.simpleString("xpack.monitoring.collection.index.recovery.active_only"));
    }

    static DeprecationIssue checkMonitoringSettingCollectIndices(final Settings settings, final PluginsAndModules pluginsAndModules) {
        return genericMonitoringSetting(settings, Setting.simpleString("xpack.monitoring.collection.indices"));
    }

    static DeprecationIssue checkMonitoringSettingCollectCcrTimeout(final Settings settings, final PluginsAndModules pluginsAndModules) {
        return genericMonitoringSetting(settings, Setting.simpleString("xpack.monitoring.collection.ccr.stats.timeout"));
    }

    static DeprecationIssue checkMonitoringSettingCollectEnrichStatsTimeout(
        final Settings settings,
        final PluginsAndModules pluginsAndModules
    ) {
        return genericMonitoringSetting(settings, Setting.simpleString("xpack.monitoring.collection.enrich.stats.timeout"));
    }

    static DeprecationIssue checkMonitoringSettingCollectIndexRecoveryStatsTimeout(
        final Settings settings,
        final PluginsAndModules pluginsAndModules
    ) {
        return genericMonitoringSetting(settings, Setting.simpleString("xpack.monitoring.collection.index.recovery.timeout"));
    }

    static DeprecationIssue checkMonitoringSettingCollectIndexStatsTimeout(
        final Settings settings,
        final PluginsAndModules pluginsAndModules
    ) {
        return genericMonitoringSetting(settings, Setting.simpleString("xpack.monitoring.collection.index.stats.timeout"));
    }

    static DeprecationIssue checkMonitoringSettingCollectMlJobStatsTimeout(
        final Settings settings,
        final PluginsAndModules pluginsAndModules
    ) {
        return genericMonitoringSetting(settings, Setting.simpleString("xpack.monitoring.collection.ml.job.stats.timeout"));
    }

    static DeprecationIssue checkMonitoringSettingCollectNodeStatsTimeout(
        final Settings settings,
        final PluginsAndModules pluginsAndModules
    ) {
        return genericMonitoringSetting(settings, Setting.simpleString("xpack.monitoring.collection.node.stats.timeout"));
    }

    static DeprecationIssue checkMonitoringSettingCollectClusterStatsTimeout(
        final Settings settings,
        final PluginsAndModules pluginsAndModules
    ) {
        return genericMonitoringSetting(settings, Setting.simpleString("xpack.monitoring.collection.cluster.stats.timeout"));
    }

    static DeprecationIssue checkMonitoringSettingExportersHost(final Settings settings, final PluginsAndModules pluginsAndModules) {
        return genericMonitoringAffixSetting(settings, "host");
    }

    static DeprecationIssue checkMonitoringSettingExportersBulkTimeout(final Settings settings, final PluginsAndModules pluginsAndModules) {
        return genericMonitoringAffixSetting(settings, "bulk.timeout");
    }

    static DeprecationIssue checkMonitoringSettingExportersConnectionTimeout(
        final Settings settings,
        final PluginsAndModules pluginsAndModules
    ) {
        return genericMonitoringAffixSetting(settings, "connection.timeout");
    }

    static DeprecationIssue checkMonitoringSettingExportersConnectionReadTimeout(
        final Settings settings,
        final PluginsAndModules pluginsAndModules
    ) {
        return genericMonitoringAffixSetting(settings, "connection.read_timeout");
    }

    static DeprecationIssue checkMonitoringSettingExportersAuthUsername(
        final Settings settings,
        final PluginsAndModules pluginsAndModules
    ) {
        return genericMonitoringAffixSetting(settings, "auth.username");
    }

    static DeprecationIssue checkMonitoringSettingExportersAuthPass(final Settings settings, final PluginsAndModules pluginsAndModules) {
        return genericMonitoringAffixSecureSetting(settings, "auth.secure_password");
    }

    static DeprecationIssue checkMonitoringSettingExportersSSL(final Settings settings, final PluginsAndModules pluginsAndModules) {
        return genericMonitoringAffixGroupedSetting(settings, "ssl");
    }

    static DeprecationIssue checkMonitoringSettingExportersProxyBase(final Settings settings, final PluginsAndModules pluginsAndModules) {
        return genericMonitoringAffixSetting(settings, "proxy.base_path");
    }

    static DeprecationIssue checkMonitoringSettingExportersSniffEnabled(
        final Settings settings,
        final PluginsAndModules pluginsAndModules
    ) {
        return genericMonitoringAffixSetting(settings, "sniff.enabled");
    }

    static DeprecationIssue checkMonitoringSettingExportersHeaders(final Settings settings, final PluginsAndModules pluginsAndModules) {
        return genericMonitoringAffixGroupedSetting(settings, "headers");
    }

    static DeprecationIssue checkMonitoringSettingExportersTemplateTimeout(
        final Settings settings,
        final PluginsAndModules pluginsAndModules
    ) {
        return genericMonitoringAffixSetting(settings, "index.template.master_timeout");
    }

    static DeprecationIssue checkMonitoringSettingExportersMasterTimeout(
        final Settings settings,
        final PluginsAndModules pluginsAndModules
    ) {
        return genericMonitoringAffixSetting(settings, "wait_master.timeout");
    }

    static DeprecationIssue checkMonitoringSettingExportersEnabled(final Settings settings, final PluginsAndModules pluginsAndModules) {
        return genericMonitoringAffixSetting(settings, "enabled");
    }

    static DeprecationIssue checkMonitoringSettingExportersType(final Settings settings, final PluginsAndModules pluginsAndModules) {
        return genericMonitoringAffixSetting(settings, "type");
    }

    static DeprecationIssue checkMonitoringSettingExportersAlertsEnabled(
        final Settings settings,
        final PluginsAndModules pluginsAndModules
    ) {
        return genericMonitoringAffixSetting(settings, "cluster_alerts.management.enabled");
    }

    static DeprecationIssue checkMonitoringSettingExportersAlertsBlacklist(
        final Settings settings,
        final PluginsAndModules pluginsAndModules
    ) {
        return genericMonitoringAffixSetting(settings, "cluster_alerts.management.blacklist");
    }

    static DeprecationIssue checkMonitoringSettingExportersIndexNameTimeFormat(
        final Settings settings,
        final PluginsAndModules pluginsAndModules
    ) {
        return genericMonitoringAffixSetting(settings, "index.name.time_format");
    }

    static DeprecationIssue checkMonitoringSettingDecommissionAlerts(final Settings settings, final PluginsAndModules pluginsAndModules) {
        return genericMonitoringSetting(settings, Setting.simpleString("xpack.monitoring.migration.decommission_alerts"));
    }

    static DeprecationIssue checkMonitoringSettingEsCollectionEnabled(final Settings settings, final PluginsAndModules pluginsAndModules) {
        return genericMonitoringSetting(settings, Setting.simpleString("xpack.monitoring.elasticsearch.collection.enabled"));
    }

    static DeprecationIssue checkMonitoringSettingCollectionEnabled(final Settings settings, final PluginsAndModules pluginsAndModules) {
        return genericMonitoringSetting(settings, Setting.simpleString("xpack.monitoring.collection.enabled"));
    }

    static DeprecationIssue checkMonitoringSettingCollectionInterval(final Settings settings, final PluginsAndModules pluginsAndModules) {
        return genericMonitoringSetting(settings, Setting.simpleString("xpack.monitoring.collection.interval"));
    }

    static DeprecationIssue checkExporterUseIngestPipelineSettings(final Settings settings, final PluginsAndModules pluginsAndModules) {
        return deprecatedAffixSetting(
            MonitoringDeprecatedSettings.USE_INGEST_PIPELINE_SETTING,
            "Remove the following settings from elasticsearch.yml: [%s]",
            "https://ela.st/es-deprecation-7-monitoring-exporter-use-ingest-setting",
            DeprecationIssue.Level.WARNING,
            settings
        );
    }

    static DeprecationIssue checkExporterPipelineMasterTimeoutSetting(final Settings settings, final PluginsAndModules pluginsAndModules) {
        return deprecatedAffixSetting(
            MonitoringDeprecatedSettings.PIPELINE_CHECK_TIMEOUT_SETTING,
            "Remove the following settings from elasticsearch.yml: [%s]",
            "https://ela.st/es-deprecation-7-monitoring-exporter-pipeline-timeout-setting",
            DeprecationIssue.Level.WARNING,
            settings
        );
    }

    static DeprecationIssue checkExporterCreateLegacyTemplateSetting(final Settings settings, final PluginsAndModules pluginsAndModules) {
        return deprecatedAffixSetting(
            MonitoringDeprecatedSettings.TEMPLATE_CREATE_LEGACY_VERSIONS_SETTING,
            "Remove the following settings from elasticsearch.yml: [%s]",
            "https://ela.st/es-deprecation-7-monitoring-exporter-create-legacy-template-setting",
            DeprecationIssue.Level.WARNING,
            settings
        );
    }

    static DeprecationIssue checkScriptContextCache(final Settings settings, final PluginsAndModules pluginsAndModules) {
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
        final PluginsAndModules pluginsAndModules
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

    static DeprecationIssue checkScriptContextCacheSizeSetting(final Settings settings, final PluginsAndModules pluginsAndModules) {
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

    static DeprecationIssue checkScriptContextCacheExpirationSetting(final Settings settings, final PluginsAndModules pluginsAndModules) {
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

    static DeprecationIssue checkEnforceDefaultTierPreferenceSetting(final Settings settings, final PluginsAndModules pluginsAndModules) {
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

    static DeprecationIssue checkLifecyleStepMasterTimeoutSetting(final Settings settings, final PluginsAndModules pluginsAndModules) {
        Setting<TimeValue> deprecatedSetting = LifecycleSettings.LIFECYCLE_STEP_MASTER_TIMEOUT_SETTING;
        String url = "https://ela.st/es-deprecation-8-lifecycle-master-timeout-setting";
        return checkRemovedSetting(
            settings,
            deprecatedSetting,
            url,
            "As of 7.16 the timeout is always infinite.",
            DeprecationIssue.Level.WARNING
        );
    }

    static DeprecationIssue checkEqlEnabledSetting(final Settings settings, final PluginsAndModules pluginsAndModules) {
        Setting<Boolean> deprecatedSetting = Setting.boolSetting(
            "xpack.eql.enabled",
            true,
            Setting.Property.NodeScope,
            Setting.Property.DeprecatedWarning
        );
        String url = "https://ela.st/es-deprecation-8-eql-enabled-setting";
        return checkRemovedSetting(
            settings,
            deprecatedSetting,
            url,
            "As of 7.9.2 basic license level features are always enabled.",
            DeprecationIssue.Level.WARNING
        );
    }

}

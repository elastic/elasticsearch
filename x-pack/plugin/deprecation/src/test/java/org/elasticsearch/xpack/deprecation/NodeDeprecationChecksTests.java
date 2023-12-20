/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.deprecation.DeprecationChecks.NODE_SETTINGS_CHECKS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;

public class NodeDeprecationChecksTests extends ESTestCase {

    public void testRemovedSettingNotSet() {
        final Settings clusterSettings = Settings.EMPTY;
        final Settings nodeSettings = Settings.EMPTY;
        final Setting<?> removedSetting = Setting.simpleString("node.removed_setting");
        final DeprecationIssue issue = NodeDeprecationChecks.checkRemovedSetting(
            clusterSettings,
            nodeSettings,
            removedSetting,
            "http://removed-setting.example.com",
            "Some detail.",
            DeprecationIssue.Level.CRITICAL
        );
        assertThat(issue, nullValue());
    }

    public void testRemovedSetting() {
        final Settings clusterSettings = Settings.EMPTY;
        final Settings nodeSettings = Settings.builder().put("node.removed_setting", "value").build();
        final Setting<?> removedSetting = Setting.simpleString("node.removed_setting");
        final DeprecationIssue issue = NodeDeprecationChecks.checkRemovedSetting(
            clusterSettings,
            nodeSettings,
            removedSetting,
            "https://removed-setting.example.com",
            "Some detail.",
            DeprecationIssue.Level.CRITICAL
        );
        assertThat(issue, not(nullValue()));
        assertThat(issue.getLevel(), equalTo(DeprecationIssue.Level.CRITICAL));
        assertThat(issue.getMessage(), equalTo("Setting [node.removed_setting] is deprecated"));
        assertThat(issue.getDetails(), equalTo("Remove the [node.removed_setting] setting. Some detail."));
        assertThat(issue.getUrl(), equalTo("https://removed-setting.example.com"));
    }

    public void testMultipleDataPaths() {
        final Settings settings = Settings.builder().putList("path.data", Arrays.asList("d1", "d2")).build();
        final XPackLicenseState licenseState = new XPackLicenseState(() -> 0);
        final DeprecationIssue issue = NodeDeprecationChecks.checkMultipleDataPaths(settings, null, null, licenseState);
        assertThat(issue, not(nullValue()));
        assertThat(issue.getLevel(), equalTo(DeprecationIssue.Level.WARNING));
        assertThat(issue.getMessage(), equalTo("Specifying multiple data paths is deprecated"));
        assertThat(
            issue.getDetails(),
            equalTo(
                "The [path.data] setting contains a list of paths. Specify a single path as a string. Use RAID or other system level "
                    + "features to utilize multiple disks. If multiple data paths are configured, the node will fail to start in 8.0. "
            )
        );
        String url = "https://ela.st/es-deprecation-7-multiple-paths";
        assertThat(issue.getUrl(), equalTo(url));
    }

    public void testNoMultipleDataPaths() {
        Settings settings = Settings.builder().put("path.data", "data").build();
        final XPackLicenseState licenseState = new XPackLicenseState(() -> 0);
        final DeprecationIssue issue = NodeDeprecationChecks.checkMultipleDataPaths(settings, null, null, licenseState);
        assertThat(issue, nullValue());
    }

    public void testDataPathsList() {
        final Settings settings = Settings.builder().putList("path.data", "d1").build();
        final XPackLicenseState licenseState = new XPackLicenseState(() -> 0);
        final DeprecationIssue issue = NodeDeprecationChecks.checkDataPathsList(settings, null, null, licenseState);
        assertThat(issue, not(nullValue()));
        assertThat(issue.getLevel(), equalTo(DeprecationIssue.Level.WARNING));
        assertThat(issue.getMessage(), equalTo("Multiple data paths are not supported"));
        assertThat(
            issue.getDetails(),
            equalTo(
                "The [path.data] setting contains a list of paths. Specify a single path as a string. Use RAID or other system level "
                    + "features to utilize multiple disks. If multiple data paths are configured, the node will fail to start in 8.0. "
            )
        );
        String url = "https://ela.st/es-deprecation-7-multiple-paths";
        assertThat(issue.getUrl(), equalTo(url));
    }

    public void testNoDataPathsListDefault() {
        final Settings settings = Settings.builder().build();
        final XPackLicenseState licenseState = new XPackLicenseState(() -> 0);
        final DeprecationIssue issue = NodeDeprecationChecks.checkDataPathsList(settings, null, null, licenseState);
        assertThat(issue, nullValue());
    }

    public void testSharedDataPathSetting() {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put(Environment.PATH_SHARED_DATA_SETTING.getKey(), createTempDir())
            .build();

        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            NODE_SETTINGS_CHECKS,
            c -> c.apply(settings, null, ClusterState.EMPTY_STATE, new XPackLicenseState(() -> 0))
        );
        final String expectedUrl =
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.13/breaking-changes-7.13.html#deprecate-shared-data-path-setting";
        assertThat(
            issues,
            contains(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "setting [path.shared_data] is deprecated and will be removed in a future version",
                    expectedUrl,
                    "Found shared data path configured. Discontinue use of this setting.",
                    false,
                    null
                )
            )
        );
    }

    public void testCheckReservedPrefixedRealmNames() {
        final Settings.Builder builder = Settings.builder();
        final boolean invalidFileRealmName = randomBoolean();
        final boolean invalidNativeRealmName = randomBoolean();
        final boolean invalidOtherRealmName = (false == invalidFileRealmName && false == invalidNativeRealmName) || randomBoolean();

        final List<String> invalidRealmNames = new ArrayList<>();

        final String fileRealmName = randomAlphaOfLengthBetween(4, 12);
        if (invalidFileRealmName) {
            builder.put("xpack.security.authc.realms.file." + "_" + fileRealmName + ".order", -20);
            invalidRealmNames.add("xpack.security.authc.realms.file." + "_" + fileRealmName);
        } else {
            builder.put("xpack.security.authc.realms.file." + fileRealmName + ".order", -20);
        }

        final String nativeRealmName = randomAlphaOfLengthBetween(4, 12);
        if (invalidNativeRealmName) {
            builder.put("xpack.security.authc.realms.native." + "_" + nativeRealmName + ".order", -10);
            invalidRealmNames.add("xpack.security.authc.realms.native." + "_" + nativeRealmName);
        } else {
            builder.put("xpack.security.authc.realms.native." + nativeRealmName + ".order", -10);
        }

        final int otherRealmId = randomIntBetween(0, 9);
        final String otherRealmName = randomAlphaOfLengthBetween(4, 12);
        if (invalidOtherRealmName) {
            builder.put("xpack.security.authc.realms.type_" + otherRealmId + "." + "_" + otherRealmName + ".order", 0);
            invalidRealmNames.add("xpack.security.authc.realms.type_" + otherRealmId + "." + "_" + otherRealmName);
        } else {
            builder.put("xpack.security.authc.realms.type_" + otherRealmId + "." + otherRealmName + ".order", 0);
        }

        final Settings settings = builder.build();
        final List<DeprecationIssue> deprecationIssues = DeprecationChecks.filterChecks(
            NODE_SETTINGS_CHECKS,
            c -> c.apply(settings, null, ClusterState.EMPTY_STATE, new XPackLicenseState(() -> 0))
        );

        assertEquals(1, deprecationIssues.size());

        final DeprecationIssue deprecationIssue = deprecationIssues.get(0);
        assertEquals("Realm that start with [_] will not be permitted in a future major release.", deprecationIssue.getMessage());
        assertEquals(
            "https://www.elastic.co/guide/en/elasticsearch/reference" + "/7.14/deprecated-7.14.html#reserved-prefixed-realm-names",
            deprecationIssue.getUrl()
        );
        assertEquals(
            "Found realm "
                + (invalidRealmNames.size() == 1 ? "name" : "names")
                + " with reserved prefix [_]: ["
                + Strings.collectionToDelimitedString(invalidRealmNames.stream().sorted().collect(Collectors.toList()), "; ")
                + "]. "
                + "In a future major release, node will fail to start if any realm names start with reserved prefix.",
            deprecationIssue.getDetails()
        );
    }

    public void testSingleDataNodeWatermarkSetting() {
        Settings settings = Settings.builder().put(DiskThresholdDecider.ENABLE_FOR_SINGLE_DATA_NODE.getKey(), true).build();

        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            NODE_SETTINGS_CHECKS,
            c -> c.apply(settings, null, ClusterState.EMPTY_STATE, new XPackLicenseState(() -> 0))
        );

        final String expectedUrl = "https://www.elastic.co/guide/en/elasticsearch/reference/7.14/"
            + "breaking-changes-7.14.html#deprecate-single-data-node-watermark";
        assertThat(
            issues,
            hasItem(
                new DeprecationIssue(
                    DeprecationIssue.Level.CRITICAL,
                    "setting [cluster.routing.allocation.disk.watermark.enable_for_single_data_node] is deprecated and"
                        + " will not be available in a future version",
                    expectedUrl,
                    "found [cluster.routing.allocation.disk.watermark.enable_for_single_data_node] configured."
                        + " Discontinue use of this setting.",
                    false,
                    null
                )
            )
        );
    }

    void monitoringSetting(String settingKey, String value) {
        Settings settings = Settings.builder().put(settingKey, value).build();
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            NODE_SETTINGS_CHECKS,
            c -> c.apply(settings, null, ClusterState.EMPTY_STATE, new XPackLicenseState(() -> 0))
        );
        final String expectedUrl = "https://ela.st/es-deprecation-7-monitoring-settings";
        assertThat(
            issues,
            hasItem(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "setting [" + settingKey + "] is deprecated and will be removed after 8.0",
                    expectedUrl,
                    "the setting [" + settingKey + "] is currently set to [" + value + "], remove this setting",
                    false,
                    null
                )
            )
        );
    }

    void monitoringExporterSetting(String suffix, String value) {
        String settingKey = "xpack.monitoring.exporters.test." + suffix;
        Settings settings = Settings.builder().put(settingKey, value).build();
        final XPackLicenseState licenseState = new XPackLicenseState(() -> 0);
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            NODE_SETTINGS_CHECKS,
            c -> c.apply(settings, null, ClusterState.EMPTY_STATE, licenseState)
        );
        final String expectedUrl = "https://ela.st/es-deprecation-7-monitoring-settings";
        assertThat(
            issues,
            hasItem(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "The [" + settingKey + "] settings are deprecated and will be removed after 8.0",
                    expectedUrl,
                    "Remove the following settings: [" + settingKey + "]",
                    false,
                    null
                )
            )
        );
    }

    void monitoringExporterGroupedSetting(String suffix, String value) {
        String settingKey = "xpack.monitoring.exporters.test." + suffix;
        String subSettingKey = settingKey + ".subsetting";
        Settings settings = Settings.builder().put(subSettingKey, value).build();
        final XPackLicenseState licenseState = new XPackLicenseState(() -> 0);
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            NODE_SETTINGS_CHECKS,
            c -> c.apply(settings, null, ClusterState.EMPTY_STATE, licenseState)
        );
        final String expectedUrl = "https://ela.st/es-deprecation-7-monitoring-settings";
        assertThat(
            issues,
            hasItem(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "The [" + settingKey + ".*] settings are deprecated and will be removed after 8.0",
                    expectedUrl,
                    "Remove the following settings: [" + subSettingKey + "]",
                    false,
                    null
                )
            )
        );
    }

    void monitoringExporterSecureSetting(String suffix, String value) {
        String settingKey = "xpack.monitoring.exporters.test." + suffix;
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(settingKey, value);
        Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        final XPackLicenseState licenseState = new XPackLicenseState(() -> 0);
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            NODE_SETTINGS_CHECKS,
            c -> c.apply(settings, null, ClusterState.EMPTY_STATE, licenseState)
        );
        final String expectedUrl = "https://ela.st/es-deprecation-7-monitoring-settings";
        assertThat(
            issues,
            hasItem(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "The [" + settingKey + "] settings are deprecated and will be removed after 8.0",
                    expectedUrl,
                    "Remove the following settings from the keystore: [" + settingKey + "]",
                    false,
                    null
                )
            )
        );
    }

    public void testCheckMonitoringSettingHistoryDuration() {
        monitoringSetting("xpack.monitoring.history.duration", "7d");
    }

    public void testCheckMonitoringSettingCollectIndexRecovery() {
        monitoringSetting("xpack.monitoring.collection.index.recovery.active_only", "true");
    }

    public void testCheckMonitoringSettingCollectIndices() {
        monitoringSetting("xpack.monitoring.collection.indices", "[test1,test2]");
    }

    public void testCheckMonitoringSettingCollectCcrTimeout() {
        monitoringSetting("xpack.monitoring.collection.ccr.stats.timeout", "10s");
    }

    public void testCheckMonitoringSettingCollectEnrichStatsTimeout() {
        monitoringSetting("xpack.monitoring.collection.enrich.stats.timeout", "10s");
    }

    public void testCheckMonitoringSettingCollectIndexRecoveryStatsTimeout() {
        monitoringSetting("xpack.monitoring.collection.index.recovery.timeout", "10s");
    }

    public void testCheckMonitoringSettingCollectIndexStatsTimeout() {
        monitoringSetting("xpack.monitoring.collection.index.stats.timeout", "10s");
    }

    public void testCheckMonitoringSettingCollectMlJobStatsTimeout() {
        monitoringSetting("xpack.monitoring.collection.ml.job.stats.timeout", "10s");
    }

    public void testCheckMonitoringSettingCollectNodeStatsTimeout() {
        monitoringSetting("xpack.monitoring.collection.node.stats.timeout", "10s");
    }

    public void testCheckMonitoringSettingCollectClusterStatsTimeout() {
        monitoringSetting("xpack.monitoring.collection.cluster.stats.timeout", "10s");
    }

    public void testCheckMonitoringSettingExportersHost() {
        monitoringExporterSetting("host", "abcdef");
    }

    public void testCheckMonitoringSettingExportersBulkTimeout() {
        monitoringExporterSetting("bulk.timeout", "10s");
    }

    public void testCheckMonitoringSettingExportersConnectionTimeout() {
        monitoringExporterSetting("connection.timeout", "10s");
    }

    public void testCheckMonitoringSettingExportersConnectionReadTimeout() {
        monitoringExporterSetting("connection.read_timeout", "10s");
    }

    public void testCheckMonitoringSettingExportersAuthUsername() {
        monitoringExporterSetting("auth.username", "abcdef");
    }

    public void testCheckMonitoringSettingExportersAuthPass() {
        monitoringExporterSecureSetting("auth.secure_password", "abcdef");
    }

    public void testCheckMonitoringSettingExportersSSL() {
        monitoringExporterGroupedSetting("ssl", "abcdef");
    }

    public void testCheckMonitoringSettingExportersProxyBase() {
        monitoringExporterSetting("proxy.base_path", "abcdef");
    }

    public void testCheckMonitoringSettingExportersSniffEnabled() {
        monitoringExporterSetting("sniff.enabled", "true");
    }

    public void testCheckMonitoringSettingExportersHeaders() {
        monitoringExporterGroupedSetting("headers", "abcdef");
    }

    public void testCheckMonitoringSettingExportersTemplateTimeout() {
        monitoringExporterSetting("index.template.master_timeout", "10s");
    }

    public void testCheckMonitoringSettingExportersMasterTimeout() {
        monitoringExporterSetting("wait_master.timeout", "10s");
    }

    public void testCheckMonitoringSettingExportersEnabled() {
        monitoringExporterSetting("enabled", "true");
    }

    public void testCheckMonitoringSettingExportersType() {
        monitoringExporterSetting("type", "local");
    }

    public void testCheckMonitoringSettingExportersAlertsEnabled() {
        monitoringExporterSetting("cluster_alerts.management.enabled", "true");
    }

    public void testCheckMonitoringSettingExportersAlertsBlacklist() {
        monitoringExporterSetting("cluster_alerts.management.blacklist", "[abcdef,ghijkl]");
    }

    public void testCheckMonitoringSettingExportersIndexNameTimeFormat() {
        monitoringExporterSetting("index.name.time_format", "yyyy-mm-dd");
    }

    public void testCheckMonitoringSettingDecomissionAlerts() {
        monitoringSetting("xpack.monitoring.migration.decommission_alerts", "true");
    }

    public void testCheckMonitoringSettingEsCollectionEnabled() {
        monitoringSetting("xpack.monitoring.elasticsearch.collection.enabled", "true");
    }

    public void testCheckMonitoringSettingCollectionEnabled() {
        monitoringSetting("xpack.monitoring.collection.enabled", "true");
    }

    public void testCheckMonitoringSettingCollectionInterval() {
        monitoringSetting("xpack.monitoring.collection.interval", "10s");
    }

    public void testExporterUseIngestPipelineSettings() {
        Settings settings = Settings.builder().put("xpack.monitoring.exporters.test.use_ingest", true).build();

        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            NODE_SETTINGS_CHECKS,
            c -> c.apply(settings, null, ClusterState.EMPTY_STATE, new XPackLicenseState(() -> 0))
        );

        final String expectedUrl = "https://ela.st/es-deprecation-7-monitoring-exporter-use-ingest-setting";
        assertThat(
            issues,
            hasItem(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "The [xpack.monitoring.exporters.test.use_ingest] settings are deprecated and will be removed after 8.0",
                    expectedUrl,
                    "Remove the following settings: [xpack.monitoring.exporters.test.use_ingest]",
                    false,
                    null
                )
            )
        );
    }

    public void testExporterPipelineMasterTimeoutSetting() {
        Settings settings = Settings.builder()
            .put("xpack.monitoring.exporters.test.index.pipeline.master_timeout", TimeValue.timeValueSeconds(10))
            .build();

        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            NODE_SETTINGS_CHECKS,
            c -> c.apply(settings, null, ClusterState.EMPTY_STATE, new XPackLicenseState(() -> 0))
        );

        final String expectedUrl = "https://ela.st/es-deprecation-7-monitoring-exporter-pipeline-timeout-setting";
        assertThat(
            issues,
            hasItem(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "The [xpack.monitoring.exporters.test.index.pipeline.master_timeout] "
                        + "settings are deprecated and will be removed after 8.0",
                    expectedUrl,
                    "Remove the following settings: [xpack.monitoring.exporters.test.index.pipeline.master_timeout]",
                    false,
                    null
                )
            )
        );
    }

    public void testExporterCreateLegacyTemplateSetting() {
        Settings settings = Settings.builder().put("xpack.monitoring.exporters.test.index.template.create_legacy_templates", true).build();

        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            NODE_SETTINGS_CHECKS,
            c -> c.apply(settings, null, ClusterState.EMPTY_STATE, new XPackLicenseState(() -> 0))
        );

        final String expectedUrl = "https://ela.st/es-deprecation-7-monitoring-exporter-create-legacy-template-setting";
        assertThat(
            issues,
            hasItem(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "The [xpack.monitoring.exporters.test.index.template.create_legacy_templates] settings are deprecated and will be "
                        + "removed after 8.0",
                    expectedUrl,
                    "Remove the following settings: " + "[xpack.monitoring.exporters.test.index.template.create_legacy_templates]",
                    false,
                    null
                )
            )
        );
    }

    public void testScriptContextCacheSetting() {
        Settings settings = Settings.builder()
            .put(ScriptService.SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), "use-context")
            .build();

        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            NODE_SETTINGS_CHECKS,
            c -> c.apply(settings, null, ClusterState.EMPTY_STATE, new XPackLicenseState(() -> 0))
        );

        assertThat(
            issues,
            hasItem(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    ScriptService.USE_CONTEXT_RATE_KEY_DEPRECATION_MESSAGE,
                    "https://ela.st/es-deprecation-7-script-context-cache",
                    "found deprecated script context caches in use, change setting to compilation rate or remove "
                        + "setting to use the default",
                    false,
                    null
                )
            )
        );
    }

    public void testScriptContextCompilationsRateLimitSetting() {
        List<String> contexts = List.of("field", "score");
        Settings settings = Settings.builder()
            .put(ScriptService.SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), "use-context")
            .put(ScriptService.SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace(contexts.get(0)).getKey(), "123/5m")
            .put(ScriptService.SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace(contexts.get(1)).getKey(), "456/7m")
            .build();

        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            NODE_SETTINGS_CHECKS,
            c -> c.apply(settings, null, ClusterState.EMPTY_STATE, new XPackLicenseState(() -> 0))
        );

        assertThat(
            issues,
            hasItem(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "Setting context-specific rate limits"
                        + " [script.context.field.max_compilations_rate,script.context.score.max_compilations_rate] is deprecated."
                        + " Use [script.max_compilations_rate] to rate limit the compilation of user scripts."
                        + " Context-specific caches are no longer needed to prevent system scripts from triggering rate limits.",
                    "https://ela.st/es-deprecation-7-script-context-cache",
                    "[script.context.field.max_compilations_rate,script.context.score.max_compilations_rate] is deprecated and"
                        + " will be removed in a future release",
                    false,
                    null
                )
            )
        );

        assertWarnings(
            "[script.context.field.max_compilations_rate] setting was deprecated in Elasticsearch and will be"
                + " removed in a future release.",
            "[script.context.score.max_compilations_rate] setting was deprecated in Elasticsearch and will be removed in a future release."
        );
    }

    public void testImplicitScriptContextCacheSetting() {
        List<String> contexts = List.of("update", "filter");
        Settings settings = Settings.builder()
            .put(ScriptService.SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace(contexts.get(0)).getKey(), "123/5m")
            .put(ScriptService.SCRIPT_CACHE_SIZE_SETTING.getConcreteSettingForNamespace(contexts.get(1)).getKey(), "2453")
            .build();

        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            NODE_SETTINGS_CHECKS,
            c -> c.apply(settings, null, ClusterState.EMPTY_STATE, new XPackLicenseState(() -> 0))
        );

        assertThat(
            issues,
            hasItem(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "Implicitly using the script context cache is deprecated, remove settings "
                        + "[script.context.filter.cache_max_size, script.context.update.max_compilations_rate] "
                        + "to use the script general cache.",
                    "https://ela.st/es-deprecation-7-script-context-cache",
                    "Remove the context-specific cache settings and set [script.max_compilations_rate] to configure the rate limit for "
                        + "the general cache. If no limit is set, the rate defaults to 150 compilations per five minutes: 150/5m. "
                        + "Context-specific caches are no longer needed to prevent system scripts from triggering rate limits.",
                    false,
                    null
                )
            )
        );

        assertWarnings(
            "[script.context.update.max_compilations_rate] setting was deprecated in Elasticsearch and will be"
                + " removed in a future release.",
            "[script.context.filter.cache_max_size] setting was deprecated in Elasticsearch and will be removed in a future release."
        );
    }

    public void testScriptContextCacheSizeSetting() {
        List<String> contexts = List.of("filter", "update");
        Settings settings = Settings.builder()
            .put(ScriptService.SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), "use-context")
            .put(ScriptService.SCRIPT_CACHE_SIZE_SETTING.getConcreteSettingForNamespace(contexts.get(0)).getKey(), 80)
            .put(ScriptService.SCRIPT_CACHE_SIZE_SETTING.getConcreteSettingForNamespace(contexts.get(1)).getKey(), 200)
            .build();

        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            NODE_SETTINGS_CHECKS,
            c -> c.apply(settings, null, ClusterState.EMPTY_STATE, new XPackLicenseState(() -> 0))
        );

        assertThat(
            issues,
            hasItem(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "Setting a context-specific cache size"
                        + " [script.context.filter.cache_max_size,script.context.update.cache_max_size] is deprecated."
                        + " Use [script.cache.max_size] to configure the size of the general cache for scripts."
                        + " Context-specific caches are no longer needed to prevent system scripts from triggering rate limits.",
                    "https://ela.st/es-deprecation-7-script-context-cache",
                    "[script.context.filter.cache_max_size,script.context.update.cache_max_size] is deprecated and will be"
                        + " removed in a future release",
                    false,
                    null
                )
            )
        );

        assertWarnings(
            "[script.context.update.cache_max_size] setting was deprecated in Elasticsearch and will be removed in a future release.",
            "[script.context.filter.cache_max_size] setting was deprecated in Elasticsearch and will be removed in a future release."
        );
    }

    public void testScriptContextCacheExpirationSetting() {
        List<String> contexts = List.of("interval", "moving-function");
        Settings settings = Settings.builder()
            .put(ScriptService.SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), "use-context")
            .put(ScriptService.SCRIPT_CACHE_EXPIRE_SETTING.getConcreteSettingForNamespace(contexts.get(0)).getKey(), "100m")
            .put(ScriptService.SCRIPT_CACHE_EXPIRE_SETTING.getConcreteSettingForNamespace(contexts.get(1)).getKey(), "2d")
            .build();

        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            NODE_SETTINGS_CHECKS,
            c -> c.apply(settings, null, ClusterState.EMPTY_STATE, new XPackLicenseState(() -> 0))
        );

        assertThat(
            issues,
            hasItem(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "Setting a context-specific cache expiration"
                        + " [script.context.interval.cache_expire,script.context.moving-function.cache_expire] is deprecated."
                        + " Use [script.cache.expire] to configure the expiration of the general cache."
                        + " Context-specific caches are no longer needed to prevent system scripts from triggering rate limits.",
                    "https://ela.st/es-deprecation-7-script-context-cache",
                    "[script.context.interval.cache_expire,script.context.moving-function.cache_expire] is deprecated and will be"
                        + " removed in a future release",
                    false,
                    null
                )
            )
        );

        assertWarnings(
            "[script.context.interval.cache_expire] setting was deprecated in Elasticsearch and will be removed in a future release.",
            "[script.context.moving-function.cache_expire] setting was deprecated in Elasticsearch and will be removed in a future release."
        );
    }

    public void testEnforceDefaultTierPreferenceSetting() {
        Settings settings = Settings.builder().put(DataTier.ENFORCE_DEFAULT_TIER_PREFERENCE_SETTING.getKey(), randomBoolean()).build();

        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            NODE_SETTINGS_CHECKS,
            c -> c.apply(settings, null, ClusterState.EMPTY_STATE, new XPackLicenseState(() -> 0))
        );

        final String expectedUrl = "https://www.elastic.co/guide/en/elasticsearch/reference/current/data-tiers.html";
        assertThat(
            issues,
            hasItem(
                new DeprecationIssue(
                    DeprecationIssue.Level.CRITICAL,
                    "setting [cluster.routing.allocation.enforce_default_tier_preference] is deprecated and"
                        + " will not be available in a future version",
                    expectedUrl,
                    "found [cluster.routing.allocation.enforce_default_tier_preference] configured." + " Discontinue use of this setting.",
                    false,
                    null
                )
            )
        );
    }

    private List<DeprecationIssue> getDeprecationIssues(Settings settings, PluginsAndModules pluginsAndModules) {
        final List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            DeprecationChecks.NODE_SETTINGS_CHECKS,
            c -> c.apply(settings, pluginsAndModules, ClusterState.EMPTY_STATE, new XPackLicenseState(() -> 0))
        );

        return issues;
    }

    public void testLifecyleStepMasterTimeoutSetting() {
        Settings settings = Settings.builder()
            .put(LifecycleSettings.LIFECYCLE_STEP_MASTER_TIMEOUT_SETTING.getKey(), randomTimeValue())
            .build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Setting [indices.lifecycle.step.master_timeout] is deprecated",
            "https://ela.st/es-deprecation-8-lifecycle-master-timeout-setting",
            "Remove the [indices.lifecycle.step.master_timeout] setting. As of 7.16 the timeout is always infinite.",
            false,
            null
        );
        assertThat(issues, hasItem(expected));
        assertWarnings(
            true,
            new DeprecationWarning(
                Level.WARN,
                "[indices.lifecycle.step.master_timeout] setting was deprecated in Elasticsearch and will be removed in a future release."
            )
        );
    }

    public void testEqlEnabledSetting() {
        Settings settings = Settings.builder().put("xpack.eql.enabled", randomBoolean()).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Setting [xpack.eql.enabled] is deprecated",
            "https://ela.st/es-deprecation-8-eql-enabled-setting",
            "Remove the [xpack.eql.enabled] setting. As of 7.9.2 basic license level features are always enabled.",
            false,
            null
        );
        assertThat(issues, hasItem(expected));
        assertWarnings(
            true,
            new DeprecationWarning(
                Level.WARN,
                "[xpack.eql.enabled] setting was deprecated in Elasticsearch and will be removed in a future release."
            )
        );
    }

    public void testDynamicSettings() {
        String concreteSettingKey = "xpack.monitoring.exporters." + randomAlphaOfLength(10) + ".use_ingest";
        Settings clusterSettings = Settings.builder().put(concreteSettingKey, randomBoolean()).build();
        Settings nodettings = Settings.builder().build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState = new XPackLicenseState(() -> 0);
        Metadata.Builder metadataBuilder = Metadata.builder();
        if (randomBoolean()) {
            metadataBuilder.persistentSettings(clusterSettings);
        } else {
            metadataBuilder.transientSettings(clusterSettings);
        }
        Metadata metadata = metadataBuilder.build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();
        final List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            DeprecationChecks.NODE_SETTINGS_CHECKS,
            c -> c.apply(nodettings, pluginsAndModules, clusterState, licenseState)
        );

        Map<String, Object> meta = null;
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "The [" + concreteSettingKey + "] settings are deprecated and will be removed after 8.0",
            "https://ela.st/es-deprecation-7-monitoring-exporter-use-ingest-setting",
            "Remove the following settings: [" + concreteSettingKey + "]",
            false,
            meta
        );
        assertThat(issues, hasItem(expected));
    }

    public void testCheckNodeAttrData() {
        Settings settings = Settings.builder().put("node.attr.data", randomAlphaOfLength(randomIntBetween(4, 20))).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Setting node.attributes.data is not recommended",
            "https://ela.st/es-deprecation-7-node-attr-data-setting",
            "One or more of your nodes is configured with node.attributes.data settings. This is typically used to create a "
                + "hot/warm or tiered architecture, based on legacy guidelines. Data tiers are a recommended replacement for tiered "
                + "architecture clusters.",
            false,
            null
        );
        assertThat(issues, hasItem(expected));
    }
}

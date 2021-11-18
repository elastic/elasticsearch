/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.bootstrap.BootstrapSettings;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.coordination.DiscoveryUpgradeService;
import org.elasticsearch.cluster.coordination.JoinHelper;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslConfigurationKeys;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.zen.FaultDetection;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.DanglingIndicesState;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.jdk.JavaVersion;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.transport.Compression;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.SniffConnectionStrategy;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.monitoring.Monitoring;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_INCLUDE_RELOCATIONS_SETTING;
import static org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider.CLUSTER_ROUTING_EXCLUDE_SETTING;
import static org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider.CLUSTER_ROUTING_INCLUDE_SETTING;
import static org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider.CLUSTER_ROUTING_REQUIRE_SETTING;
import static org.elasticsearch.xpack.core.security.authc.RealmSettings.RESERVED_REALM_NAME_PREFIX;
import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.PRINCIPAL_ATTRIBUTE;

class NodeDeprecationChecks {

    static final String JAVA_DEPRECATION_MESSAGE = "Java 11 is required in 8.0";

    static DeprecationIssue checkPidfile(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return checkDeprecatedSetting(
            settings,
            pluginsAndModules,
            Environment.PIDFILE_SETTING,
            Environment.NODE_PIDFILE_SETTING,
            "https://ela.st/es-deprecation-7-pidfile-setting"
        );
    }

    static DeprecationIssue checkProcessors(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return checkDeprecatedSetting(
            settings,
            pluginsAndModules,
            EsExecutors.PROCESSORS_SETTING,
            EsExecutors.NODE_PROCESSORS_SETTING,
            "https://ela.st/es-deprecation-7-processors-setting"
        );
    }

    static DeprecationIssue checkMissingRealmOrders(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        final Set<String> orderNotConfiguredRealms = RealmSettings.getRealmSettings(settings)
            .entrySet()
            .stream()
            .filter(e -> false == e.getValue().hasValue(RealmSettings.ORDER_SETTING_KEY))
            .filter(e -> e.getValue().getAsBoolean(RealmSettings.ENABLED_SETTING_KEY, true))
            .map(e -> RealmSettings.realmSettingPrefix(e.getKey()) + RealmSettings.ORDER_SETTING_KEY)
            .collect(Collectors.toSet());

        if (orderNotConfiguredRealms.isEmpty()) {
            return null;
        }

        final String details = String.format(
            Locale.ROOT,
            "Specify the realm order for all realms [%s]. If no realm order is specified, the node will fail to start in 8.0. ",
            String.join("; ", orderNotConfiguredRealms)
        );
        return new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Realm order is required",
            "https://ela.st/es-deprecation-7-realm-orders-required",
            details,
            false,
            null
        );
    }

    static DeprecationIssue checkUniqueRealmOrders(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        final Map<String, List<String>> orderToRealmSettings = RealmSettings.getRealmSettings(settings)
            .entrySet()
            .stream()
            .filter(e -> e.getValue().hasValue(RealmSettings.ORDER_SETTING_KEY))
            .collect(
                Collectors.groupingBy(
                    e -> e.getValue().get(RealmSettings.ORDER_SETTING_KEY),
                    Collectors.mapping(
                        e -> RealmSettings.realmSettingPrefix(e.getKey()) + RealmSettings.ORDER_SETTING_KEY,
                        Collectors.toList()
                    )
                )
            );

        Set<String> duplicateOrders = orderToRealmSettings.entrySet()
            .stream()
            .filter(entry -> entry.getValue().size() > 1)
            .map(entry -> entry.getKey() + ": " + entry.getValue())
            .collect(Collectors.toSet());

        if (duplicateOrders.isEmpty()) {
            return null;
        }

        final String details = String.format(
            Locale.ROOT,
            "The same order is configured for multiple realms: [%s]]. Configure a unique order for each realm. If duplicate realm orders "
                + "exist, the node will fail to start in 8.0. ",
            String.join("; ", duplicateOrders)
        );

        return new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Realm orders must be unique",
            "https://ela.st/es-deprecation-7-realm-orders-unique",
            details,
            false,
            null
        );
    }

    static DeprecationIssue checkImplicitlyDisabledSecurityOnBasicAndTrial(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        if (XPackSettings.SECURITY_ENABLED.exists(settings) == false
            && (licenseState.getOperationMode().equals(License.OperationMode.BASIC)
                || licenseState.getOperationMode().equals(License.OperationMode.TRIAL))) {
            String details = "Security will no longer be disabled by default for Trial licenses in 8.0. The [xpack.security.enabled] "
                + "setting will always default to \"true\". See https://ela.st/es-deprecation-7-security-minimal-setup to secure your "
                + "cluster. To explicitly disable security, set [xpack.security.enabled] to \"false\" (not recommended).";
            return new DeprecationIssue(
                DeprecationIssue.Level.CRITICAL,
                "Security is enabled by default for all licenses",
                "https://ela.st/es-deprecation-7-implicitly-disabled-security",
                details,
                false,
                null
            );
        }
        return null;
    }

    static DeprecationIssue checkImplicitlyDisabledBasicRealms(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        final Map<RealmConfig.RealmIdentifier, Settings> realmSettings = RealmSettings.getRealmSettings(settings);
        if (realmSettings.isEmpty()) {
            return null;
        }

        boolean anyRealmEnabled = false;
        final Set<String> unconfiguredBasicRealms = new HashSet<>(
            org.elasticsearch.core.Set.of(FileRealmSettings.TYPE, NativeRealmSettings.TYPE)
        );
        for (Map.Entry<RealmConfig.RealmIdentifier, Settings> realmSetting : realmSettings.entrySet()) {
            anyRealmEnabled = anyRealmEnabled || realmSetting.getValue().getAsBoolean(RealmSettings.ENABLED_SETTING_KEY, true);
            unconfiguredBasicRealms.remove(realmSetting.getKey().getType());
        }

        final String details;
        if (false == anyRealmEnabled) {
            final List<String> explicitlyDisabledBasicRealms = Sets.difference(
                org.elasticsearch.core.Set.of(FileRealmSettings.TYPE, NativeRealmSettings.TYPE),
                unconfiguredBasicRealms
            ).stream().sorted().collect(Collectors.toList());
            if (explicitlyDisabledBasicRealms.isEmpty()) {
                return null;
            }
            details = String.format(
                Locale.ROOT,
                "Found explicitly disabled basic %s: [%s]. But %s will be enabled because no other realms are configured or enabled. "
                    + "In next major release, explicitly disabled basic realms will remain disabled.",
                explicitlyDisabledBasicRealms.size() == 1 ? "realm" : "realms",
                Strings.collectionToDelimitedString(explicitlyDisabledBasicRealms, ","),
                explicitlyDisabledBasicRealms.size() == 1 ? "it" : "they"
            );
        } else {
            if (unconfiguredBasicRealms.isEmpty()) {
                return null;
            }
            details = String.format(
                Locale.ROOT,
                "Found implicitly disabled basic %s: [%s]. %s disabled because there are other explicitly configured realms."
                    + "In next major release, basic realms will always be enabled unless explicitly disabled.",
                unconfiguredBasicRealms.size() == 1 ? "realm" : "realms",
                Strings.collectionToDelimitedString(unconfiguredBasicRealms, ","),
                unconfiguredBasicRealms.size() == 1 ? "It is" : "They are"
            );
        }
        return new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "File and/or native realms are enabled by default in next major release.",
            "https://ela.st/es-deprecation-7-implicitly-disabled-basic-realms",
            details,
            false,
            null
        );

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
            if (realmIdentifier.getName().startsWith(RESERVED_REALM_NAME_PREFIX)) {
                reservedPrefixedRealmIdentifiers.add(realmIdentifier);
            }
        }
        if (reservedPrefixedRealmIdentifiers.isEmpty()) {
            return null;
        } else {
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                "Prefixing realm names with an underscore (_) is deprecated",
                "https://ela.st/es-deprecation-7-realm-names",
                String.format(
                    Locale.ROOT,
                    "Rename the following realm%s in the realm chain: %s.",
                    reservedPrefixedRealmIdentifiers.size() > 1 ? "s" : "",
                    reservedPrefixedRealmIdentifiers.stream()
                        .map(rid -> RealmSettings.PREFIX + rid.getType() + "." + rid.getName())
                        .sorted()
                        .collect(Collectors.joining(", "))
                ),
                false,
                null
            );
        }
    }

    static DeprecationIssue checkThreadPoolListenerQueueSize(final Settings settings) {
        return checkThreadPoolListenerSetting("thread_pool.listener.queue_size", settings);
    }

    static DeprecationIssue checkThreadPoolListenerSize(final Settings settings) {
        return checkThreadPoolListenerSetting("thread_pool.listener.size", settings);
    }

    private static DeprecationIssue checkThreadPoolListenerSetting(final String name, final Settings settings) {
        final FixedExecutorBuilder builder = new FixedExecutorBuilder(settings, "listener", 1, -1, "thread_pool.listener", true);
        final List<Setting<?>> listenerSettings = builder.getRegisteredSettings();
        final Optional<Setting<?>> setting = listenerSettings.stream().filter(s -> s.getKey().equals(name)).findFirst();
        assert setting.isPresent();
        return checkRemovedSetting(
            settings,
            setting.get(),
            "https://ela.st/es-deprecation-7-thread-pool-listener-settings",
            "The listener pool is no longer used in 8.0."
        );
    }

    public static DeprecationIssue checkClusterRemoteConnectSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        if (RemoteClusterService.ENABLE_REMOTE_CLUSTERS.exists(settings) == false) {
            return null;
        }
        final String deprecatedSettingKey = RemoteClusterService.ENABLE_REMOTE_CLUSTERS.getKey();
        final String message = String.format(Locale.ROOT, "Setting [%s] is deprecated", deprecatedSettingKey);
        final String details = String.format(
            Locale.ROOT,
            "Remove the [%s] setting. Use the [remote_cluster_client] role instead. Set [node.roles] to [data_frozen,master,"
                + "remote_cluster_client,data,data_content,data_hot,data_warm,data_cold,ingest] on eligible cross-cluster client nodes.",
            deprecatedSettingKey
        );
        return new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            message,
            "https://ela.st/es-deprecation-7-cluster-remote-connect-setting",
            details,
            false,
            null
        );
    }

    public static DeprecationIssue checkNodeLocalStorageSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return checkRemovedSetting(
            settings,
            Node.NODE_LOCAL_STORAGE_SETTING,
            "https://ela.st/es-deprecation-7-node-local-storage-setting",
            "All nodes require local storage in 8.0 and cannot share data paths."
        );
    }

    public static DeprecationIssue checkNodeBasicLicenseFeatureEnabledSetting(final Settings settings, Setting<?> setting) {
        return checkRemovedSetting(
            settings,
            setting,
            "https://ela.st/es-deprecation-7-xpack-basic-feature-settings",
            "Basic features are always enabled in 8.0."
        );
    }

    public static DeprecationIssue checkLegacyRoleSettings(
        final Setting<Boolean> legacyRoleSetting,
        final Settings settings,
        final PluginsAndModules pluginsAndModules
    ) {
        assert legacyRoleSetting.isDeprecated() : legacyRoleSetting;
        if (legacyRoleSetting.exists(settings) == false) {
            return null;
        }
        String legacyRoleSettingKey = legacyRoleSetting.getKey();
        String role;
        if (legacyRoleSettingKey.isEmpty() == false
            && legacyRoleSettingKey.contains(".")
            && legacyRoleSettingKey.indexOf(".") <= legacyRoleSettingKey.length() + 2) {
            role = legacyRoleSettingKey.substring(legacyRoleSettingKey.indexOf(".") + 1);
        } else {
            role = "unknown"; // Should never get here, but putting these checks to avoid crashing the API just in case
        }
        final String message = String.format(Locale.ROOT, "Setting [%s] is deprecated", legacyRoleSettingKey);
        final String details = String.format(
            Locale.ROOT,
            "Remove the [%s] setting. Set [%s] and include the [%s] role.",
            legacyRoleSettingKey,
            NodeRoleSettings.NODE_ROLES_SETTING.getKey(),
            role
        );
        return new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            message,
            "https://ela.st/es-deprecation-7-node-roles",
            details,
            false,
            null
        );
    }

    static DeprecationIssue checkBootstrapSystemCallFilterSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return checkRemovedSetting(
            settings,
            BootstrapSettings.SYSTEM_CALL_FILTER_SETTING,
            "https://ela.st/es-deprecation-7-system-call-filter-setting",
            "System call filters are always required in 8.0."
        );
    }

    private static DeprecationIssue checkDeprecatedSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final Setting<?> deprecatedSetting,
        final Setting<?> replacementSetting,
        final String url
    ) {
        return checkDeprecatedSetting(settings, pluginsAndModules, deprecatedSetting, replacementSetting, (v, s) -> v, url);
    }

    private static DeprecationIssue checkDeprecatedSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final Setting<?> deprecatedSetting,
        final Setting<?> replacementSetting,
        final BiFunction<String, Settings, String> replacementValue,
        final String url
    ) {
        assert deprecatedSetting.isDeprecated() : deprecatedSetting;
        if (deprecatedSetting.exists(settings) == false) {
            return null;
        }
        final String deprecatedSettingKey = deprecatedSetting.getKey();
        final String replacementSettingKey = replacementSetting.getKey();
        final String value = deprecatedSetting.get(settings).toString();
        final String message = String.format(Locale.ROOT, "Setting [%s] is deprecated", deprecatedSettingKey);
        final String details = String.format(
            Locale.ROOT,
            "Remove the [%s] setting and set [%s] to [%s].",
            deprecatedSettingKey,
            replacementSettingKey,
            replacementValue.apply(value, settings)
        );
        return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, details, false, null);
    }

    private static DeprecationIssue checkDeprecatedSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final Setting<?> deprecatedSetting,
        final Setting.AffixSetting<?> replacementSetting,
        final String star,
        final String url
    ) {
        return checkDeprecatedSetting(settings, pluginsAndModules, deprecatedSetting, replacementSetting, (v, s) -> v, star, url);
    }

    private static DeprecationIssue checkDeprecatedSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final Setting<?> deprecatedSetting,
        final Setting.AffixSetting<?> replacementSetting,
        final BiFunction<String, Settings, String> replacementValue,
        final String star,
        final String url
    ) {
        assert deprecatedSetting.isDeprecated() : deprecatedSetting;
        if (deprecatedSetting.exists(settings) == false) {
            return null;
        }
        final String deprecatedSettingKey = deprecatedSetting.getKey();
        final String replacementSettingKey = replacementSetting.getKey();
        final String value = deprecatedSetting.get(settings).toString();
        final String message = String.format(Locale.ROOT, "Setting [%s] is deprecated", deprecatedSettingKey, replacementSettingKey);
        final String details = String.format(
            Locale.ROOT,
            "Remove the [%s] setting. Set [%s] to [%s], where * is %s",
            deprecatedSettingKey,
            replacementSettingKey,
            replacementValue.apply(value, settings),
            star
        );
        return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, details, false, null);
    }

    static DeprecationIssue checkRemovedSetting(
        final Settings settings,
        final Setting<?> removedSetting,
        final String url,
        String additionalDetailMessage
    ) {
        return checkRemovedSetting(settings, removedSetting, url, additionalDetailMessage, DeprecationIssue.Level.CRITICAL);
    }

    static DeprecationIssue checkDeprecatedSetting(final Settings settings, final Setting<?> deprecatedSetting, final String url) {
        if (deprecatedSetting.exists(settings) == false) {
            return null;
        }
        final String deprecatedSettingKey = deprecatedSetting.getKey();
        final String value = deprecatedSetting.get(settings).toString();
        final String message = String.format(Locale.ROOT, "Setting [%s] is deprecated", deprecatedSettingKey);
        final String details = String.format(Locale.ROOT, "Remove the [%s] setting.", deprecatedSettingKey, value);
        return new DeprecationIssue(DeprecationIssue.Level.WARNING, message, url, details, false, null);
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
        final String details = String.format(Locale.ROOT, "Remove the [%s] setting. %s", removedSettingKey, additionalDetailMessage);
        return new DeprecationIssue(deprecationLevel, message, url, details, false, null);
    }

    static DeprecationIssue javaVersionCheck(
        Settings nodeSettings,
        PluginsAndModules plugins,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        final JavaVersion javaVersion = JavaVersion.current();

        if (javaVersion.compareTo(JavaVersion.parse("11")) < 0) {
            return new DeprecationIssue(
                DeprecationIssue.Level.CRITICAL,
                JAVA_DEPRECATION_MESSAGE,
                "https://ela.st/es-deprecation-7-java-version",
                "This node is running Java version ["
                    + javaVersion.toString()
                    + "]. Consider switching to a distribution of "
                    + "Elasticsearch with a bundled JDK or upgrade. If you are already using a distribution with a bundled JDK, ensure the "
                    + "JAVA_HOME environment variable is not set.",
                false,
                null
            );
        }
        return null;
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
                DeprecationIssue.Level.CRITICAL,
                "Specifying multiple data paths is deprecated",
                "https://ela.st/es-deprecation-7-multiple-paths",
                "The [path.data] setting contains a list of paths. Specify a single path as a string. Use RAID or other system level "
                    + "features to utilize multiple disks. If multiple data paths are configured, the node will fail to start in 8.0. ",
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
                DeprecationIssue.Level.CRITICAL,
                "Multiple data paths are not supported",
                "https://ela.st/es-deprecation-7-multiple-paths",
                "The [path.data] setting contains a list of paths. Specify a single path as a string. Use RAID or other system level "
                    + "features to utilize multiple disks. If multiple data paths are configured, the node will fail to start in 8.0. ",
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
            final String message = String.format(Locale.ROOT, "Setting [%s] is deprecated", Environment.PATH_SHARED_DATA_SETTING.getKey());
            final String url = "https://ela.st/es-deprecation-7-shared-path-settings";
            final String details = String.format(
                Locale.ROOT,
                "Remove the [%s] setting. This setting has had no effect since 6.0.",
                Environment.PATH_SHARED_DATA_SETTING.getKey()
            );
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, details, false, null);
        }
        return null;
    }

    static DeprecationIssue checkSingleDataNodeWatermarkSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        if (DiskThresholdDecider.ENABLE_FOR_SINGLE_DATA_NODE.get(settings) == false
            && DiskThresholdDecider.ENABLE_FOR_SINGLE_DATA_NODE.exists(settings)) {
            String key = DiskThresholdDecider.ENABLE_FOR_SINGLE_DATA_NODE.getKey();
            return new DeprecationIssue(
                DeprecationIssue.Level.CRITICAL,
                String.format(Locale.ROOT, "Setting [%s=false] is deprecated", key),
                "https://ela.st/es-deprecation-7-disk-watermark-enable-for-single-node-setting",
                String.format(
                    Locale.ROOT,
                    "Disk watermarks do not treat single-node clusters differently in versions 8.0 and later, and [%s] may not be set to "
                        + "[false] in these versions. Set [%s] to [true] to adopt the future behavior before upgrading.",
                    key,
                    key
                ),
                false,
                null
            );
        }

        if (DiskThresholdDecider.ENABLE_FOR_SINGLE_DATA_NODE.get(settings) == false
            && clusterState.getNodes().getDataNodes().size() == 1
            && clusterState.getNodes().getLocalNode().isMasterNode()) {
            String key = DiskThresholdDecider.ENABLE_FOR_SINGLE_DATA_NODE.getKey();
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                "Disk watermarks do not treat single-node clusters differently in versions 8.0 and later.",
                "https://ela.st/es-deprecation-7-disk-watermark-enable-for-single-node-setting",
                String.format(
                    Locale.ROOT,
                    "Disk watermarks do not treat single-node clusters differently in versions 8.0 and later, which will affect the "
                        + "behavior of this cluster. Set [%s] to [true] to adopt the future behavior before upgrading.",
                    key
                ),
                false,
                null
            );

        }

        return null;
    }

    static DeprecationIssue checkMonitoringExporterPassword(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        ClusterState cs,
        XPackLicenseState licenseState
    ) {
        // Mimic the HttpExporter#AUTH_PASSWORD_SETTING setting here to avoid a dependency on monitoring module:
        // (just having the setting prefix and suffic here is sufficient to check on whether this setting is used)
        final Setting.AffixSetting<String> AUTH_PASSWORD_SETTING = Setting.affixKeySetting(
            "xpack.monitoring.exporters.",
            "auth.password",
            s -> Setting.simpleString(s)
        );
        List<Setting<?>> passwords = AUTH_PASSWORD_SETTING.getAllConcreteSettings(settings)
            .sorted(Comparator.comparing(Setting::getKey))
            .collect(Collectors.toList());

        if (passwords.isEmpty()) {
            return null;
        }

        final String passwordSettings = passwords.stream().map(Setting::getKey).collect(Collectors.joining(","));
        final String message = String.format(Locale.ROOT, "Monitoring exporters must use secure passwords", passwordSettings);
        final String details = String.format(
            Locale.ROOT,
            "Remove the non-secure monitoring exporter password settings: [%s]. Configure secure passwords with "
                + "[xpack.monitoring.exporters.*.auth.secure_password].",
            passwordSettings
        );
        final String url = "https://ela.st/es-deprecation-7-monitoring-exporter-passwords";
        return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, details, false, null);
    }

    static DeprecationIssue checkJoinTimeoutSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return checkRemovedSetting(
            settings,
            JoinHelper.JOIN_TIMEOUT_SETTING,
            "https://ela.st/es-deprecation-7-cluster-join-timeout-setting",
            "Cluster join attempts never time out in 8.0.",
            DeprecationIssue.Level.CRITICAL
        );
    }

    static DeprecationIssue checkSearchRemoteSettings(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        ClusterState cs,
        XPackLicenseState licenseState
    ) {
        List<Setting<?>> remoteClusterSettings = new ArrayList<>();
        remoteClusterSettings.addAll(
            SniffConnectionStrategy.SEARCH_REMOTE_CLUSTERS_SEEDS.getAllConcreteSettings(settings)
                .sorted(Comparator.comparing(Setting::getKey))
                .collect(Collectors.toList())
        );
        remoteClusterSettings.addAll(
            SniffConnectionStrategy.SEARCH_REMOTE_CLUSTERS_PROXY.getAllConcreteSettings(settings)
                .sorted(Comparator.comparing(Setting::getKey))
                .collect(Collectors.toList())
        );
        remoteClusterSettings.addAll(
            RemoteClusterService.SEARCH_REMOTE_CLUSTER_SKIP_UNAVAILABLE.getAllConcreteSettings(settings)
                .sorted(Comparator.comparing(Setting::getKey))
                .collect(Collectors.toList())
        );
        if (SniffConnectionStrategy.SEARCH_REMOTE_CONNECTIONS_PER_CLUSTER.exists(settings)) {
            remoteClusterSettings.add(SniffConnectionStrategy.SEARCH_REMOTE_CONNECTIONS_PER_CLUSTER);
        }
        if (RemoteClusterService.SEARCH_REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING.exists(settings)) {
            remoteClusterSettings.add(RemoteClusterService.SEARCH_REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING);
        }
        if (RemoteClusterService.SEARCH_REMOTE_NODE_ATTRIBUTE.exists(settings)) {
            remoteClusterSettings.add(RemoteClusterService.SEARCH_REMOTE_NODE_ATTRIBUTE);
        }
        if (RemoteClusterService.SEARCH_ENABLE_REMOTE_CLUSTERS.exists(settings)) {
            remoteClusterSettings.add(RemoteClusterService.SEARCH_ENABLE_REMOTE_CLUSTERS);
        }
        if (remoteClusterSettings.isEmpty()) {
            return null;
        }
        final String remoteClusterSeedSettings = remoteClusterSettings.stream().map(Setting::getKey).collect(Collectors.joining(","));
        final String message = "Remotes for cross cluster search must be configured with cluster remote settings";
        final String details = String.format(
            Locale.ROOT,
            "Replace the search.remote settings [%s] with their secure [cluster.remote] equivalents",
            remoteClusterSeedSettings
        );
        final String url = "https://ela.st/es-deprecation-7-search-remote-settings";
        return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, details, false, null);
    }

    static DeprecationIssue checkClusterRoutingAllocationIncludeRelocationsSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return checkRemovedSetting(
            settings,
            CLUSTER_ROUTING_ALLOCATION_INCLUDE_RELOCATIONS_SETTING,
            "https://ela.st/es-deprecation-7-cluster-routing-allocation-disk-include-relocations-setting",
            "Relocating shards are always taken into account in 8.0.",
            DeprecationIssue.Level.CRITICAL
        );
    }

    static DeprecationIssue checkFractionalByteValueSettings(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Map<String, String> fractionalByteSettings = new HashMap<>();
        for (String key : settings.keySet()) {
            try {
                settings.getAsBytesSize(key, ByteSizeValue.ZERO);
                String stringValue = settings.get(key);
                if (stringValue.contains(".")) {
                    fractionalByteSettings.put(key, stringValue);
                }
            } catch (Exception ignoreThis) {
                // We expect anything that is not a byte setting to throw an exception, but we don't care about those
            }
        }
        if (fractionalByteSettings.isEmpty()) {
            return null;
        }
        String url = "https://ela.st/es-deprecation-7-fractional-byte-settings";
        String message = "Configuring fractional byte sizes is deprecated";
        String details = String.format(
            Locale.ROOT,
            "Set the following to whole numbers: [%s].",
            fractionalByteSettings.entrySet()
                .stream()
                .map(fractionalByteSetting -> fractionalByteSetting.getKey())
                .collect(Collectors.joining(", "))
        );
        return new DeprecationIssue(DeprecationIssue.Level.WARNING, message, url, details, false, null);
    }

    static DeprecationIssue checkFrozenCacheLeniency(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        final String cacheSizeSettingKey = "xpack.searchable.snapshot.shared_cache.size";
        Setting<ByteSizeValue> cacheSizeSetting = Setting.byteSizeSetting(cacheSizeSettingKey, ByteSizeValue.ZERO);
        if (cacheSizeSetting.exists(settings)) {
            ByteSizeValue cacheSize = cacheSizeSetting.get(settings);
            if (cacheSize.getBytes() > 0) {
                final List<DiscoveryNodeRole> roles = NodeRoleSettings.NODE_ROLES_SETTING.get(settings);
                if (DataTier.isFrozenNode(new HashSet<>(roles)) == false) {
                    String message = String.format(
                        Locale.ROOT,
                        "Only frozen nodes can have a [%s] greater than zero.",
                        cacheSizeSettingKey
                    );
                    String url = "https://ela.st/es-deprecation-7-searchable-snapshot-shared-cache-setting";
                    String details = String.format(
                        Locale.ROOT,
                        "Set [%s] to zero on any node that doesn't have the [data_frozen] role.",
                        cacheSizeSettingKey
                    );
                    return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, details, false, null);
                }
            }
        }
        return null;
    }

    static DeprecationIssue checkSslServerEnabled(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        List<String> details = new ArrayList<>();
        for (String prefix : new String[] { "xpack.security.transport.ssl", "xpack.security.http.ssl" }) {
            final String enabledSettingKey = prefix + ".enabled";
            String enabledSettingValue = settings.get(enabledSettingKey);
            Settings sslSettings = settings.filter(setting -> setting.startsWith(prefix));
            if (enabledSettingValue == null && sslSettings.size() > 0) {
                String keys = sslSettings.keySet().stream().collect(Collectors.joining(","));
                String detail = String.format(
                    Locale.ROOT,
                    "The [%s] setting is not configured, but the following SSL settings are: [%s]."
                        + " To configure SSL, set [%s] or the node will fail to start in 8.0.",
                    enabledSettingKey,
                    keys,
                    enabledSettingKey
                );
                details.add(detail);
            }
        }
        if (details.isEmpty()) {
            return null;
        } else {
            String url = "https://ela.st/es-deprecation-7-explicit-ssl-required";
            String message = "Must explicitly enable or disable SSL to configure SSL settings";
            String detailsString = details.stream().collect(Collectors.joining("; "));
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, detailsString, false, null);
        }
    }

    static DeprecationIssue checkSslCertConfiguration(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        List<String> details = new ArrayList<>();
        for (String prefix : new String[] { "xpack.security.transport.ssl", "xpack.security.http.ssl" }) {
            final String enabledSettingKey = prefix + ".enabled";
            boolean sslEnabled = settings.getAsBoolean(enabledSettingKey, false);
            if (sslEnabled) {
                String keystorePathSettingKey = prefix + "." + SslConfigurationKeys.KEYSTORE_PATH;
                String keyPathSettingKey = prefix + "." + SslConfigurationKeys.KEY;
                String certificatePathSettingKey = prefix + "." + SslConfigurationKeys.CERTIFICATE;
                boolean keystorePathSettingExists = settings.get(keystorePathSettingKey) != null;
                boolean keyPathSettingExists = settings.get(keyPathSettingKey) != null;
                boolean certificatePathSettingExists = settings.get(certificatePathSettingKey) != null;
                if (keystorePathSettingExists == false && keyPathSettingExists == false && certificatePathSettingExists == false) {
                    String detail = String.format(
                        Locale.ROOT,
                        "None of [%s], [%s], or [%s] are set. If [%s] is true either use a " + "keystore, or configure [%s] and [%s].",
                        keystorePathSettingKey,
                        keyPathSettingKey,
                        certificatePathSettingKey,
                        enabledSettingKey,
                        keyPathSettingKey,
                        certificatePathSettingKey
                    );
                    details.add(detail);
                } else if (keystorePathSettingExists && keyPathSettingExists && certificatePathSettingExists) {
                    String detail = String.format(
                        Locale.ROOT,
                        "All of [%s], [%s], and [%s] are set. Either use a keystore, or " + "configure [%s] and [%s].",
                        keystorePathSettingKey,
                        keyPathSettingKey,
                        certificatePathSettingKey,
                        keyPathSettingKey,
                        certificatePathSettingKey
                    );
                    details.add(detail);
                } else if (keystorePathSettingExists && (keyPathSettingExists || certificatePathSettingExists)) {
                    String detail = String.format(
                        Locale.ROOT,
                        "Do not configure both [%s] and [%s]. Either" + " use a keystore, or configure [%s] and [%s].",
                        keystorePathSettingKey,
                        keyPathSettingExists ? keyPathSettingKey : certificatePathSettingKey,
                        keyPathSettingKey,
                        certificatePathSettingKey
                    );
                    details.add(detail);
                } else if ((keyPathSettingExists && certificatePathSettingExists == false)
                    || (keyPathSettingExists == false && certificatePathSettingExists)) {
                        String detail = String.format(
                            Locale.ROOT,
                            "[%s] is set but [%s] is not",
                            keyPathSettingExists ? keyPathSettingKey : certificatePathSettingKey,
                            keyPathSettingExists ? certificatePathSettingKey : keyPathSettingKey
                        );
                        details.add(detail);
                    }
            }
        }
        if (details.isEmpty()) {
            return null;
        } else {
            String url = "https://ela.st/es-deprecation-7-ssl-settings";
            String message = "Must either configure a keystore or set the key path and certificate path when SSL is enabled";
            String detailsString = details.stream().collect(Collectors.joining("; "));
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, detailsString, false, null);
        }
    }

    static DeprecationIssue checkNoPermitHandshakeFromIncompatibleBuilds(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState,
        Supplier<String> permitsHandshakesFromIncompatibleBuildsSupplier
    ) {
        if (permitsHandshakesFromIncompatibleBuildsSupplier.get() != null) {
            final String message = String.format(
                Locale.ROOT,
                "Setting the [%s] system property is deprecated",
                TransportService.PERMIT_HANDSHAKES_FROM_INCOMPATIBLE_BUILDS_KEY
            );
            final String details = String.format(
                Locale.ROOT,
                "Remove the [%s] system property. Handshakes from incompatible builds are not allowed in 8.0.",
                TransportService.PERMIT_HANDSHAKES_FROM_INCOMPATIBLE_BUILDS_KEY
            );
            String url = "https://ela.st/es-deprecation-7-permit-handshake-from-incompatible-builds-setting";
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, details, false, null);
        }
        return null;
    }

    static DeprecationIssue checkTransportClientProfilesFilterSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        ClusterState cs,
        XPackLicenseState licenseState
    ) {
        final Setting.AffixSetting<String> transportTypeProfileSetting = Setting.affixKeySetting(
            "transport.profiles.",
            "xpack.security.type",
            s -> Setting.simpleString(s)
        );
        List<Setting<?>> transportProfiles = transportTypeProfileSetting.getAllConcreteSettings(settings)
            .sorted(Comparator.comparing(Setting::getKey))
            .collect(Collectors.toList());

        if (transportProfiles.isEmpty()) {
            return null;
        }

        final String transportProfilesSettings = transportProfiles.stream().map(Setting::getKey).collect(Collectors.joining(","));
        final String message = String.format(
            Locale.ROOT,
            "Settings [%s] for the Transport client are deprecated",
            transportProfilesSettings
        );
        final String details = "Remove all [transport.profiles] settings. The Transport client no longer exists in 8.0.";

        final String url = "https://ela.st/es-deprecation-7-transport-profiles-settings";
        return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, details, false, null);
    }

    static DeprecationIssue checkDelayClusterStateRecoverySettings(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        List<Setting<Integer>> deprecatedSettings = new ArrayList<>();
        deprecatedSettings.add(GatewayService.EXPECTED_NODES_SETTING);
        deprecatedSettings.add(GatewayService.EXPECTED_MASTER_NODES_SETTING);
        deprecatedSettings.add(GatewayService.RECOVER_AFTER_NODES_SETTING);
        deprecatedSettings.add(GatewayService.RECOVER_AFTER_MASTER_NODES_SETTING);
        List<Setting<Integer>> existingSettings = deprecatedSettings.stream()
            .filter(deprecatedSetting -> deprecatedSetting.exists(settings))
            .collect(Collectors.toList());
        if (existingSettings.isEmpty()) {
            return null;
        }
        final String settingNames = existingSettings.stream().map(Setting::getKey).collect(Collectors.joining(","));
        final String message = String.format(
            Locale.ROOT,
            "Delaying cluster state recovery based on the number of available master nodes is not supported",
            settingNames
        );
        final String details = String.format(
            Locale.ROOT,
            "Use gateway.expected_data_nodes to wait for a certain number of data nodes. Remove the following settings or the node will "
                + "fail to start in 8.0: [%s]",
            settingNames
        );
        final String url = "https://ela.st/es-deprecation-7-deferred-cluster-state-recovery";
        return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, details, false, null);
    }

    static DeprecationIssue checkFixedAutoQueueSizeThreadpool(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        List<Setting<Integer>> deprecatedSettings = new ArrayList<>();
        deprecatedSettings.add(Setting.intSetting("thread_pool.search.min_queue_size", 1, Setting.Property.Deprecated));
        deprecatedSettings.add(Setting.intSetting("thread_pool.search.max_queue_size", 1, Setting.Property.Deprecated));
        deprecatedSettings.add(Setting.intSetting("thread_pool.search.auto_queue_frame_size", 1, Setting.Property.Deprecated));
        deprecatedSettings.add(Setting.intSetting("thread_pool.search.target_response_time", 1, Setting.Property.Deprecated));
        deprecatedSettings.add(Setting.intSetting("thread_pool.search_throttled.min_queue_size", 1, Setting.Property.Deprecated));
        deprecatedSettings.add(Setting.intSetting("thread_pool.search_throttled.max_queue_size", 1, Setting.Property.Deprecated));
        deprecatedSettings.add(Setting.intSetting("thread_pool.search_throttled.auto_queue_frame_size", 1, Setting.Property.Deprecated));
        deprecatedSettings.add(Setting.intSetting("thread_pool.search_throttled.target_response_time", 1, Setting.Property.Deprecated));
        List<Setting<Integer>> existingSettings = deprecatedSettings.stream()
            .filter(deprecatedSetting -> deprecatedSetting.exists(settings))
            .collect(Collectors.toList());
        if (existingSettings.isEmpty()) {
            return null;
        }
        final String settingNames = existingSettings.stream().map(Setting::getKey).collect(Collectors.joining(","));
        final String message = "The fixed_auto_queue_size threadpool type is not supported";
        final String details = String.format(
            Locale.ROOT,
            "Remove the following settings or the node will fail to start in 8.0: [%s].",
            settingNames
        );
        final String url = "https://ela.st/es-deprecation-7-fixed-auto-queue-size-settings";
        return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, details, false, null);
    }

    static DeprecationIssue checkClusterRoutingRequireSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return checkRemovedSetting(
            settings,
            CLUSTER_ROUTING_REQUIRE_SETTING,
            "https://ela.st/es-deprecation-7-tier-filtering-settings",
            "Use [index.routing.allocation.include._tier_preference] to control allocation to data tiers.",
            DeprecationIssue.Level.CRITICAL
        );
    }

    static DeprecationIssue checkClusterRoutingIncludeSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return checkRemovedSetting(
            settings,
            CLUSTER_ROUTING_INCLUDE_SETTING,
            "https://ela.st/es-deprecation-7-tier-filtering-settings",
            "Use [index.routing.allocation.include._tier_preference] to control allocation to data tiers.",
            DeprecationIssue.Level.CRITICAL
        );
    }

    static DeprecationIssue checkClusterRoutingExcludeSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return checkRemovedSetting(
            settings,
            CLUSTER_ROUTING_EXCLUDE_SETTING,
            "https://ela.st/es-deprecation-7-tier-filtering-settings",
            "Use [index.routing.allocation.include._tier_preference] to control allocation to data tiers.",
            DeprecationIssue.Level.CRITICAL
        );
    }

    static DeprecationIssue checkAcceptDefaultPasswordSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return checkRemovedSetting(
            settings,
            Setting.boolSetting(SecurityField.setting("authc.accept_default_password"), true, Setting.Property.Deprecated),
            "https://ela.st/es-deprecation-7-accept-default-password-setting",
            "This setting has not had any effect since 6.0.",
            DeprecationIssue.Level.CRITICAL
        );
    }

    static DeprecationIssue checkAcceptRolesCacheMaxSizeSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return checkRemovedSetting(
            settings,
            Setting.intSetting(SecurityField.setting("authz.store.roles.index.cache.max_size"), 10000, Setting.Property.Deprecated),
            "https://ela.st/es-deprecation-7-roles-index-cache-settings",
            "Native role cache settings have had no effect since 5.2.",
            DeprecationIssue.Level.CRITICAL
        );
    }

    static DeprecationIssue checkRolesCacheTTLSizeSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return checkRemovedSetting(
            settings,
            Setting.timeSetting(
                SecurityField.setting("authz.store.roles.index.cache.ttl"),
                TimeValue.timeValueMinutes(20),
                Setting.Property.Deprecated
            ),
            "https://ela.st/es-deprecation-7-roles-index-cache-settings",
            "Native role cache settings have had no effect since 5.2.",
            DeprecationIssue.Level.CRITICAL
        );
    }

    static DeprecationIssue checkMaxLocalStorageNodesSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return checkRemovedSetting(
            settings,
            NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING,
            "https://ela.st/es-deprecation-7-max-local-storage-nodes",
            "All nodes require local storage in 8.0 and cannot share data paths.",
            DeprecationIssue.Level.CRITICAL
        );
    }

    static DeprecationIssue checkSamlNameIdFormatSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        final String principalKeySuffix = ".attributes.principal";
        List<String> detailsList = PRINCIPAL_ATTRIBUTE.getAttribute()
            .getAllConcreteSettings(settings)
            .sorted(Comparator.comparing(Setting::getKey))
            .map(concreteSamlPrincipalSetting -> {
                String concreteSamlPrincipalSettingKey = concreteSamlPrincipalSetting.getKey();
                int principalKeySuffixIndex = concreteSamlPrincipalSettingKey.indexOf(principalKeySuffix);
                if (principalKeySuffixIndex > 0) {
                    String realm = concreteSamlPrincipalSettingKey.substring(0, principalKeySuffixIndex);
                    String concreteNameIdFormatSettingKey = realm + ".nameid_format";
                    if (settings.get(concreteNameIdFormatSettingKey) == null) {
                        return String.format(
                            Locale.ROOT,
                            "Configure \"%s\" for SAML realms: \"%s\".",
                            concreteNameIdFormatSettingKey,
                            realm
                        );
                    }
                }
                return null;
            })
            .filter(detail -> detail != null)
            .collect(Collectors.toList());
        if (detailsList.isEmpty()) {
            return null;
        } else {
            String message = "The SAML nameid_format is not set and no longer defaults to "
                + "\"urn:oasis:names:tc:SAML:2.0:nameid-format:transient\"";
            String url = "https://ela.st/es-deprecation-7-saml-nameid-format";
            String details = detailsList.stream().collect(Collectors.joining(" "));
            return new DeprecationIssue(DeprecationIssue.Level.WARNING, message, url, details, false, null);
        }
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
                "Remove the context-specific cache settings and set [script.max_compilations_rate] to configure the rate limit for the "
                    + "general cache. If no limit is set, the rate defaults to 150 compilations per five minutes: 150/5m. Context-specific "
                    + "caches are no longer needed to prevent system scripts from triggering rate limits.",
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
        if (contextCompilationRates.isEmpty() == false) {
            String maxSettings = contextCompilationRates.stream()
                .sorted()
                .map(c -> maxSetting.getConcreteSettingForNamespace(c).getKey())
                .collect(Collectors.joining(","));
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                "Setting a context-specific rate limit is deprecated",
                "https://ela.st/es-deprecation-7-script-context-cache",
                String.format(
                    Locale.ROOT,
                    "Remove the context-specific rate limit settings: [%s]. Instead, set [%s] to configure the "
                        + "rate limit for the general cache.  If no limit is set, the rate defaults to 150 compilations per five minutes: "
                        + "150/5m. Context-specific caches are no longer needed to prevent system scripts from triggering rate limits.",
                    maxSettings,
                    ScriptService.SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey()
                ),
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
                "Setting a context-specific cache size is deprecated",
                "https://ela.st/es-deprecation-7-script-context-cache",
                String.format(
                    Locale.ROOT,
                    "Remove the context-specific cache size settings: [%s]. Instead, set [script.cache_max_size] to configure the size of "
                        + "the general cache. Context-specific caches are no longer needed to prevent system scripts from triggering rate"
                        + " limits.",
                    cacheSizeSettings
                ),
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
                "Setting a context-specific cache expiration is deprecated",
                "https://ela.st/es-deprecation-7-script-context-cache",
                String.format(
                    Locale.ROOT,
                    "Remove the context-specific cache expiration settings: [%s]. Instead, set [script.cache.expire] to configure the "
                        + "expiration of the general cache. Context-specific caches are no longer needed to prevent system scripts from "
                        + "triggering rate limits.",
                    cacheExpireSettings
                ),
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

    static DeprecationIssue genericMonitoringSetting(final Settings settings, final Setting<?> deprecated) {
        return checkDeprecatedSetting(settings, deprecated, MONITORING_SETTING_DEPRECATION_LINK);
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

    static DeprecationIssue checkMonitoringSettingHistoryDuration(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringSetting(settings, Setting.simpleString("xpack.monitoring.history.duration"));
    }

    static DeprecationIssue checkMonitoringSettingCollectIndexRecovery(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringSetting(settings, Setting.simpleString("xpack.monitoring.collection.index.recovery.active_only"));
    }

    static DeprecationIssue checkMonitoringSettingCollectIndices(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringSetting(settings, Setting.simpleString("xpack.monitoring.collection.indices"));
    }

    static DeprecationIssue checkMonitoringSettingCollectCcrTimeout(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringSetting(settings, Setting.simpleString("xpack.monitoring.collection.ccr.stats.timeout"));
    }

    static DeprecationIssue checkMonitoringSettingCollectEnrichStatsTimeout(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringSetting(settings, Setting.simpleString("xpack.monitoring.collection.enrich.stats.timeout"));
    }

    static DeprecationIssue checkMonitoringSettingCollectIndexRecoveryStatsTimeout(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringSetting(settings, Setting.simpleString("xpack.monitoring.collection.index.recovery.timeout"));
    }

    static DeprecationIssue checkMonitoringSettingCollectIndexStatsTimeout(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringSetting(settings, Setting.simpleString("xpack.monitoring.collection.index.stats.timeout"));
    }

    static DeprecationIssue checkMonitoringSettingCollectMlJobStatsTimeout(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringSetting(settings, Setting.simpleString("xpack.monitoring.collection.ml.job.stats.timeout"));
    }

    static DeprecationIssue checkMonitoringSettingCollectNodeStatsTimeout(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringSetting(settings, Setting.simpleString("xpack.monitoring.collection.node.stats.timeout"));
    }

    static DeprecationIssue checkMonitoringSettingCollectClusterStatsTimeout(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringSetting(settings, Setting.simpleString("xpack.monitoring.collection.cluster.stats.timeout"));
    }

    static DeprecationIssue checkMonitoringSettingExportersHost(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringAffixSetting(settings, "host");
    }

    static DeprecationIssue checkMonitoringSettingExportersBulkTimeout(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringAffixSetting(settings, "bulk.timeout");
    }

    static DeprecationIssue checkMonitoringSettingExportersConnectionTimeout(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringAffixSetting(settings, "connection.timeout");
    }

    static DeprecationIssue checkMonitoringSettingExportersConnectionReadTimeout(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringAffixSetting(settings, "connection.read_timeout");
    }

    static DeprecationIssue checkMonitoringSettingExportersAuthUsername(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringAffixSetting(settings, "auth.username");
    }

    static DeprecationIssue checkMonitoringSettingExportersAuthPass(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringAffixSecureSetting(settings, "auth.secure_password");
    }

    static DeprecationIssue checkMonitoringSettingExportersSSL(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringAffixGroupedSetting(settings, "ssl");
    }

    static DeprecationIssue checkMonitoringSettingExportersProxyBase(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringAffixSetting(settings, "proxy.base_path");
    }

    static DeprecationIssue checkMonitoringSettingExportersSniffEnabled(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringAffixSetting(settings, "sniff.enabled");
    }

    static DeprecationIssue checkMonitoringSettingExportersHeaders(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringAffixGroupedSetting(settings, "headers");
    }

    static DeprecationIssue checkMonitoringSettingExportersTemplateTimeout(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringAffixSetting(settings, "index.template.master_timeout");
    }

    static DeprecationIssue checkMonitoringSettingExportersMasterTimeout(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringAffixSetting(settings, "wait_master.timeout");
    }

    static DeprecationIssue checkMonitoringSettingExportersEnabled(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringAffixSetting(settings, "enabled");
    }

    static DeprecationIssue checkMonitoringSettingExportersType(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringAffixSetting(settings, "type");
    }

    static DeprecationIssue checkMonitoringSettingExportersAlertsEnabled(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringAffixSetting(settings, "cluster_alerts.management.enabled");
    }

    static DeprecationIssue checkMonitoringSettingExportersAlertsBlacklist(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringAffixSetting(settings, "cluster_alerts.management.blacklist");
    }

    static DeprecationIssue checkMonitoringSettingExportersIndexNameTimeFormat(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringAffixSetting(settings, "index.name.time_format");
    }

    static DeprecationIssue checkMonitoringSettingDecommissionAlerts(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringSetting(settings, Setting.simpleString("xpack.monitoring.migration.decommission_alerts"));
    }

    static DeprecationIssue checkMonitoringSettingEsCollectionEnabled(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringSetting(settings, Setting.simpleString("xpack.monitoring.elasticsearch.collection.enabled"));
    }

    static DeprecationIssue checkMonitoringSettingCollectionEnabled(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringSetting(settings, Setting.simpleString("xpack.monitoring.collection.enabled"));
    }

    static DeprecationIssue checkMonitoringSettingCollectionInterval(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return genericMonitoringSetting(settings, Setting.simpleString("xpack.monitoring.collection.interval"));
    }

    static DeprecationIssue checkExporterUseIngestPipelineSettings(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return deprecatedAffixSetting(
            Setting.affixKeySetting("xpack.monitoring.exporters.", "use_ingest", key -> Setting.boolSetting(key, true)),
            "Remove the following settings from elasticsearch.yml: [%s]",
            "https://ela.st/es-deprecation-7-monitoring-exporter-use-ingest-setting",
            DeprecationIssue.Level.WARNING,
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
            Setting.affixKeySetting(
                "xpack.monitoring.exporters.",
                "index.pipeline.master_timeout",
                (key) -> Setting.timeSetting(key, TimeValue.MINUS_ONE)
            ),
            "Remove the following settings from elasticsearch.yml: [%s]",
            "https://ela.st/es-deprecation-7-monitoring-exporter-pipeline-timeout-setting",
            DeprecationIssue.Level.WARNING,
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
            Setting.affixKeySetting(
                "xpack.monitoring.exporters.",
                "index.template.create_legacy_templates",
                (key) -> Setting.boolSetting(key, true)
            ),
            "Remove the following settings from elasticsearch.yml: [%s]",
            "https://ela.st/es-deprecation-7-monitoring-exporter-create-legacy-template-setting",
            DeprecationIssue.Level.WARNING,
            settings
        );
    }

    static DeprecationIssue checkSettingNoReplacement(Settings settings, Setting<?> deprecatedSetting, String url) {
        assert deprecatedSetting.isDeprecated() : deprecatedSetting;
        if (deprecatedSetting.exists(settings) == false) {
            return null;
        }
        final String deprecatedSettingKey = deprecatedSetting.getKey();
        final String message = String.format(Locale.ROOT, "Setting [%s] is deprecated", deprecatedSettingKey);
        final String details = String.format(Locale.ROOT, "Remove the [%s] setting.", deprecatedSetting.getKey());
        return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, details, false, null);
    }

    static DeprecationIssue checkReroutePrioritySetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<Priority> deprecatedSetting = ShardStateAction.FOLLOW_UP_REROUTE_PRIORITY_SETTING;
        String url = "https://ela.st/es-deprecation-7-reroute-priority-setting";
        return checkRemovedSetting(
            settings,
            deprecatedSetting,
            url,
            "In a future release this setting will have no effect and the priority will always be NORMAL."
        );
    }

    static DeprecationIssue checkZenBwcPingTimeoutSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<TimeValue> deprecatedSetting = DiscoveryUpgradeService.BWC_PING_TIMEOUT_SETTING;
        String url = "https://ela.st/es-deprecation-7-unused_zen_settings";
        return checkSettingNoReplacement(settings, deprecatedSetting, url);
    }

    static DeprecationIssue checkZenUnsafeBootstrappingOnUpgradeSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<Boolean> deprecatedSetting = DiscoveryUpgradeService.ENABLE_UNSAFE_BOOTSTRAPPING_ON_UPGRADE_SETTING;
        String url = "https://ela.st/es-deprecation-7-unused_zen_settings";
        return checkSettingNoReplacement(settings, deprecatedSetting, url);
    }

    static DeprecationIssue checkZenCommitTimeoutSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<TimeValue> deprecatedSetting = DiscoverySettings.COMMIT_TIMEOUT_SETTING;
        String url = "https://ela.st/es-deprecation-7-unused_zen_settings";
        return checkSettingNoReplacement(settings, deprecatedSetting, url);
    }

    static DeprecationIssue checkZenPublishDiffEnableSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<Boolean> deprecatedSetting = DiscoverySettings.PUBLISH_DIFF_ENABLE_SETTING;
        String url = "https://ela.st/es-deprecation-7-unused_zen_settings";
        return checkSettingNoReplacement(settings, deprecatedSetting, url);
    }

    static DeprecationIssue checkZenConnectOnNetworkDisconnectSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<Boolean> deprecatedSetting = FaultDetection.CONNECT_ON_NETWORK_DISCONNECT_SETTING;
        String url = "https://ela.st/es-deprecation-7-unused_zen_settings";
        return checkSettingNoReplacement(settings, deprecatedSetting, url);
    }

    static DeprecationIssue checkZenPingIntervalSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<TimeValue> deprecatedSetting = FaultDetection.PING_INTERVAL_SETTING;
        String url = "https://ela.st/es-deprecation-7-unused_zen_settings";
        return checkSettingNoReplacement(settings, deprecatedSetting, url);
    }

    static DeprecationIssue checkZenPingTimeoutSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<TimeValue> deprecatedSetting = FaultDetection.PING_TIMEOUT_SETTING;
        String url = "https://ela.st/es-deprecation-7-unused_zen_settings";
        return checkSettingNoReplacement(settings, deprecatedSetting, url);
    }

    static DeprecationIssue checkZenPingRetriesSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<Integer> deprecatedSetting = FaultDetection.PING_RETRIES_SETTING;
        String url = "https://ela.st/es-deprecation-7-unused_zen_settings";
        return checkSettingNoReplacement(settings, deprecatedSetting, url);
    }

    static DeprecationIssue checkZenRegisterConnectionListenerSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<Boolean> deprecatedSetting = FaultDetection.REGISTER_CONNECTION_LISTENER_SETTING;
        String url = "https://ela.st/es-deprecation-7-unused_zen_settings";
        return checkSettingNoReplacement(settings, deprecatedSetting, url);
    }

    static DeprecationIssue checkAutoImportDanglingIndicesSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<Boolean> deprecatedSetting = DanglingIndicesState.AUTO_IMPORT_DANGLING_INDICES_SETTING;
        String url = "https://ela.st/es-deprecation-7-auto-import-dangling-indices-setting";
        return checkRemovedSetting(settings, deprecatedSetting, url, "Use of this setting is unsafe.");
    }

    static DeprecationIssue checkHttpContentTypeRequiredSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<Boolean> deprecatedSetting = HttpTransportSettings.SETTING_HTTP_CONTENT_TYPE_REQUIRED;
        String url = "https://ela.st/es-deprecation-7-http-content-type-required-setting";
        return checkRemovedSetting(settings, deprecatedSetting, url, "This setting no longer has any effect.");
    }

    static DeprecationIssue checkFsRepositoryCompressionSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<Boolean> deprecatedSetting = FsRepository.REPOSITORIES_COMPRESS_SETTING;
        Setting<Boolean> replacementSetting = FsRepository.COMPRESS_SETTING;
        String url = "https://ela.st/es-deprecation-7-filesystem-repository-compression-setting";
        return checkDeprecatedSetting(settings, pluginsAndModules, deprecatedSetting, replacementSetting, url);
    }

    static DeprecationIssue checkHttpTcpNoDelaySetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<Boolean> deprecatedSetting = HttpTransportSettings.OLD_SETTING_HTTP_TCP_NO_DELAY;
        Setting<Boolean> replacementSetting = HttpTransportSettings.SETTING_HTTP_TCP_NO_DELAY;
        String url = "https://ela.st/es-deprecation-7-transport-settings";
        return checkDeprecatedSetting(settings, pluginsAndModules, deprecatedSetting, replacementSetting, url);
    }

    static DeprecationIssue checkNetworkTcpConnectTimeoutSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<TimeValue> deprecatedSetting = NetworkService.TCP_CONNECT_TIMEOUT;
        Setting<TimeValue> replacementSetting = TransportSettings.CONNECT_TIMEOUT;
        String url = "https://ela.st/es-deprecation-7-transport-settings";
        return checkDeprecatedSetting(settings, pluginsAndModules, deprecatedSetting, replacementSetting, url);
    }

    static DeprecationIssue checkTransportTcpConnectTimeoutSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<TimeValue> deprecatedSetting = TransportSettings.TCP_CONNECT_TIMEOUT;
        Setting<TimeValue> replacementSetting = TransportSettings.CONNECT_TIMEOUT;
        String url = "https://ela.st/es-deprecation-7-transport-settings";
        return checkDeprecatedSetting(settings, pluginsAndModules, deprecatedSetting, replacementSetting, url);
    }

    static DeprecationIssue checkTransportPortSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<String> deprecatedSetting = TransportSettings.OLD_PORT;
        Setting<String> replacementSetting = TransportSettings.PORT;
        String url = "https://ela.st/es-deprecation-7-transport-settings";
        return checkDeprecatedSetting(settings, pluginsAndModules, deprecatedSetting, replacementSetting, url);
    }

    static DeprecationIssue checkTcpNoDelaySetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<Boolean> deprecatedSetting = TransportSettings.OLD_TCP_NO_DELAY;
        Setting<Boolean> replacementSetting = TransportSettings.TCP_NO_DELAY;
        String url = "https://ela.st/es-deprecation-7-transport-settings";
        return checkDeprecatedSetting(settings, pluginsAndModules, deprecatedSetting, replacementSetting, url);
    }

    static DeprecationIssue checkTransportCompressSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<Compression.Enabled> deprecatedSetting = TransportSettings.OLD_TRANSPORT_COMPRESS;
        Setting<Compression.Enabled> replacementSetting = TransportSettings.TRANSPORT_COMPRESS;
        String url = "https://ela.st/es-deprecation-7-transport-settings";
        return checkDeprecatedSetting(settings, pluginsAndModules, deprecatedSetting, replacementSetting, url);
    }

    static DeprecationIssue checkXpackDataFrameEnabledSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<Boolean> deprecatedSetting = Setting.boolSetting(
            "xpack.data_frame.enabled",
            true,
            Setting.Property.NodeScope,
            Setting.Property.Deprecated
        );
        String url = "https://ela.st/es-deprecation-7-xpack-dataframe-setting";
        return checkRemovedSetting(settings, deprecatedSetting, url, "As of 7.9.2 basic license level features are always enabled.");
    }

    static DeprecationIssue checkWatcherHistoryCleanerServiceSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<Boolean> deprecatedSetting = Monitoring.CLEAN_WATCHER_HISTORY;
        String url = "https://ela.st/es-deprecation-7-watcher-history-cleaner-setting";
        return checkRemovedSetting(
            settings,
            deprecatedSetting,
            url,
            "Watcher history indices are now managed by the watch-history-ilm-policy ILM policy."
        );
    }

    static DeprecationIssue checkLifecyleStepMasterTimeoutSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<TimeValue> deprecatedSetting = LifecycleSettings.LIFECYCLE_STEP_MASTER_TIMEOUT_SETTING;
        String url = "https://ela.st/es-deprecation-7-lifecycle-master-timeout-setting";
        return checkRemovedSetting(
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
            settings,
            deprecatedSetting,
            url,
            "As of 7.9.2 basic license level features are always enabled.",
            DeprecationIssue.Level.WARNING
        );
    }
}

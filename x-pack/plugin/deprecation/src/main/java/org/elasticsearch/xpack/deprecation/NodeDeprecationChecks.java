/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.bootstrap.BootstrapSettings;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.coordination.DiscoveryUpgradeService;
import org.elasticsearch.cluster.coordination.JoinHelper;
import org.elasticsearch.cluster.coordination.NoMasterBlockService;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.allocation.DataTier;
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
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.SeedHostsResolver;
import org.elasticsearch.discovery.SettingsBasedSeedHostsProvider;
import org.elasticsearch.discovery.zen.ElectMasterService;
import org.elasticsearch.discovery.zen.FaultDetection;
import org.elasticsearch.discovery.zen.ZenDiscovery;
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
import java.util.Collections;
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
import java.util.stream.Stream;

import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_INCLUDE_RELOCATIONS_SETTING;
import static org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider.CLUSTER_ROUTING_EXCLUDE_SETTING;
import static org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider.CLUSTER_ROUTING_INCLUDE_SETTING;
import static org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider.CLUSTER_ROUTING_REQUIRE_SETTING;
import static org.elasticsearch.xpack.core.ilm.LifecycleSettings.LIFECYCLE_POLL_INTERVAL_SETTING;
import static org.elasticsearch.xpack.core.security.authc.RealmSettings.RESERVED_REALM_NAME_PREFIX;
import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.PRINCIPAL_ATTRIBUTE;

class NodeDeprecationChecks {

    private static final Logger logger = LogManager.getLogger(NodeDeprecationChecks.class);
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
            String details = "In Elasticsearch 8.0 the [xpack.security.enabled] setting will always default to true. "
                + "In your environment, the value is not set and you need to set it before upgrading, along with other required "
                + "security settings. See https://ela.st/es-deprecation-7-implicitly-disabled-security to secure your cluster. "
                + "To explicitly disable security, set [xpack.security.enabled] to \"false\" (not recommended).";
            return new DeprecationIssue(
                DeprecationIssue.Level.CRITICAL,
                "Security features are enabled by default for all licenses in versions 8.0 and later",
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

    static DeprecationIssue checkThreadPoolListenerQueueSize(final Settings settings, final ClusterState clusterState) {
        return checkThreadPoolListenerSetting("thread_pool.listener.queue_size", settings, clusterState);
    }

    static DeprecationIssue checkThreadPoolListenerSize(final Settings settings, final ClusterState clusterState) {
        return checkThreadPoolListenerSetting("thread_pool.listener.size", settings, clusterState);
    }

    private static DeprecationIssue checkThreadPoolListenerSetting(
        final String name,
        final Settings settings,
        final ClusterState clusterState
    ) {
        final FixedExecutorBuilder builder = new FixedExecutorBuilder(settings, "listener", 1, -1, "thread_pool.listener", true);
        final List<Setting<?>> listenerSettings = builder.getRegisteredSettings();
        final Optional<Setting<?>> setting = listenerSettings.stream().filter(s -> s.getKey().equals(name)).findFirst();
        assert setting.isPresent();
        return checkRemovedSetting(
            clusterState.metadata().settings(),
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
            clusterState.metadata().settings(),
            settings,
            Node.NODE_LOCAL_STORAGE_SETTING,
            "https://ela.st/es-deprecation-7-node-local-storage-setting",
            "All nodes require local storage in 8.0 and cannot share data paths."
        );
    }

    public static DeprecationIssue checkNodeBasicLicenseFeatureEnabledSetting(
        final Settings settings,
        Setting<?> setting,
        final ClusterState clusterState
    ) {
        return checkRemovedSetting(
            clusterState.metadata().settings(),
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
            clusterState.metadata().settings(),
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
        final Settings clusterSettings,
        final Settings nodeSettings,
        final Setting<?> removedSetting,
        final String url,
        String additionalDetailMessage
    ) {
        return checkRemovedSetting(
            clusterSettings,
            nodeSettings,
            removedSetting,
            url,
            additionalDetailMessage,
            DeprecationIssue.Level.CRITICAL
        );
    }

    static DeprecationIssue checkDeprecatedSetting(
        final Settings clusterSettings,
        final Settings nodeSettings,
        final Setting<?> deprecatedSetting,
        final String url
    ) {
        if (deprecatedSetting.exists(clusterSettings) == false && deprecatedSetting.exists(nodeSettings) == false) {
            return null;
        }
        boolean canAutoRemoveSetting = deprecatedSetting.exists(clusterSettings) && deprecatedSetting.exists(nodeSettings) == false;
        final String deprecatedSettingKey = deprecatedSetting.getKey();
        final String value = deprecatedSetting.exists(clusterSettings)
            ? deprecatedSetting.get(clusterSettings).toString()
            : deprecatedSetting.get(nodeSettings).toString();
        final String message = String.format(Locale.ROOT, "Setting [%s] is deprecated", deprecatedSettingKey);
        final String details = String.format(Locale.ROOT, "Remove the [%s] setting.", deprecatedSettingKey, value);
        final Map<String, Object> meta = createMetaMapForRemovableSettings(canAutoRemoveSetting, deprecatedSettingKey);
        return new DeprecationIssue(DeprecationIssue.Level.WARNING, message, url, details, false, meta);
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
        if ((clusterSettings == null || removedSetting.exists(clusterSettings) == false)
            && (nodeSettings == null || removedSetting.exists(nodeSettings) == false)) {
            return null;
        }
        final String removedSettingKey = removedSetting.getKey();
        Object removedSettingValue = removedSetting.exists(clusterSettings)
            ? removedSetting.get(clusterSettings).toString()
            : removedSetting.get(nodeSettings).toString();
        String value;
        if (removedSettingValue instanceof TimeValue) {
            value = ((TimeValue) removedSettingValue).getStringRep();
        } else {
            value = removedSettingValue.toString();
        }
        final String message = String.format(Locale.ROOT, "Setting [%s] is deprecated", removedSettingKey);
        final String details = String.format(Locale.ROOT, "Remove the [%s] setting. %s", removedSettingKey, additionalDetailMessage);
        boolean canAutoRemoveSetting = removedSetting.exists(clusterSettings) && removedSetting.exists(nodeSettings) == false;
        Map<String, Object> meta = createMetaMapForRemovableSettings(canAutoRemoveSetting, removedSettingKey);
        return new DeprecationIssue(deprecationLevel, message, url, details, false, meta);
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
                DeprecationIssue.Level.WARNING,
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
                DeprecationIssue.Level.WARNING,
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
            return new DeprecationIssue(DeprecationIssue.Level.WARNING, message, url, details, false, null);
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
        ClusterState clusterState,
        XPackLicenseState licenseState
    ) {
        return deprecatedAffixSetting(
            Setting.affixKeySetting(
                "xpack.monitoring.exporters.",
                "auth.password",
                (Function<String, Setting<String>>) Setting::simpleString
            ),
            "Monitoring exporters must use secure passwords",
            "Remove the non-secure monitoring exporter password settings: [%s]. Configure secure passwords with "
                + "[xpack.monitoring.exporters.*.auth.secure_password].",
            "https://ela.st/es-deprecation-7-monitoring-exporter-passwords",
            DeprecationIssue.Level.CRITICAL,
            clusterState.metadata().settings(),
            settings
        );
    }

    static DeprecationIssue checkJoinTimeoutSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        return checkRemovedSetting(
            clusterState.metadata().settings(),
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
            clusterState.metadata().settings(),
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
            clusterState.metadata().settings(),
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
            clusterState.metadata().settings(),
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
            clusterState.metadata().settings(),
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
            clusterState.metadata().settings(),
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
            clusterState.metadata().settings(),
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
            clusterState.metadata().settings(),
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
            clusterState.metadata().settings(),
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
        String detail =
            "Remove the context-specific cache settings and set [script.max_compilations_rate] to configure the rate limit for the "
                + "general cache. If no limit is set, the rate defaults to 150 compilations per five minutes: 150/5m. Context-specific "
                + "caches are no longer needed to prevent system scripts from triggering rate limits.";
        if (ScriptService.isUseContextCacheSet(settings)) {
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                ScriptService.USE_CONTEXT_RATE_KEY_DEPRECATION_MESSAGE,
                "https://ela.st/es-deprecation-7-script-context-cache",
                detail,
                false,
                null
            );
        } else if (ScriptService.isImplicitContextCacheSet(settings)) {
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                ScriptService.contextDeprecationMessage(settings),
                "https://ela.st/es-deprecation-7-script-context-cache",
                detail,
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
        Settings clusterSettings,
        Settings nodeSettings
    ) {
        return deprecatedAffixSetting(deprecatedAffixSetting, null, detailPattern, url, warningLevel, clusterSettings, nodeSettings);
    }

    private static DeprecationIssue deprecatedAffixSetting(
        Setting.AffixSetting<?> deprecatedAffixSetting,
        String message,
        String detailPattern,
        String url,
        DeprecationIssue.Level warningLevel,
        Settings clusterSettings,
        Settings nodeSettings
    ) {
        List<Setting<?>> deprecatedConcreteNodeSettings = deprecatedAffixSetting.getAllConcreteSettings(nodeSettings)
            .sorted(Comparator.comparing(Setting::getKey))
            .collect(Collectors.toList());
        List<Setting<?>> deprecatedConcreteClusterSettings = deprecatedAffixSetting.getAllConcreteSettings(clusterSettings)
            .sorted(Comparator.comparing(Setting::getKey))
            .collect(Collectors.toList());

        if (deprecatedConcreteNodeSettings.isEmpty() && deprecatedConcreteClusterSettings.isEmpty()) {
            return null;
        }

        List<String> deprecatedNodeSettingKeys = deprecatedConcreteNodeSettings.stream().map(Setting::getKey).collect(Collectors.toList());
        List<String> deprecatedClusterSettingKeys = deprecatedConcreteClusterSettings.stream()
            .map(Setting::getKey)
            .collect(Collectors.toList());

        final String concatSettingNames = Stream.concat(deprecatedNodeSettingKeys.stream(), deprecatedClusterSettingKeys.stream())
            .distinct()
            .collect(Collectors.joining(","));
        if (message == null) {
            message = String.format(Locale.ROOT, "The [%s] settings are deprecated and will be removed after 8.0", concatSettingNames);
        }
        final String details = String.format(Locale.ROOT, detailPattern, concatSettingNames);
        boolean canBeFixedByRemovingDynamicSetting = deprecatedNodeSettingKeys.containsAll(deprecatedClusterSettingKeys) == false;
        if (canBeFixedByRemovingDynamicSetting) {
            deprecatedClusterSettingKeys.removeAll(deprecatedNodeSettingKeys);
        }
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
            .collect(Collectors.toList());
        List<Setting<Settings>> deprecatedConcreteClusterSettings = deprecatedAffixSetting.getAllConcreteSettings(clusterSettings)
            .sorted(Comparator.comparing(Setting::getKey))
            .collect(Collectors.toList());

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
        List<String> allNodeSubSettingKeys = deprecatedConcreteNodeSettings.stream().map(affixSetting -> {
            String groupPrefix = affixSetting.getKey();
            Settings groupSettings = affixSetting.get(nodeSettings);
            Set<String> subSettings = groupSettings.keySet();
            return subSettings.stream().map(key -> groupPrefix + key).collect(Collectors.toList());
        }).flatMap(List::stream).sorted().collect(Collectors.toList());

        List<String> allClusterSubSettingKeys = deprecatedConcreteClusterSettings.stream().map(affixSetting -> {
            String groupPrefix = affixSetting.getKey();
            Settings groupSettings = affixSetting.get(clusterSettings);
            Set<String> subSettings = groupSettings.keySet();
            return subSettings.stream().map(key -> groupPrefix + key).collect(Collectors.toList());
        }).flatMap(List::stream).sorted().collect(Collectors.toList());

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
        boolean canBeFixedByRemovingDynamicSetting = allNodeSubSettingKeys.containsAll(allClusterSubSettingKeys) == false;
        if (canBeFixedByRemovingDynamicSetting) {
            allClusterSubSettingKeys.removeAll(allNodeSubSettingKeys);
        }
        /* Removing affix settings can cause more problems than it's worth, so always make meta null even if the settings are only set
         * dynamically
         */
        final Map<String, Object> meta = null;
        return new DeprecationIssue(warningLevel, message, url, details, false, meta);
    }

    private static final String MONITORING_SETTING_DEPRECATION_LINK = "https://ela.st/es-deprecation-7-monitoring-settings";

    static DeprecationIssue genericMonitoringSetting(
        final ClusterState clusterState,
        final Settings nodeSettings,
        final Setting<?> deprecated
    ) {
        return checkDeprecatedSetting(clusterState.metadata().settings(), nodeSettings, deprecated, MONITORING_SETTING_DEPRECATION_LINK);
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
            Setting.affixKeySetting("xpack.monitoring.exporters.", "use_ingest", key -> Setting.boolSetting(key, true)),
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
            Setting.affixKeySetting(
                "xpack.monitoring.exporters.",
                "index.pipeline.master_timeout",
                (key) -> Setting.timeSetting(key, TimeValue.MINUS_ONE)
            ),
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
            Setting.affixKeySetting(
                "xpack.monitoring.exporters.",
                "index.template.create_legacy_templates",
                (key) -> Setting.boolSetting(key, true)
            ),
            "Remove the following settings: [%s]",
            "https://ela.st/es-deprecation-7-monitoring-exporter-create-legacy-template-setting",
            DeprecationIssue.Level.WARNING,
            clusterState.metadata().settings(),
            settings
        );
    }

    static DeprecationIssue checkSettingNoReplacement(Settings settings, Setting<?> deprecatedSetting, String url) {
        return checkSettingNoReplacement(Settings.EMPTY, settings, deprecatedSetting, url);

    }

    private static DeprecationIssue checkSettingNoReplacement(
        Settings clusterSettings,
        Settings nodeSettings,
        Setting<?> deprecatedSetting,
        String url
    ) {
        assert deprecatedSetting.isDeprecated() : deprecatedSetting;
        if (deprecatedSetting.exists(clusterSettings) == false && deprecatedSetting.exists(nodeSettings) == false) {
            return null;
        }
        boolean canAutoRemoveSetting = deprecatedSetting.exists(clusterSettings) && deprecatedSetting.exists(nodeSettings) == false;
        final String deprecatedSettingKey = deprecatedSetting.getKey();
        final String message = String.format(Locale.ROOT, "Setting [%s] is deprecated", deprecatedSettingKey);
        final String details = String.format(Locale.ROOT, "Remove the [%s] setting.", deprecatedSetting.getKey());
        final Map<String, Object> meta = createMetaMapForRemovableSettings(canAutoRemoveSetting, deprecatedSettingKey);
        return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, details, false, meta);
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
            clusterState.metadata().settings(),
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
        return checkSettingNoReplacement(clusterState.metadata().settings(), settings, deprecatedSetting, url);
    }

    static DeprecationIssue checkZenUnsafeBootstrappingOnUpgradeSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<Boolean> deprecatedSetting = DiscoveryUpgradeService.ENABLE_UNSAFE_BOOTSTRAPPING_ON_UPGRADE_SETTING;
        String url = "https://ela.st/es-deprecation-7-unused_zen_settings";
        return checkSettingNoReplacement(clusterState.metadata().settings(), settings, deprecatedSetting, url);
    }

    static DeprecationIssue checkZenCommitTimeoutSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<TimeValue> deprecatedSetting = DiscoverySettings.COMMIT_TIMEOUT_SETTING;
        String url = "https://ela.st/es-deprecation-7-unused_zen_settings";
        return checkSettingNoReplacement(clusterState.metadata().settings(), settings, deprecatedSetting, url);
    }

    static DeprecationIssue checkZenPublishDiffEnableSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<Boolean> deprecatedSetting = DiscoverySettings.PUBLISH_DIFF_ENABLE_SETTING;
        String url = "https://ela.st/es-deprecation-7-unused_zen_settings";
        return checkSettingNoReplacement(clusterState.metadata().settings(), settings, deprecatedSetting, url);
    }

    static DeprecationIssue checkZenConnectOnNetworkDisconnectSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<Boolean> deprecatedSetting = FaultDetection.CONNECT_ON_NETWORK_DISCONNECT_SETTING;
        String url = "https://ela.st/es-deprecation-7-unused_zen_settings";
        return checkSettingNoReplacement(clusterState.metadata().settings(), settings, deprecatedSetting, url);
    }

    static DeprecationIssue checkZenPingIntervalSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<TimeValue> deprecatedSetting = FaultDetection.PING_INTERVAL_SETTING;
        String url = "https://ela.st/es-deprecation-7-unused_zen_settings";
        return checkSettingNoReplacement(clusterState.metadata().settings(), settings, deprecatedSetting, url);
    }

    static DeprecationIssue checkZenFDPingTimeoutSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<TimeValue> deprecatedSetting = FaultDetection.PING_TIMEOUT_SETTING;
        String url = "https://ela.st/es-deprecation-7-unused_zen_settings";
        return checkSettingNoReplacement(clusterState.metadata().settings(), settings, deprecatedSetting, url);
    }

    static DeprecationIssue checkZenPingRetriesSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<Integer> deprecatedSetting = FaultDetection.PING_RETRIES_SETTING;
        String url = "https://ela.st/es-deprecation-7-unused_zen_settings";
        return checkSettingNoReplacement(clusterState.metadata().settings(), settings, deprecatedSetting, url);
    }

    static DeprecationIssue checkZenRegisterConnectionListenerSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<Boolean> deprecatedSetting = FaultDetection.REGISTER_CONNECTION_LISTENER_SETTING;
        String url = "https://ela.st/es-deprecation-7-unused_zen_settings";
        return checkSettingNoReplacement(clusterState.metadata().settings(), settings, deprecatedSetting, url);
    }

    static DeprecationIssue checkZenMinimumMasterNodesSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<Integer> deprecatedSetting = ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING;
        String url = "https://ela.st/es-deprecation-7-unused_zen_settings";
        return checkSettingNoReplacement(clusterState.metadata().settings(), settings, deprecatedSetting, url);
    }

    static DeprecationIssue checkZenHostsProviderSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<List<String>> deprecatedSetting = DiscoveryModule.LEGACY_DISCOVERY_HOSTS_PROVIDER_SETTING;
        String url = "https://ela.st/es-deprecation-7-unused_zen_settings";
        return checkSettingNoReplacement(clusterState.metadata().settings(), settings, deprecatedSetting, url);
    }

    static DeprecationIssue checkZenJoinRetryAttemptsSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<Integer> deprecatedSetting = ZenDiscovery.JOIN_RETRY_ATTEMPTS_SETTING;
        String url = "https://ela.st/es-deprecation-7-unused_zen_settings";
        return checkSettingNoReplacement(clusterState.metadata().settings(), settings, deprecatedSetting, url);
    }

    static DeprecationIssue checkZenJoinRetryDelaySetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<TimeValue> deprecatedSetting = ZenDiscovery.JOIN_RETRY_DELAY_SETTING;
        String url = "https://ela.st/es-deprecation-7-unused_zen_settings";
        return checkSettingNoReplacement(clusterState.metadata().settings(), settings, deprecatedSetting, url);
    }

    static DeprecationIssue checkZenJoinTimeoutSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<TimeValue> deprecatedSetting = ZenDiscovery.JOIN_TIMEOUT_SETTING;
        String url = "https://ela.st/es-deprecation-7-unused_zen_settings";
        return checkSettingNoReplacement(clusterState.metadata().settings(), settings, deprecatedSetting, url);
    }

    static DeprecationIssue checkZenMasterElectionIgnoreNonMasterPingsSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<Boolean> deprecatedSetting = ZenDiscovery.MASTER_ELECTION_IGNORE_NON_MASTER_PINGS_SETTING;
        String url = "https://ela.st/es-deprecation-7-unused_zen_settings";
        return checkSettingNoReplacement(clusterState.metadata().settings(), settings, deprecatedSetting, url);
    }

    static DeprecationIssue checkZenMasterElectionWaitForJoinsTimeoutSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<TimeValue> deprecatedSetting = ZenDiscovery.MASTER_ELECTION_WAIT_FOR_JOINS_TIMEOUT_SETTING;
        String url = "https://ela.st/es-deprecation-7-unused_zen_settings";
        return checkSettingNoReplacement(clusterState.metadata().settings(), settings, deprecatedSetting, url);
    }

    static DeprecationIssue checkZenMaxPingsFromAnotherMasterSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<Integer> deprecatedSetting = ZenDiscovery.MAX_PINGS_FROM_ANOTHER_MASTER_SETTING;
        String url = "https://ela.st/es-deprecation-7-unused_zen_settings";
        return checkSettingNoReplacement(clusterState.metadata().settings(), settings, deprecatedSetting, url);
    }

    static DeprecationIssue checkZenNoMasterBlockSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<ClusterBlock> deprecatedSetting = NoMasterBlockService.LEGACY_NO_MASTER_BLOCK_SETTING;
        String url = "https://ela.st/es-deprecation-7-unused_zen_settings";
        return checkSettingNoReplacement(clusterState.metadata().settings(), settings, deprecatedSetting, url);
    }

    static DeprecationIssue checkZenPintUnicastConcurrentConnectssSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<Integer> deprecatedSetting = SeedHostsResolver.LEGACY_DISCOVERY_ZEN_PING_UNICAST_CONCURRENT_CONNECTS_SETTING;
        String url = "https://ela.st/es-deprecation-7-unused_zen_settings";
        return checkSettingNoReplacement(clusterState.metadata().settings(), settings, deprecatedSetting, url);
    }

    static DeprecationIssue checkZenPingUnicastHostsSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<List<String>> deprecatedSetting = SettingsBasedSeedHostsProvider.LEGACY_DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING;
        String url = "https://ela.st/es-deprecation-7-unused_zen_settings";
        return checkSettingNoReplacement(clusterState.metadata().settings(), settings, deprecatedSetting, url);
    }

    static DeprecationIssue checkZenPingUnicastHostsResolveTimeoutSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<TimeValue> deprecatedSetting = SeedHostsResolver.LEGACY_DISCOVERY_ZEN_PING_UNICAST_HOSTS_RESOLVE_TIMEOUT;
        String url = "https://ela.st/es-deprecation-7-unused_zen_settings";
        return checkSettingNoReplacement(clusterState.metadata().settings(), settings, deprecatedSetting, url);
    }

    static DeprecationIssue checkZenPingTimeoutSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<TimeValue> deprecatedSetting = ZenDiscovery.PING_TIMEOUT_SETTING;
        String url = "https://ela.st/es-deprecation-7-unused_zen_settings";
        return checkSettingNoReplacement(clusterState.metadata().settings(), settings, deprecatedSetting, url);
    }

    static DeprecationIssue checkZenPublishMaxPendingClusterStatesSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<Integer> deprecatedSetting = ZenDiscovery.MAX_PENDING_CLUSTER_STATES_SETTING;
        String url = "https://ela.st/es-deprecation-7-unused_zen_settings";
        return checkSettingNoReplacement(clusterState.metadata().settings(), settings, deprecatedSetting, url);
    }

    static DeprecationIssue checkZenPublishTimeoutSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<TimeValue> deprecatedSetting = DiscoverySettings.PUBLISH_TIMEOUT_SETTING;
        String url = "https://ela.st/es-deprecation-7-unused_zen_settings";
        return checkSettingNoReplacement(clusterState.metadata().settings(), settings, deprecatedSetting, url);
    }

    static DeprecationIssue checkZenPingSendLeaveRequestSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<Boolean> deprecatedSetting = ZenDiscovery.SEND_LEAVE_REQUEST_SETTING;
        String url = "https://ela.st/es-deprecation-7-unused_zen_settings";
        return checkSettingNoReplacement(clusterState.metadata().settings(), settings, deprecatedSetting, url);
    }

    static DeprecationIssue checkAutoImportDanglingIndicesSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<Boolean> deprecatedSetting = DanglingIndicesState.AUTO_IMPORT_DANGLING_INDICES_SETTING;
        String url = "https://ela.st/es-deprecation-7-auto-import-dangling-indices-setting";
        return checkRemovedSetting(clusterState.metadata().settings(), settings, deprecatedSetting, url, "Use of this setting is unsafe.");
    }

    static DeprecationIssue checkHttpContentTypeRequiredSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Setting<Boolean> deprecatedSetting = HttpTransportSettings.SETTING_HTTP_CONTENT_TYPE_REQUIRED;
        String url = "https://ela.st/es-deprecation-7-http-content-type-required-setting";
        return checkRemovedSetting(
            clusterState.metadata().settings(),
            settings,
            deprecatedSetting,
            url,
            "This setting no longer has any effect."
        );
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
        return checkRemovedSetting(
            clusterState.metadata().settings(),
            settings,
            deprecatedSetting,
            url,
            "As of 7.9.2 basic license level features are always enabled."
        );
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
            clusterState.metadata().settings(),
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

    static DeprecationIssue checkPollIntervalTooLow(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final ClusterState clusterState,
        final XPackLicenseState licenseState
    ) {
        Settings clusterSettings = clusterState.metadata().settings();
        String pollIntervalString = clusterSettings.get(LIFECYCLE_POLL_INTERVAL_SETTING.getKey());
        if (Strings.isNullOrEmpty(pollIntervalString)) {
            return null;
        }

        TimeValue pollInterval;
        try {
            pollInterval = TimeValue.parseTimeValue(pollIntervalString, LIFECYCLE_POLL_INTERVAL_SETTING.getKey());
        } catch (IllegalArgumentException e) {
            logger.error("Failed to parse [{}] value: [{}]", LIFECYCLE_POLL_INTERVAL_SETTING.getKey(), pollIntervalString);
            return null;
        }

        if (pollInterval.compareTo(TimeValue.timeValueSeconds(1)) < 0) {
            boolean canAutoRemoveSetting = LIFECYCLE_POLL_INTERVAL_SETTING.exists(clusterSettings)
                && LIFECYCLE_POLL_INTERVAL_SETTING.exists(settings) == false;
            Map<String, Object> meta = createMetaMapForRemovableSettings(canAutoRemoveSetting, LIFECYCLE_POLL_INTERVAL_SETTING.getKey());
            return new DeprecationIssue(
                DeprecationIssue.Level.CRITICAL,
                "Index Lifecycle Management poll interval is set too low",
                "https://ela.st/es-deprecation-7-indices-lifecycle-poll-interval-setting",
                String.format(
                    Locale.ROOT,
                    "The ILM [%s] setting is set to [%s]. Set the interval to at least 1s.",
                    LIFECYCLE_POLL_INTERVAL_SETTING.getKey(),
                    pollIntervalString
                ),
                false,
                meta
            );
        }
        return null;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

class NodeDeprecationChecks {

    static DeprecationIssue checkPidfile(final Settings settings, final PluginsAndModules pluginsAndModules) {
        return checkDeprecatedSetting(
            settings,
            pluginsAndModules,
            Environment.PIDFILE_SETTING,
            Environment.NODE_PIDFILE_SETTING,
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.4/breaking-changes-7.4.html#deprecate-pidfile");
    }

    static DeprecationIssue checkProcessors(final Settings settings , final PluginsAndModules pluginsAndModules) {
        return checkDeprecatedSetting(
            settings,
            pluginsAndModules,
            EsExecutors.PROCESSORS_SETTING,
            EsExecutors.NODE_PROCESSORS_SETTING,
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.4/breaking-changes-7.4.html#deprecate-processors");
    }

    static DeprecationIssue checkMissingRealmOrders(final Settings settings, final PluginsAndModules pluginsAndModules) {
        final Set<String> orderNotConfiguredRealms = RealmSettings.getRealmSettings(settings).entrySet()
                .stream()
                .filter(e -> false == e.getValue().hasValue(RealmSettings.ORDER_SETTING_KEY))
                .map(e -> RealmSettings.realmSettingPrefix(e.getKey()) + RealmSettings.ORDER_SETTING_KEY)
                .collect(Collectors.toSet());

        if (orderNotConfiguredRealms.isEmpty()) {
            return null;
        }

        final String details = String.format(
            Locale.ROOT,
            "Found realms without order config: [%s]. In next major release, node will fail to start with missing realm order.",
            String.join("; ", orderNotConfiguredRealms));
        return new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Realm order will be required in next major release.",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.7/breaking-changes-7.7.html#deprecate-missing-realm-order",
            details
        );
    }

    static DeprecationIssue checkUniqueRealmOrders(final Settings settings, final PluginsAndModules pluginsAndModules) {
        final Map<String, List<String>> orderToRealmSettings =
            RealmSettings.getRealmSettings(settings).entrySet()
                .stream()
                .filter(e -> e.getValue().hasValue(RealmSettings.ORDER_SETTING_KEY))
                .collect(Collectors.groupingBy(
                    e -> e.getValue().get(RealmSettings.ORDER_SETTING_KEY),
                    Collectors.mapping(e -> RealmSettings.realmSettingPrefix(e.getKey()) + RealmSettings.ORDER_SETTING_KEY,
                        Collectors.toList())));

        Set<String> duplicateOrders = orderToRealmSettings.entrySet().stream()
            .filter(entry -> entry.getValue().size() > 1)
            .map(entry -> entry.getKey() + ": " + entry.getValue())
            .collect(Collectors.toSet());

        if (duplicateOrders.isEmpty()) {
            return null;
        }

        final String details = String.format(
            Locale.ROOT,
            "Found multiple realms configured with the same order: [%s]. " +
                "In next major release, node will fail to start with duplicated realm order.",
            String.join("; ", duplicateOrders));

        return new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Realm orders must be unique in next major release.",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.7/breaking-changes-7.7.html#deprecate-duplicated-realm-orders",
            details
        );
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
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.x/breaking-changes-7.7.html#deprecate-listener-thread-pool");
    }

    public static DeprecationIssue checkClusterRemoteConnectSetting(final Settings settings, final PluginsAndModules pluginsAndModules) {
        return checkDeprecatedSetting(
            settings,
            pluginsAndModules,
            RemoteClusterService.ENABLE_REMOTE_CLUSTERS,
            Setting.boolSetting(
                "node.remote_cluster_client",
                RemoteClusterService.ENABLE_REMOTE_CLUSTERS,
                Property.Deprecated,
                Property.NodeScope),
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.7/breaking-changes-7.7.html#deprecate-cluster-remote-connect"
        );
    }

    public static DeprecationIssue checkNodeLocalStorageSetting(final Settings settings, final PluginsAndModules pluginsAndModules) {
        return checkRemovedSetting(
            settings,
            Node.NODE_LOCAL_STORAGE_SETTING,
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.8/breaking-changes-7.8.html#deprecate-node-local-storage"
        );
    }

    public static DeprecationIssue checkNodeBasicLicenseFeatureEnabledSetting(final Settings settings, Setting<?> setting) {
        return checkRemovedSetting(
            settings,
            setting,
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.8/breaking-changes-7.8.html#deprecate-basic-license-feature-enabled"
        );
    }

    public static DeprecationIssue checkGeneralScriptSizeSetting(final Settings settings, final PluginsAndModules pluginsAndModules) {
        return checkDeprecatedSetting(
            settings,
            pluginsAndModules,
            ScriptService.SCRIPT_GENERAL_CACHE_SIZE_SETTING,
            ScriptService.SCRIPT_CACHE_SIZE_SETTING,
            "a script context",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.9/breaking-changes-7.9.html#deprecate_general_script_cache_size"
        );
    }

    public static DeprecationIssue checkGeneralScriptExpireSetting(final Settings settings, final PluginsAndModules pluginsAndModules) {
        return checkDeprecatedSetting(
            settings,
            pluginsAndModules,
            ScriptService.SCRIPT_GENERAL_CACHE_EXPIRE_SETTING,
            ScriptService.SCRIPT_CACHE_EXPIRE_SETTING,
            "a script context",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.9/breaking-changes-7.9.html#deprecate_general_script_expire"
        );
    }

    public static DeprecationIssue checkGeneralScriptCompileSettings(final Settings settings, final PluginsAndModules pluginsAndModules) {
        return checkDeprecatedSetting(
            settings,
            pluginsAndModules,
            ScriptService.SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING,
            ScriptService.SCRIPT_MAX_COMPILATIONS_RATE_SETTING,
            "a script context",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.9/breaking-changes-7.9.html#deprecate_general_script_compile_rate"
        );
    }

    private static DeprecationIssue checkDeprecatedSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final Setting<?> deprecatedSetting,
        final Setting<?> replacementSetting,
        final String url) {
        assert deprecatedSetting.isDeprecated() : deprecatedSetting;
        if (deprecatedSetting.exists(settings) == false) {
            return null;
        }
        final String deprecatedSettingKey = deprecatedSetting.getKey();
        final String replacementSettingKey = replacementSetting.getKey();
        final String value = deprecatedSetting.get(settings).toString();
        final String message = String.format(
            Locale.ROOT,
            "setting [%s] is deprecated in favor of setting [%s]",
            deprecatedSettingKey,
            replacementSettingKey);
        final String details = String.format(
            Locale.ROOT,
            "the setting [%s] is currently set to [%s], instead set [%s] to [%s]",
            deprecatedSettingKey,
            value,
            replacementSettingKey,
            value);
        return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, details);
    }

    private static DeprecationIssue checkDeprecatedSetting(
        final Settings settings,
        final PluginsAndModules pluginsAndModules,
        final Setting<?> deprecatedSetting,
        final Setting.AffixSetting<?> replacementSetting,
        final String star,
        final String url) {
        assert deprecatedSetting.isDeprecated() : deprecatedSetting;
        if (deprecatedSetting.exists(settings) == false) {
            return null;
        }
        final String deprecatedSettingKey = deprecatedSetting.getKey();
        final String replacementSettingKey = replacementSetting.getKey();
        final String value = deprecatedSetting.get(settings).toString();
        final String message = String.format(
            Locale.ROOT,
            "setting [%s] is deprecated in favor of grouped setting [%s]",
            deprecatedSettingKey,
            replacementSettingKey);
        final String details = String.format(
            Locale.ROOT,
            "the setting [%s] is currently set to [%s], instead set [%s] to [%s] where * is %s",
            deprecatedSettingKey,
            value,
            replacementSettingKey,
            value,
            star);
        return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, details);
    }

    static DeprecationIssue checkRemovedSetting(final Settings settings, final Setting<?> removedSetting, final String url) {
        if (removedSetting.exists(settings) == false) {
            return null;
        }
        final String removedSettingKey = removedSetting.getKey();
        final String value = removedSetting.get(settings).toString();
        final String message =
            String.format(Locale.ROOT, "setting [%s] is deprecated and will be removed in the next major version", removedSettingKey);
        final String details =
            String.format(Locale.ROOT, "the setting [%s] is currently set to [%s], remove this setting", removedSettingKey, value);
        return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, details);
    }

}

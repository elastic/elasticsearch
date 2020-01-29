/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
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
        final String orderNotConfiguredRealms = RealmSettings.getRealmSettings(settings).entrySet()
                .stream()
                .filter(e -> Objects.isNull(e.getValue().get(RealmSettings.ORDER_SETTING_KEY)))
                .map(e -> RealmSettings.realmSettingPrefix(e.getKey()) + RealmSettings.ORDER_SETTING_KEY)
                .collect(Collectors.joining("; "));

        if (false == Strings.hasText(orderNotConfiguredRealms)) {
            return null;
        }

        final String details = String.format(
            Locale.ROOT,
            "Found realms without order config: [%s]. In next major release, node will fail to start with missing realm order",
            orderNotConfiguredRealms);
        return new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Realm order will be required in next major release.",
            "",
            details
        );
    }

    static DeprecationIssue checkUniqueRealmOrders(final Settings settings, final PluginsAndModules pluginsAndModules) {
        final Map<String, List<String>> orderToRealmName =
            RealmSettings.getRealmSettings(settings).entrySet()
                .stream()
                .filter(e -> Objects.nonNull(e.getValue().get(RealmSettings.ORDER_SETTING_KEY)))
                .collect(Collectors.groupingBy(
                    e -> e.getValue().get(RealmSettings.ORDER_SETTING_KEY),
                    Collectors.mapping(e -> e.getKey().getName(), Collectors.toList())));

        String duplicateOrders = orderToRealmName.entrySet().stream()
            .filter(entry -> entry.getValue().size() > 1)
            .map(entry -> entry.getKey() + ": " + entry.getValue())
            .collect(Collectors.joining("; "));

        if (false == Strings.hasText(duplicateOrders)) {
            return null;
        }

        final String details = String.format(
            Locale.ROOT,
            "Found multiple realms configured with the same order: [%s]. " +
                "In next major release, node will fail to start with duplicated realm order",
            duplicateOrders);

        return new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Realm orders must be unique in next major release",
            "",
            details
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

}

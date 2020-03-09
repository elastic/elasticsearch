/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.List;
import java.util.Locale;
import java.util.function.BiFunction;

public class NodeDeprecationChecks {

    public static List<BiFunction<Settings, PluginsAndModules, DeprecationIssue>> NODE_SETTINGS_CHECKS;

    static {
        NODE_SETTINGS_CHECKS = List.of();
    }

    static DeprecationIssue checkRemovedSetting(final Settings settings, final Setting<?> removedSetting, final String url) {
        if (removedSetting.exists(settings) == false) {
            return null;
        }
        final String removedSettingKey = removedSetting.getKey();
        final String value = removedSetting.get(settings).toString();
        final String message =
            String.format(Locale.ROOT, "setting [%s] is deprecated and will be removed in the next major version", removedSettingKey);
        final String details = String.format("the setting [%s] is currently set to [%s], remove this setting", removedSettingKey, value);
        return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, details);
    }

}

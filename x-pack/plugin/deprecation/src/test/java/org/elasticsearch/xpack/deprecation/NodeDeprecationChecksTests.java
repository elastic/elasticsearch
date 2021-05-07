/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.bootstrap.BootstrapSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class NodeDeprecationChecksTests extends ESTestCase {

    public void testCheckBootstrapSystemCallFilterSetting() {
        final boolean boostrapSystemCallFilter = randomBoolean();
        final Settings settings =
            Settings.builder().put(BootstrapSettings.SYSTEM_CALL_FILTER_SETTING.getKey(), boostrapSystemCallFilter).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(List.of(), List.of());
        final List<DeprecationIssue> issues =
            DeprecationChecks.filterChecks(DeprecationChecks.NODE_SETTINGS_CHECKS, c -> c.apply(settings, pluginsAndModules));
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "setting [bootstrap.system_call_filter] is deprecated and will be removed in the next major version",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.13/breaking-changes-7.13.html#deprecate-system-call-filter-setting",
            "the setting [bootstrap.system_call_filter] is currently set to [" + boostrapSystemCallFilter + "], remove this setting");
        assertThat(issues, hasItem(expected));
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{BootstrapSettings.SYSTEM_CALL_FILTER_SETTING});
    }

    public void testRemovedSettingNotSet() {
        final Settings settings = Settings.EMPTY;
        final Setting<?> removedSetting = Setting.simpleString("node.removed_setting");
        final DeprecationIssue issue =
            NodeDeprecationChecks.checkRemovedSetting(settings, removedSetting, "http://removed-setting.example.com");
        assertThat(issue, nullValue());
    }

    public void testRemovedSetting() {
        final Settings settings = Settings.builder().put("node.removed_setting", "value").build();
        final Setting<?> removedSetting = Setting.simpleString("node.removed_setting");
        final DeprecationIssue issue =
            NodeDeprecationChecks.checkRemovedSetting(settings, removedSetting, "https://removed-setting.example.com");
        assertThat(issue, not(nullValue()));
        assertThat(issue.getLevel(), equalTo(DeprecationIssue.Level.CRITICAL));
        assertThat(
            issue.getMessage(),
            equalTo("setting [node.removed_setting] is deprecated and will be removed in the next major version"));
        assertThat(
            issue.getDetails(),
            equalTo("the setting [node.removed_setting] is currently set to [value], remove this setting"));
        assertThat(issue.getUrl(), equalTo("https://removed-setting.example.com"));
    }

}

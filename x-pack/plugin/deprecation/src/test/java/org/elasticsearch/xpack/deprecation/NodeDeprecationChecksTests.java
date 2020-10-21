/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class NodeDeprecationChecksTests extends ESTestCase {

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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.settings;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class ClusterSettingsTests extends ESTestCase {

    public void testWatchAfterApply() {
        Setting<String> clusterSetting = Setting.simpleString("cluster.setting", Setting.Property.NodeScope, Setting.Property.Dynamic);
        Settings nodeSettings = Settings.builder().put("cluster.setting", "initial_value").build();

        ClusterSettings clusterSettings = new ClusterSettings(nodeSettings, Set.of(clusterSetting));
        Settings newSettings = Settings.builder().put("cluster.setting", "updated_value").build();
        clusterSettings.applySettings(newSettings);

        // the value should be current when initializing the consumer
        clusterSettings.initializeAndWatch(clusterSetting, value -> { assertThat(value, equalTo("updated_value")); });
    }
}

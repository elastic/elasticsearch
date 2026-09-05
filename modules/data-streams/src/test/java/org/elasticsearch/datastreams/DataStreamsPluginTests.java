/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.ColumnarCodecClusterSettingProvider;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

public class DataStreamsPluginTests extends ESTestCase {

    public void testColumnarClusterKillSwitchRegisteredWhenFlagEnabled() {
        assumeTrue("columnar_codec feature flag must be enabled", ColumnarCodecClusterSettingProvider.isFeatureFlagEnabled());
        assertTrue(
            "cluster.columnar_codec.enabled must be registered when the columnar_codec feature flag is enabled",
            settingKeys().contains(ColumnarCodecClusterSettingProvider.COLUMNAR_CODEC_CLUSTER_ENABLED_SETTING.getKey())
        );
    }

    public void testColumnarClusterKillSwitchNotRegisteredWhenFlagDisabled() {
        assumeFalse("columnar_codec feature flag must be disabled", ColumnarCodecClusterSettingProvider.isFeatureFlagEnabled());
        assertFalse(
            "cluster.columnar_codec.enabled must not be registered when the columnar_codec feature flag is disabled",
            settingKeys().contains(ColumnarCodecClusterSettingProvider.COLUMNAR_CODEC_CLUSTER_ENABLED_SETTING.getKey())
        );
    }

    private static List<String> settingKeys() {
        return new DataStreamsPlugin(Settings.EMPTY).getSettings().stream().map(Setting::getKey).toList();
    }
}

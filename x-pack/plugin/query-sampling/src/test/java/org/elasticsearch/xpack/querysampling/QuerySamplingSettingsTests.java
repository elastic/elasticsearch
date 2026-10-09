/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class QuerySamplingSettingsTests extends ESTestCase {

    public void testDefaults() {
        assertThat(QuerySamplingSettings.ACCEPTANCE_SCALE.get(Settings.EMPTY), equalTo(1.0));
        assertThat(QuerySamplingSettings.HEAD_THRESHOLD.get(Settings.EMPTY), equalTo(100L));
        assertThat(QuerySamplingSettings.MULTIPLICITY_WINDOW.get(Settings.EMPTY), equalTo(TimeValue.timeValueHours(1)));
    }

    public void testSamplingCanBeTunedWithoutARestart() {
        for (Setting<?> setting : new Setting<?>[] {
            QuerySamplingSettings.ACCEPTANCE_SCALE,
            QuerySamplingSettings.HEAD_THRESHOLD,
            QuerySamplingSettings.MULTIPLICITY_WINDOW }) {
            assertTrue(setting.getKey() + " is dynamic", setting.isDynamic());
        }
    }

    public void testValuesOutOfRangeAreRejected() {
        expectInvalid(QuerySamplingSettings.ACCEPTANCE_SCALE, "-0.1");
        expectInvalid(QuerySamplingSettings.ACCEPTANCE_SCALE, "101");
        expectInvalid(QuerySamplingSettings.HEAD_THRESHOLD, "0");
        expectInvalid(QuerySamplingSettings.MULTIPLICITY_WINDOW, "500ms");
    }

    private static void expectInvalid(Setting<?> setting, String value) {
        expectThrows(IllegalArgumentException.class, () -> setting.get(Settings.builder().put(setting.getKey(), value).build()));
    }
}

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

    public void testTheGoldenPromotionIntervalIsZeroOrAtLeastFiveMinutes() {
        assertThat(get(QuerySamplingSettings.GOLDEN_PROMOTION_INTERVAL, "0"), equalTo(TimeValue.ZERO));
        assertThat(get(QuerySamplingSettings.GOLDEN_PROMOTION_INTERVAL, "5m"), equalTo(TimeValue.timeValueMinutes(5)));
        expectThrows(IllegalArgumentException.class, () -> get(QuerySamplingSettings.GOLDEN_PROMOTION_INTERVAL, "4m"));
    }

    public void testTheMultiplicityWindowIsAtLeastOneMinute() {
        assertThat(get(QuerySamplingSettings.MULTIPLICITY_WINDOW, "1m"), equalTo(TimeValue.timeValueMinutes(1)));
        expectThrows(IllegalArgumentException.class, () -> get(QuerySamplingSettings.MULTIPLICITY_WINDOW, "30s"));
    }

    public void testTheRetentionIsAtLeastOneHour() {
        assertThat(get(QuerySamplingSettings.RETENTION, "1h"), equalTo(TimeValue.timeValueHours(1)));
        expectThrows(IllegalArgumentException.class, () -> get(QuerySamplingSettings.RETENTION, "30m"));
    }

    public void testTheWeightsRefreshIntervalIsAtLeastTenSeconds() {
        assertThat(get(QuerySamplingSettings.WEIGHTS_REFRESH_INTERVAL, "10s"), equalTo(TimeValue.timeValueSeconds(10)));
        expectThrows(IllegalArgumentException.class, () -> get(QuerySamplingSettings.WEIGHTS_REFRESH_INTERVAL, "5s"));
    }

    public void testThereAreAtMostSixtyFourSpatialClusters() {
        assertThat(get(QuerySamplingSettings.SPATIAL_CLUSTERS, "64"), equalTo(64));
        expectThrows(IllegalArgumentException.class, () -> get(QuerySamplingSettings.SPATIAL_CLUSTERS, "65"));
    }

    private static <T> T get(Setting<T> setting, String value) {
        return setting.get(Settings.builder().put(setting.getKey(), value).build());
    }
}

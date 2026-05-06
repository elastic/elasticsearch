/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.search;

import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItems;

public class SearchResponseMetricsTests extends ESTestCase {

    public void testAllSearchPhaseHistogramsRegistered() {
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        new SearchResponseMetrics(meterRegistry);
        assertThat(
            meterRegistry.getRecorder().getRegisteredMetrics(InstrumentType.LONG_HISTOGRAM),
            hasItems(
                "es.search_response.took_durations.histogram",
                "es.search_response.took_durations.can_match.histogram",
                "es.search_response.took_durations.dfs.histogram",
                "es.search_response.took_durations.dfs_query.histogram",
                "es.search_response.took_durations.fetch.histogram",
                "es.search_response.took_durations.open_pit.histogram",
                "es.search_response.took_durations.query.histogram",
                "es.search_response.took_durations.rank_feature.histogram"
            )
        );
    }

    public void testRecordRankFeaturePhaseDuration() {
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        SearchResponseMetrics metrics = new SearchResponseMetrics(meterRegistry);

        Map<String, Object> attributes = Map.of("target", "user");
        long durationMillis = randomLongBetween(1, 1000);
        metrics.recordSearchPhaseDuration("rank-feature", TimeUnit.MILLISECONDS.toNanos(durationMillis), attributes);

        List<Measurement> measurements = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_HISTOGRAM, "es.search_response.took_durations.rank_feature.histogram");
        assertThat(measurements, contains(new Measurement(durationMillis, attributes, false)));
    }
}

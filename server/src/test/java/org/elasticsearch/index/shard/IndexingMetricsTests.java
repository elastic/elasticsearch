/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.metric.MetricAttributes;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class IndexingMetricsTests extends ESTestCase {

    public void testCompletedItemsAndFailuresFeedSeparateCounters() {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        IndexingMetrics metrics = new IndexingMetrics(registry);
        IndexMode indexMode = randomFrom(IndexMode.values());
        int count = randomIntBetween(1, 10_000);
        Exception error = randomFrom(new IOException(randomAlphaOfLength(5)), new IllegalStateException(randomAlphaOfLength(5)));

        metrics.onBulkItemsCompleted(indexMode, count);
        metrics.onBulkItemFailed(indexMode, error);

        Measurement operations = single(registry, InstrumentType.LONG_COUNTER, IndexingMetrics.INDEXING_OPERATIONS_TOTAL);
        assertThat(operations.getLong(), equalTo((long) count));
        assertThat(operations.attributes(), equalTo(Map.of(MetricAttributes.ES_INDEX_MODE, indexMode.getName())));
        Measurement failure = single(registry, InstrumentType.LONG_COUNTER, IndexingMetrics.INDEXING_FAILURE_TOTAL);
        assertThat(failure.getLong(), equalTo(1L));
        assertThat(
            failure.attributes(),
            equalTo(
                Map.of(MetricAttributes.ES_INDEX_MODE, indexMode.getName(), MetricAttributes.ERROR_TYPE, error.getClass().getSimpleName())
            )
        );
    }

    private static Measurement single(RecordingMeterRegistry registry, InstrumentType type, String name) {
        List<Measurement> measurements = registry.getRecorder().getMeasurements(type, name);
        assertThat(name, measurements, hasSize(1));
        return measurements.get(0);
    }
}

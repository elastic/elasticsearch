/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.reindex.ReindexMetrics.ATTRIBUTE_NAME_ERROR_TYPE;
import static org.elasticsearch.reindex.ReindexMetrics.ATTRIBUTE_NAME_SOURCE;
import static org.elasticsearch.reindex.ReindexMetrics.ATTRIBUTE_VALUE_SOURCE_LOCAL;
import static org.elasticsearch.reindex.ReindexMetrics.ATTRIBUTE_VALUE_SOURCE_REMOTE;
import static org.elasticsearch.reindex.ReindexMetrics.REINDEX_COMPLETION_COUNTER;
import static org.elasticsearch.reindex.ReindexMetrics.REINDEX_TIME_HISTOGRAM;

public class ReindexMetricsTests extends ESTestCase {

    private RecordingMeterRegistry registry;
    private ReindexMetrics metrics;

    @Before
    public void createMetrics() {
        registry = new RecordingMeterRegistry();
        metrics = new ReindexMetrics(registry);
    }

    public void testRecordTookTime() {
        long secondsTaken = randomLongBetween(1, Long.MAX_VALUE);

        // first metric
        metrics.recordTookTime(secondsTaken, false);

        List<Measurement> measurements = registry.getRecorder().getMeasurements(InstrumentType.LONG_HISTOGRAM, REINDEX_TIME_HISTOGRAM);
        assertEquals(1, measurements.size());
        assertEquals(secondsTaken, measurements.getFirst().getLong());
        assertEquals(ATTRIBUTE_VALUE_SOURCE_LOCAL, measurements.getFirst().attributes().get(ATTRIBUTE_NAME_SOURCE));

        // second metric
        long remoteSecondsTaken = randomLongBetween(1, Long.MAX_VALUE);
        metrics.recordTookTime(remoteSecondsTaken, true);

        measurements = registry.getRecorder().getMeasurements(InstrumentType.LONG_HISTOGRAM, REINDEX_TIME_HISTOGRAM);
        assertEquals(2, measurements.size());
        assertEquals(secondsTaken, measurements.getFirst().getLong());
        assertEquals(ATTRIBUTE_VALUE_SOURCE_LOCAL, measurements.getFirst().attributes().get(ATTRIBUTE_NAME_SOURCE));
        assertEquals(remoteSecondsTaken, measurements.get(1).getLong());
        assertEquals(ATTRIBUTE_VALUE_SOURCE_REMOTE, measurements.get(1).attributes().get(ATTRIBUTE_NAME_SOURCE));
    }

    public void testRecordSuccess() {
        // first metric
        metrics.recordSuccess(false);

        List<Measurement> measurements = registry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, REINDEX_COMPLETION_COUNTER);
        assertEquals(1, measurements.size());
        assertEquals(1, measurements.getFirst().getLong());
        assertEquals(ATTRIBUTE_VALUE_SOURCE_LOCAL, measurements.getFirst().attributes().get(ATTRIBUTE_NAME_SOURCE));
        assertNull(measurements.getFirst().attributes().get(ATTRIBUTE_NAME_ERROR_TYPE));

        // second metric
        metrics.recordSuccess(true);

        measurements = registry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, REINDEX_COMPLETION_COUNTER);
        assertEquals(2, measurements.size());
        assertEquals(1, measurements.get(1).getLong());
        assertEquals(ATTRIBUTE_VALUE_SOURCE_REMOTE, measurements.get(1).attributes().get(ATTRIBUTE_NAME_SOURCE));
        assertNull(measurements.get(1).attributes().get(ATTRIBUTE_NAME_ERROR_TYPE));
    }

    public void testRecordFailure() {
        // first metric
        metrics.recordFailure(false, new IllegalArgumentException("random failure"));

        List<Measurement> measurements = registry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, REINDEX_COMPLETION_COUNTER);
        assertEquals(1, measurements.size());
        assertEquals(1, measurements.getFirst().getLong());
        assertEquals("java.lang.IllegalArgumentException", measurements.getFirst().attributes().get(ATTRIBUTE_NAME_ERROR_TYPE));
        assertEquals(ATTRIBUTE_VALUE_SOURCE_LOCAL, measurements.getFirst().attributes().get(ATTRIBUTE_NAME_SOURCE));

        // second metric
        metrics.recordFailure(true, new ElasticsearchStatusException("another failure", RestStatus.BAD_REQUEST));

        measurements = registry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, REINDEX_COMPLETION_COUNTER);
        assertEquals(2, measurements.size());
        assertEquals(1, measurements.getFirst().getLong());
        assertEquals(1, measurements.get(1).getLong());
        assertEquals(RestStatus.BAD_REQUEST.name(), measurements.get(1).attributes().get(ATTRIBUTE_NAME_ERROR_TYPE));
        assertEquals(ATTRIBUTE_VALUE_SOURCE_REMOTE, measurements.get(1).attributes().get(ATTRIBUTE_NAME_SOURCE));
    }
}

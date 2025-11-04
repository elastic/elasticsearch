/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.reindex.ReindexMetrics.REINDEX_FAILURE_HISTOGRAM;
import static org.elasticsearch.reindex.ReindexMetrics.REINDEX_FAILURE_HISTOGRAM_REMOTE;
import static org.elasticsearch.reindex.ReindexMetrics.REINDEX_SUCCESS_HISTOGRAM;
import static org.elasticsearch.reindex.ReindexMetrics.REINDEX_SUCCESS_HISTOGRAM_REMOTE;
import static org.elasticsearch.reindex.ReindexMetrics.REINDEX_TIME_HISTOGRAM;
import static org.elasticsearch.reindex.ReindexMetrics.REINDEX_TIME_HISTOGRAM_REMOTE;

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

        // second metric
        long remoteSecondsTaken = randomLongBetween(1, Long.MAX_VALUE);
        metrics.recordTookTime(remoteSecondsTaken, true);

        measurements = registry.getRecorder().getMeasurements(InstrumentType.LONG_HISTOGRAM, REINDEX_TIME_HISTOGRAM);
        assertEquals(2, measurements.size());
        assertEquals(secondsTaken, measurements.getFirst().getLong());
        assertEquals(remoteSecondsTaken, measurements.getLast().getLong());
        List<Measurement> measurementsRemote = registry.getRecorder()
            .getMeasurements(InstrumentType.LONG_HISTOGRAM, REINDEX_TIME_HISTOGRAM_REMOTE);
        assertEquals(1, measurementsRemote.size());
        assertEquals(remoteSecondsTaken, measurementsRemote.getFirst().getLong());
    }

    public void testRecordSuccess() {
        // first metric
        metrics.recordSuccess(false);

        List<Measurement> measurements = registry.getRecorder().getMeasurements(InstrumentType.LONG_HISTOGRAM, REINDEX_SUCCESS_HISTOGRAM);
        assertEquals(1, measurements.size());
        assertEquals(1, measurements.getFirst().getLong());

        // second metric
        metrics.recordSuccess(true);

        measurements = registry.getRecorder().getMeasurements(InstrumentType.LONG_HISTOGRAM, REINDEX_SUCCESS_HISTOGRAM);
        assertEquals(2, measurements.size());
        assertEquals(1, measurements.getFirst().getLong());
        assertEquals(1, measurements.getLast().getLong());
        List<Measurement> measurementsRemote = registry.getRecorder()
            .getMeasurements(InstrumentType.LONG_HISTOGRAM, REINDEX_SUCCESS_HISTOGRAM_REMOTE);
        assertEquals(1, measurementsRemote.size());
        assertEquals(1, measurements.getFirst().getLong());
    }

    public void testRecordFailure() {
        // first metric
        metrics.recordFailure(false);

        List<Measurement> measurements = registry.getRecorder().getMeasurements(InstrumentType.LONG_HISTOGRAM, REINDEX_FAILURE_HISTOGRAM);
        assertEquals(1, measurements.size());
        assertEquals(1, measurements.getFirst().getLong());

        // second metric
        metrics.recordFailure(true);

        measurements = registry.getRecorder().getMeasurements(InstrumentType.LONG_HISTOGRAM, REINDEX_FAILURE_HISTOGRAM);
        assertEquals(2, measurements.size());
        assertEquals(1, measurements.getFirst().getLong());
        assertEquals(1, measurements.getLast().getLong());
        List<Measurement> measurementsRemote = registry.getRecorder()
            .getMeasurements(InstrumentType.LONG_HISTOGRAM, REINDEX_FAILURE_HISTOGRAM_REMOTE);
        assertEquals(1, measurementsRemote.size());
        assertEquals(1, measurements.getFirst().getLong());
    }
}

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

import static org.elasticsearch.reindex.ReindexMetrics.REINDEX_TIME_HISTOGRAM;

public class ReindexMetricsTests extends ESTestCase {

    private RecordingMeterRegistry recordingMeterRegistry;
    private ReindexMetrics metrics;

    @Before
    public void createMetrics() {
        recordingMeterRegistry = new RecordingMeterRegistry();
        metrics = new ReindexMetrics(recordingMeterRegistry);
    }

    public void testRecordTookTime() {
        int secondsTaken = randomIntBetween(1, 50);
        metrics.recordTookTime(secondsTaken);
        List<Measurement> measurements = recordingMeterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_HISTOGRAM, REINDEX_TIME_HISTOGRAM);
        assertEquals(measurements.size(), 1);
        assertEquals(measurements.get(0).getLong(), secondsTaken);
    }
}

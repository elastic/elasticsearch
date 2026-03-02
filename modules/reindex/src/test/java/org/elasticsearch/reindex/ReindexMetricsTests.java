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
import org.elasticsearch.index.reindex.AbstractBulkByScrollRequest;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.reindex.ReindexMetrics.ATTRIBUTE_NAME_ERROR_TYPE;
import static org.elasticsearch.reindex.ReindexMetrics.ATTRIBUTE_NAME_SLICING_MODE;
import static org.elasticsearch.reindex.ReindexMetrics.ATTRIBUTE_NAME_SOURCE;
import static org.elasticsearch.reindex.ReindexMetrics.ATTRIBUTE_VALUE_SOURCE_LOCAL;
import static org.elasticsearch.reindex.ReindexMetrics.ATTRIBUTE_VALUE_SOURCE_REMOTE;
import static org.elasticsearch.reindex.ReindexMetrics.REINDEX_COMPLETION_COUNTER;
import static org.elasticsearch.reindex.ReindexMetrics.REINDEX_TIME_HISTOGRAM;
import static org.elasticsearch.reindex.ReindexMetrics.SlicingMode;

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

        metrics.recordTookTime(secondsTaken, false, SlicingMode.NONE);

        List<Measurement> measurements = registry.getRecorder().getMeasurements(InstrumentType.LONG_HISTOGRAM, REINDEX_TIME_HISTOGRAM);
        assertEquals(1, measurements.size());
        assertEquals(secondsTaken, measurements.getFirst().getLong());
        assertEquals(ATTRIBUTE_VALUE_SOURCE_LOCAL, measurements.getFirst().attributes().get(ATTRIBUTE_NAME_SOURCE));
        assertEquals(SlicingMode.NONE.value(), measurements.getFirst().attributes().get(ATTRIBUTE_NAME_SLICING_MODE));

        long remoteSecondsTaken = randomLongBetween(1, Long.MAX_VALUE);
        metrics.recordTookTime(remoteSecondsTaken, true, SlicingMode.AUTO);

        measurements = registry.getRecorder().getMeasurements(InstrumentType.LONG_HISTOGRAM, REINDEX_TIME_HISTOGRAM);
        assertEquals(2, measurements.size());
        assertEquals(secondsTaken, measurements.getFirst().getLong());
        assertEquals(ATTRIBUTE_VALUE_SOURCE_LOCAL, measurements.getFirst().attributes().get(ATTRIBUTE_NAME_SOURCE));
        assertEquals(remoteSecondsTaken, measurements.get(1).getLong());
        assertEquals(ATTRIBUTE_VALUE_SOURCE_REMOTE, measurements.get(1).attributes().get(ATTRIBUTE_NAME_SOURCE));
        assertEquals(SlicingMode.AUTO.value(), measurements.get(1).attributes().get(ATTRIBUTE_NAME_SLICING_MODE));
    }

    public void testRecordSuccess() {
        metrics.recordSuccess(false, SlicingMode.NONE);

        List<Measurement> measurements = registry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, REINDEX_COMPLETION_COUNTER);
        assertEquals(1, measurements.size());
        assertEquals(1, measurements.getFirst().getLong());
        assertEquals(ATTRIBUTE_VALUE_SOURCE_LOCAL, measurements.getFirst().attributes().get(ATTRIBUTE_NAME_SOURCE));
        assertEquals(SlicingMode.NONE.value(), measurements.getFirst().attributes().get(ATTRIBUTE_NAME_SLICING_MODE));
        assertNull(measurements.getFirst().attributes().get(ATTRIBUTE_NAME_ERROR_TYPE));

        metrics.recordSuccess(true, SlicingMode.FIXED);

        measurements = registry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, REINDEX_COMPLETION_COUNTER);
        assertEquals(2, measurements.size());
        assertEquals(1, measurements.get(1).getLong());
        assertEquals(ATTRIBUTE_VALUE_SOURCE_REMOTE, measurements.get(1).attributes().get(ATTRIBUTE_NAME_SOURCE));
        assertEquals(SlicingMode.FIXED.value(), measurements.get(1).attributes().get(ATTRIBUTE_NAME_SLICING_MODE));
        assertNull(measurements.get(1).attributes().get(ATTRIBUTE_NAME_ERROR_TYPE));
    }

    public void testRecordFailure() {
        metrics.recordFailure(false, SlicingMode.NONE, new IllegalArgumentException("random failure"));

        List<Measurement> measurements = registry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, REINDEX_COMPLETION_COUNTER);
        assertEquals(1, measurements.size());
        assertEquals(1, measurements.getFirst().getLong());
        assertEquals("java.lang.IllegalArgumentException", measurements.getFirst().attributes().get(ATTRIBUTE_NAME_ERROR_TYPE));
        assertEquals(ATTRIBUTE_VALUE_SOURCE_LOCAL, measurements.getFirst().attributes().get(ATTRIBUTE_NAME_SOURCE));
        assertEquals(SlicingMode.NONE.value(), measurements.getFirst().attributes().get(ATTRIBUTE_NAME_SLICING_MODE));

        metrics.recordFailure(true, SlicingMode.AUTO, new ElasticsearchStatusException("another failure", RestStatus.BAD_REQUEST));

        measurements = registry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, REINDEX_COMPLETION_COUNTER);
        assertEquals(2, measurements.size());
        assertEquals(1, measurements.getFirst().getLong());
        assertEquals(1, measurements.get(1).getLong());
        assertEquals(RestStatus.BAD_REQUEST.name(), measurements.get(1).attributes().get(ATTRIBUTE_NAME_ERROR_TYPE));
        assertEquals(ATTRIBUTE_VALUE_SOURCE_REMOTE, measurements.get(1).attributes().get(ATTRIBUTE_NAME_SOURCE));
        assertEquals(SlicingMode.AUTO.value(), measurements.get(1).attributes().get(ATTRIBUTE_NAME_SLICING_MODE));
    }

    public void testResolveSlicingModeNone() {
        ReindexRequest request = new ReindexRequest();
        assertEquals(SlicingMode.NONE, ReindexMetrics.resolveSlicingMode(request));
    }

    public void testResolveSlicingModeNoneOneSlice() {
        ReindexRequest request = new ReindexRequest();
        request.setSlices(1);
        assertEquals(SlicingMode.NONE, ReindexMetrics.resolveSlicingMode(request));
    }

    public void testResolveSlicingModeFixed() {
        ReindexRequest request = new ReindexRequest();
        request.setSlices(randomIntBetween(2, 20));
        assertEquals(SlicingMode.FIXED, ReindexMetrics.resolveSlicingMode(request));
    }

    public void testResolveSlicingModeAuto() {
        ReindexRequest request = new ReindexRequest();
        request.setSlices(AbstractBulkByScrollRequest.AUTO_SLICES);
        assertEquals(SlicingMode.AUTO, ReindexMetrics.resolveSlicingMode(request));
    }

    public void testResolveSlicingModeManual() {
        ReindexRequest request = new ReindexRequest();
        request.getSearchRequest().source(new SearchSourceBuilder().slice(new SliceBuilder(0, 3)));
        assertEquals(SlicingMode.MANUAL, ReindexMetrics.resolveSlicingMode(request));
    }
}

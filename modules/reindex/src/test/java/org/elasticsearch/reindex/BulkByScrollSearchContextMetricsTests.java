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

import static org.elasticsearch.reindex.BulkByScrollSearchContextMetrics.ATTRIBUTE_NAME_SEARCH_SOURCE;
import static org.elasticsearch.reindex.BulkByScrollSearchContextMetrics.ATTRIBUTE_NAME_TASK_KIND;
import static org.elasticsearch.reindex.BulkByScrollSearchContextMetrics.ATTRIBUTE_VALUE_SEARCH_SOURCE_LOCAL;
import static org.elasticsearch.reindex.BulkByScrollSearchContextMetrics.ATTRIBUTE_VALUE_SEARCH_SOURCE_REMOTE;
import static org.elasticsearch.reindex.BulkByScrollSearchContextMetrics.SEARCH_CONTEXT_KEEPALIVE_EXPIRED_COUNTER;

/**
 * Tests that {@link BulkByScrollSearchContextMetrics#recordKeepaliveExpiry} emits the search-context keep-alive expiry
 * counter once per call with the expected {@code task_kind} and {@code search_source} attributes.
 */
public class BulkByScrollSearchContextMetricsTests extends ESTestCase {

    private RecordingMeterRegistry registry;
    private BulkByScrollSearchContextMetrics metrics;

    @Before
    public void createMetrics() {
        registry = new RecordingMeterRegistry();
        metrics = new BulkByScrollSearchContextMetrics(registry);
    }

    /** Test a local reindex (Update-By-Query and Delete-By-Query do not share a local / remote distinction) */
    public void testRecordKeepaliveExpiryLocalReindex() {
        metrics.recordKeepaliveExpiry(BulkByScrollSearchContextMetrics.TaskKind.REINDEX, false);

        List<Measurement> measurements = registry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, SEARCH_CONTEXT_KEEPALIVE_EXPIRED_COUNTER);
        assertEquals(1, measurements.size());
        assertEquals(1, measurements.getFirst().getLong());
        assertEquals(
            BulkByScrollSearchContextMetrics.TaskKind.REINDEX.attributeValue(),
            measurements.getFirst().attributes().get(ATTRIBUTE_NAME_TASK_KIND)
        );
        assertEquals(ATTRIBUTE_VALUE_SEARCH_SOURCE_LOCAL, measurements.getFirst().attributes().get(ATTRIBUTE_NAME_SEARCH_SOURCE));
    }

    /** Test a remote reindex (Update-By-Query and Delete-By-Query do not share a local / remote distinction) */
    public void testRecordKeepaliveExpiryRemoteReindex() {
        metrics.recordKeepaliveExpiry(BulkByScrollSearchContextMetrics.TaskKind.REINDEX, true);

        List<Measurement> measurements = registry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, SEARCH_CONTEXT_KEEPALIVE_EXPIRED_COUNTER);
        assertEquals(1, measurements.size());
        assertEquals(1, measurements.getFirst().getLong());
        assertEquals(
            BulkByScrollSearchContextMetrics.TaskKind.REINDEX.attributeValue(),
            measurements.getFirst().attributes().get(ATTRIBUTE_NAME_TASK_KIND)
        );
        assertEquals(ATTRIBUTE_VALUE_SEARCH_SOURCE_REMOTE, measurements.getFirst().attributes().get(ATTRIBUTE_NAME_SEARCH_SOURCE));
    }

    /** Separate task kinds produce separate measurements, each tagged as local search. */
    public void testRecordKeepaliveExpiryUpdateByQueryAndDeleteByQuery() {
        metrics.recordKeepaliveExpiry(BulkByScrollSearchContextMetrics.TaskKind.UPDATE_BY_QUERY, false);
        metrics.recordKeepaliveExpiry(BulkByScrollSearchContextMetrics.TaskKind.DELETE_BY_QUERY, false);

        List<Measurement> measurements = registry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, SEARCH_CONTEXT_KEEPALIVE_EXPIRED_COUNTER);
        assertEquals(2, measurements.size());
        assertEquals(
            BulkByScrollSearchContextMetrics.TaskKind.UPDATE_BY_QUERY.attributeValue(),
            measurements.get(0).attributes().get(ATTRIBUTE_NAME_TASK_KIND)
        );
        assertEquals(
            BulkByScrollSearchContextMetrics.TaskKind.DELETE_BY_QUERY.attributeValue(),
            measurements.get(1).attributes().get(ATTRIBUTE_NAME_TASK_KIND)
        );
        assertEquals(ATTRIBUTE_VALUE_SEARCH_SOURCE_LOCAL, measurements.get(0).attributes().get(ATTRIBUTE_NAME_SEARCH_SOURCE));
        assertEquals(ATTRIBUTE_VALUE_SEARCH_SOURCE_LOCAL, measurements.get(1).attributes().get(ATTRIBUTE_NAME_SEARCH_SOURCE));
    }
}

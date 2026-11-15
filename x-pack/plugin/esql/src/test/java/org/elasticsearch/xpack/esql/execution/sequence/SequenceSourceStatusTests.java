/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

/** Verifies the bounded aggregate status survives profiling transport and is omitted for unsupported peers. */
public class SequenceSourceStatusTests extends AbstractWireSerializingTestCase<SequenceSourceStatus> {
    @Override
    protected SequenceSourceStatus createTestInstance() {
        return status(randomValues(12), randomBoolean() ? counters(randomValues(9)) : null);
    }

    @Override
    protected SequenceSourceStatus mutateInstance(SequenceSourceStatus instance) {
        long[] values = baseValues(instance);
        var latest = instance.latestValidCounters();
        int field = between(0, 21);
        if (field < 12) {
            values[field] = randomValueOtherThan(values[field], ESTestCase::randomNonNegativeLong);
        } else if (field == 12 || latest == null) {
            latest = latest == null ? counters(randomValues(9)) : null;
        } else {
            long[] latestValues = latestValues(latest);
            int index = field - 13;
            latestValues[index] = randomValueOtherThan(latestValues[index], ESTestCase::randomNonNegativeLong);
            latest = counters(latestValues);
        }
        return status(values, latest);
    }

    @Override
    protected Writeable.Reader<SequenceSourceStatus> instanceReader() {
        return SequenceSourceStatus::readFrom;
    }

    public void testToXContent() {
        var status = new SequenceSourceStatus(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
        assertNull(status.latestValidCounters());
        assertEquals("""
            {"candidate_search_requests":1,"payload_search_requests":2,"candidate_rows_fetched":3,
            "payload_rows_fetched":4,"completed_matches":5,"rows_emitted":6,"stage_drivers":7,
            "stage_driver_cpu_nanos":8,"stage_driver_iterations":9,"candidate_search_took_millis":10,
            "payload_search_took_millis":11,"max_pending_candidates":12}""".replace("\n", ""), Strings.toString(status));
        String human = Strings.toString(status, false, true);
        assertThat(human, containsString("\"stage_driver_cpu_time\":\"8nanos\""));
        assertThat(human, containsString("\"candidate_search_took\":\"10ms\""));
        assertThat(human, containsString("\"payload_search_took\":\"11ms\""));
        assertEquals(7L, status.documentsFound());
        assertEquals(6L, status.rowsEmitted());
    }

    public void testLatestValidZeroCountersArePresentInProfile() {
        var status = new SequenceSourceStatus(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, counters(new long[9]));
        assertEquals("""
            {"candidate_search_requests":1,"payload_search_requests":2,"candidate_rows_fetched":3,
            "payload_rows_fetched":4,"completed_matches":5,"rows_emitted":6,"stage_drivers":7,
            "stage_driver_cpu_nanos":8,"stage_driver_iterations":9,"candidate_search_took_millis":10,
            "payload_search_took_millis":11,"max_pending_candidates":12,"candidates_dropped_at_bound":0,
            "searches_abandoned_at_work_limit":0,"keys_that_hit_bound":0,"search_steps":0,
            "max_search_steps_per_final_row":0,"condition_evaluations":0,"condition_false_or_null":0,
            "largest_ring_fill":0,"candidates_used_without_report":0}""".replace("\n", ""), Strings.toString(status));
        assertEquals(7L, status.documentsFound());
        String human = Strings.toString(status, false, true);
        assertThat(human, containsString("\"stage_driver_cpu_time\":\"8nanos\""));
        assertThat(human, containsString("\"candidate_search_took\":\"10ms\""));
        assertThat(human, containsString("\"payload_search_took\":\"11ms\""));
    }

    public void testEachLatestValidCounterAffectsEquality() {
        long[] base = randomValues(12);
        long[] latest = randomValues(9);
        var original = status(base, counters(latest));
        assertNotEquals(original, status(base, null));
        for (int i = 0; i < latest.length; i++) {
            long[] changed = latest.clone();
            changed[i] = randomValueOtherThan(changed[i], ESTestCase::randomNonNegativeLong);
            assertNotEquals(original, status(base, counters(changed)));
        }
    }

    public void testLatestValidProfileValues() {
        var status = new SequenceSourceStatus(
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            counters(new long[] { 13, 14, 15, 16, 17, 18, 19, 20, 21 })
        );
        assertEquals("""
            {"candidate_search_requests":1,"payload_search_requests":2,"candidate_rows_fetched":3,
            "payload_rows_fetched":4,"completed_matches":5,"rows_emitted":6,"stage_drivers":7,
            "stage_driver_cpu_nanos":8,"stage_driver_iterations":9,"candidate_search_took_millis":10,
            "payload_search_took_millis":11,"max_pending_candidates":12,"candidates_dropped_at_bound":13,
            "searches_abandoned_at_work_limit":14,"keys_that_hit_bound":15,"search_steps":16,
            "max_search_steps_per_final_row":17,"condition_evaluations":18,"condition_false_or_null":19,
            "largest_ring_fill":20,"candidates_used_without_report":21}""".replace("\n", ""), Strings.toString(status));
    }

    public void testWireLayoutAcrossVersions() throws IOException {
        long[] base = randomValues(12);
        long[] latest = randomValues(9);
        var present = status(base, counters(latest));
        var absent = status(base, null);
        for (TransportVersion version : new TransportVersion[] {
            Sequence.ESQL_SEQUENCE,
            SequenceSourceStatus.ESQL_SEQUENCE_LATEST_VALID_STATUS,
            TransportVersion.current() }) {
            boolean supportsLatest = version.supports(SequenceSourceStatus.ESQL_SEQUENCE_LATEST_VALID_STATUS);
            for (var original : new SequenceSourceStatus[] { absent, present }) {
                // Inspect the wire directly to catch ordering, omission and unexpected trailing bytes.
                try (var out = new BytesStreamOutput()) {
                    out.setTransportVersion(version);
                    original.writeTo(out);
                    try (var in = out.bytes().streamInput()) {
                        in.setTransportVersion(version);
                        for (long value : base) {
                            assertEquals(value, in.readVLong());
                        }
                        if (supportsLatest) {
                            assertEquals(original.latestValidCounters() != null, in.readBoolean());
                            if (original.latestValidCounters() != null) {
                                for (long value : latest) {
                                    assertEquals(value, in.readVLong());
                                }
                            }
                        }
                        assertEquals(0, in.available());
                    }
                }
                // Construct the expected layout independently of the status writer.
                try (var out = new BytesStreamOutput()) {
                    out.setTransportVersion(version);
                    for (long value : base) {
                        out.writeVLong(value);
                    }
                    if (supportsLatest) {
                        out.writeBoolean(original.latestValidCounters() != null);
                        if (original.latestValidCounters() != null) {
                            for (long value : latest) {
                                out.writeVLong(value);
                            }
                        }
                    }
                    try (var in = out.bytes().streamInput()) {
                        in.setTransportVersion(version);
                        assertEquals(supportsLatest ? original : absent, SequenceSourceStatus.readFrom(in));
                        assertEquals(0, in.available());
                    }
                }
            }
        }
        assertFalse(Sequence.ESQL_SEQUENCE.supports(SequenceSourceStatus.ESQL_SEQUENCE_LATEST_VALID_STATUS));
    }

    public void testNamedWriteableRegisteredByPlugin() throws IOException {
        var registry = new NamedWriteableRegistry(new EsqlPlugin().getNamedWriteables());
        var original = status(randomValues(12), counters(randomValues(9)));
        for (TransportVersion version : new TransportVersion[] { Sequence.ESQL_SEQUENCE, TransportVersion.current() }) {
            try (var out = new BytesStreamOutput()) {
                out.setTransportVersion(version);
                out.writeNamedWriteable(original);
                try (var in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry)) {
                    in.setTransportVersion(version);
                    var expected = version.supports(SequenceSourceStatus.ESQL_SEQUENCE_LATEST_VALID_STATUS)
                        ? original
                        : status(baseValues(original), null);
                    assertEquals(expected, in.readNamedWriteable(Operator.Status.class));
                    assertEquals(0, in.available());
                }
            }
        }
    }

    public void testRejectUnsupportedPeerBeforeWritingOrReading() throws IOException {
        var unsupported = TransportVersionUtils.getPreviousVersion(Sequence.ESQL_SEQUENCE);
        try (var out = new BytesStreamOutput()) {
            out.setTransportVersion(unsupported);
            assertThat(
                expectThrows(IllegalArgumentException.class, () -> createTestInstance().writeTo(out)).getMessage(),
                containsString("SEQUENCE is not supported")
            );
            assertEquals(0, out.size());
            try (var in = out.bytes().streamInput()) {
                in.setTransportVersion(unsupported);
                assertThat(
                    expectThrows(IllegalArgumentException.class, () -> SequenceSourceStatus.readFrom(in)).getMessage(),
                    containsString("SEQUENCE is not supported")
                );
            }
        }
    }

    public void testOperatorStatusOmitsOnlyUnsupportedSnapshot() throws IOException {
        var registry = new NamedWriteableRegistry(new EsqlPlugin().getNamedWriteables());
        var snapshot = status(randomValues(12), counters(randomValues(9)));
        var original = new OperatorStatus("SequenceSource[stages=2]", snapshot);
        for (TransportVersion version : new TransportVersion[] {
            TransportVersionUtils.getPreviousVersion(Sequence.ESQL_SEQUENCE),
            Sequence.ESQL_SEQUENCE,
            TransportVersion.current() }) {
            try (var out = new BytesStreamOutput()) {
                out.setTransportVersion(version);
                original.writeTo(out);
                try (var in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry)) {
                    in.setTransportVersion(version);
                    var restored = OperatorStatus.readFrom(in);
                    assertEquals(original.operator(), restored.operator());
                    var expected = version.supports(Sequence.ESQL_SEQUENCE) == false ? null
                        : version.supports(SequenceSourceStatus.ESQL_SEQUENCE_LATEST_VALID_STATUS) ? snapshot
                        : status(baseValues(snapshot), null);
                    assertEquals(expected, restored.status());
                    assertEquals(0, in.available());
                }
            }
        }
    }

    private static long[] randomValues(int count) {
        long[] values = new long[count];
        for (int i = 0; i < count; i++) {
            values[i] = randomNonNegativeLong();
        }
        return values;
    }

    private static long[] baseValues(SequenceSourceStatus status) {
        return new long[] {
            status.candidateSearchRequests(),
            status.payloadSearchRequests(),
            status.candidateRowsFetched(),
            status.payloadRowsFetched(),
            status.completedMatches(),
            status.rowsEmitted(),
            status.stageDrivers(),
            status.stageDriverCpuNanos(),
            status.stageDriverIterations(),
            status.candidateSearchTookMillis(),
            status.payloadSearchTookMillis(),
            status.maxPendingCandidates() };
    }

    private static long[] latestValues(SequenceSourceStatus.LatestValidCounters counters) {
        return new long[] {
            counters.candidatesDroppedAtBound(),
            counters.searchesAbandonedAtWorkLimit(),
            counters.keysThatHitBound(),
            counters.searchSteps(),
            counters.maxSearchStepsPerFinalRow(),
            counters.conditionEvaluations(),
            counters.conditionFalseOrNull(),
            counters.largestRingFill(),
            counters.candidatesUsedWithoutReport() };
    }

    private static SequenceSourceStatus.LatestValidCounters counters(long[] values) {
        return new SequenceSourceStatus.LatestValidCounters(
            values[0],
            values[1],
            values[2],
            values[3],
            values[4],
            values[5],
            values[6],
            values[7],
            values[8]
        );
    }

    private static SequenceSourceStatus status(long[] values, SequenceSourceStatus.LatestValidCounters latest) {
        return new SequenceSourceStatus(
            values[0],
            values[1],
            values[2],
            values[3],
            values[4],
            values[5],
            values[6],
            values[7],
            values[8],
            values[9],
            values[10],
            values[11],
            latest
        );
    }
}

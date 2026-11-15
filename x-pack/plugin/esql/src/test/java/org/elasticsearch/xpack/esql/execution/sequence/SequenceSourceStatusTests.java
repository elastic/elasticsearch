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
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

/** Seven bounded-work counters are mandatory in every supported source snapshot. */
public class SequenceSourceStatusTests extends AbstractWireSerializingTestCase<SequenceSourceStatus> {
    @Override
    protected SequenceSourceStatus createTestInstance() {
        return status(randomValues(12), counters(randomValues(7)), randomBoolean() ? runtime() : null);
    }

    @Override
    protected SequenceSourceStatus mutateInstance(SequenceSourceStatus instance) {
        long[] base = baseValues(instance);
        long[] work = workValues(instance.latestValidCounters());
        int field = between(0, 19);
        if (field == 19) {
            return status(base, counters(work), instance.runtime() == null ? runtime() : null);
        }
        long[] values = field < 12 ? base : work;
        int index = field < 12 ? field : field - 12;
        values[index] = randomValueOtherThan(values[index], ESTestCase::randomNonNegativeLong);
        return status(base, counters(work), instance.runtime());
    }

    @Override
    protected Writeable.Reader<SequenceSourceStatus> instanceReader() {
        return SequenceSourceStatus::readFrom;
    }

    public void testEveryZeroCounterIsPresentInProfile() {
        var status = status(new long[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 }, counters(new long[7]));
        assertEquals(
            """
                {"candidate_search_requests":1,"payload_search_requests":2,"candidate_rows_fetched":3,
                "payload_rows_fetched":4,"completed_matches":5,"rows_emitted":6,"stage_drivers":7,
                "stage_driver_cpu_nanos":8,"stage_driver_iterations":9,"candidate_search_took_millis":10,
                "payload_search_took_millis":11,"max_pending_candidates":12,"candidates_dropped_at_bound":0,
                "searches_abandoned_at_work_limit":0,"keys_that_hit_bound":0,"search_steps":0,
                "max_search_steps_per_final_row":0,"condition_evaluations":0,"largest_ring_fill":0}""".replace("\n", ""),
            Strings.toString(status)
        );
        assertEquals(7L, status.documentsFound());
        String human = Strings.toString(status, false, true);
        assertThat(human, containsString("\"stage_driver_cpu_time\":\"8nanos\""));
        assertThat(human, containsString("\"candidate_search_took\":\"10ms\""));
        assertThat(human, containsString("\"payload_search_took\":\"11ms\""));
        assertFalse(human.contains("latest_valid"));
        assertFalse(human.contains("condition_false_or_null"));
        assertFalse(human.contains("candidates_used_without_report"));
    }

    public void testEveryWorkCounterAffectsEqualityAndProfile() {
        long[] base = new long[12];
        long[] work = new long[] { 13, 14, 15, 16, 17, 18, 19 };
        var original = status(base, counters(work));
        String json = Strings.toString(original);
        String[] fields = {
            "candidates_dropped_at_bound",
            "searches_abandoned_at_work_limit",
            "keys_that_hit_bound",
            "search_steps",
            "max_search_steps_per_final_row",
            "condition_evaluations",
            "largest_ring_fill" };
        for (int index = 0; index < work.length; index++) {
            long[] changed = work.clone();
            changed[index]++;
            assertNotEquals(original, status(base, counters(changed)));
            assertThat(json, containsString("\"" + fields[index] + "\":" + work[index]));
        }
    }

    public void testWireLayoutHasSevenMandatoryCountersAndNoTrailingBytes() throws IOException {
        long[] base = randomValues(12);
        long[] work = randomValues(7);
        var original = status(base, counters(work));
        for (TransportVersion version : new TransportVersion[] { Sequence.ESQL_SEQUENCE_R5, TransportVersion.current() }) {
            try (var out = new BytesStreamOutput()) {
                out.setTransportVersion(version);
                original.writeTo(out);
                try (var in = out.bytes().streamInput()) {
                    in.setTransportVersion(version);
                    for (long value : base) {
                        assertEquals(value, in.readVLong());
                    }
                    for (long value : work) {
                        assertEquals(value, in.readVLong());
                    }
                    assertFalse(in.readBoolean());
                    assertEquals(0, in.available());
                }
            }
            // Assemble the independent expected layout as a second check of the reader.
            try (var out = new BytesStreamOutput()) {
                out.setTransportVersion(version);
                for (long value : base) {
                    out.writeVLong(value);
                }
                for (long value : work) {
                    out.writeVLong(value);
                }
                out.writeBoolean(false);
                try (var in = out.bytes().streamInput()) {
                    in.setTransportVersion(version);
                    assertEquals(original, SequenceSourceStatus.readFrom(in));
                    assertEquals(0, in.available());
                }
            }
        }
    }

    public void testNamedWriteableRegisteredByPlugin() throws IOException {
        var registry = new NamedWriteableRegistry(new EsqlPlugin().getNamedWriteables());
        var original = createTestInstance();
        try (var out = new BytesStreamOutput()) {
            out.writeNamedWriteable(original);
            try (var in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry)) {
                assertEquals(original, in.readNamedWriteable(Operator.Status.class));
                assertEquals(0, in.available());
            }
        }
    }

    public void testRejectUnsupportedPeerBeforeWritingOrReading() throws IOException {
        var unsupported = TransportVersionUtils.getPreviousVersion(Sequence.ESQL_SEQUENCE_R5);
        try (var out = new BytesStreamOutput()) {
            out.setTransportVersion(unsupported);
            expectThrows(IllegalArgumentException.class, () -> createTestInstance().writeTo(out));
            assertEquals(0, out.size());
            out.writeByte((byte) 42);
            try (var in = out.bytes().streamInput()) {
                in.setTransportVersion(unsupported);
                expectThrows(IllegalArgumentException.class, () -> SequenceSourceStatus.readFrom(in));
                assertEquals(1, in.available());
            }
        }
    }

    public void testOperatorStatusOmitsUnsupportedSnapshot() throws IOException {
        var registry = new NamedWriteableRegistry(new EsqlPlugin().getNamedWriteables());
        var snapshot = createTestInstance();
        var original = new OperatorStatus("SequenceSource[stages=2]", snapshot);
        for (TransportVersion version : new TransportVersion[] {
            TransportVersionUtils.getPreviousVersion(Sequence.ESQL_SEQUENCE_SINGLE_ENGINE),
            Sequence.ESQL_SEQUENCE_SINGLE_ENGINE,
            TransportVersionUtils.getPreviousVersion(Sequence.ESQL_SEQUENCE_R5),
            Sequence.ESQL_SEQUENCE_R5,
            TransportVersion.current() }) {
            try (var out = new BytesStreamOutput()) {
                out.setTransportVersion(version);
                original.writeTo(out);
                try (var in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry)) {
                    in.setTransportVersion(version);
                    var restored = OperatorStatus.readFrom(in);
                    assertEquals(original.operator(), restored.operator());
                    assertEquals(version.supports(Sequence.ESQL_SEQUENCE_R5) ? snapshot : null, restored.status());
                    assertEquals(0, in.available());
                }
            }
        }
    }

    public void testRuntimeProfileIsImmutableAndSerialized() throws IOException {
        var rows = new ArrayList<>(List.of(11L, 12L, 13L));
        var candidates = new ArrayList<>(List.of(21L, 22L, 23L));
        var runtime = new SequenceSourceStatus.RuntimeProfile(1, 2, 3, 4, 5, 6, rows, candidates, 7, 8);
        var equal = new SequenceSourceStatus.RuntimeProfile(1, 2, 3, 4, 5, 6, List.of(11L, 12L, 13L), List.of(21L, 22L, 23L), 7, 8);
        rows.set(0, 99L);
        candidates.clear();
        assertEquals(equal, runtime);
        assertEquals(equal.hashCode(), runtime.hashCode());
        expectThrows(UnsupportedOperationException.class, () -> runtime.rowsFetched().add(1L));
        expectThrows(UnsupportedOperationException.class, () -> runtime.candidatesAdmitted().add(1L));
        var original = status(new long[12], counters(new long[7]), runtime);
        assertThat(
            Strings.toString(original),
            containsString(
                "\"runtime\":{\"search_wait_nanos\":1,\"stage_run_nanos\":2,\"candidate_build_nanos\":3,"
                    + "\"matcher_nanos\":4,\"condition_nanos\":5,\"result_build_nanos\":6,\"rows_fetched\":[11,12,13],"
                    + "\"candidates_admitted\":[21,22,23],\"peak_keys\":7,\"peak_bytes_held\":8}"
            )
        );
        try (var out = new BytesStreamOutput()) {
            out.setTransportVersion(Sequence.ESQL_SEQUENCE_R5);
            original.writeTo(out);
            try (var in = out.bytes().streamInput()) {
                in.setTransportVersion(Sequence.ESQL_SEQUENCE_R5);
                assertEquals(original, SequenceSourceStatus.readFrom(in));
                assertEquals(0, in.available());
            }
        }
    }

    public void testEveryRuntimeFieldAffectsEquality() {
        var expected = new SequenceSourceStatus.RuntimeProfile(1, 2, 3, 4, 5, 6, List.of(11L), List.of(21L), 7, 8);
        var variants = List.of(
            new SequenceSourceStatus.RuntimeProfile(0, 2, 3, 4, 5, 6, List.of(11L), List.of(21L), 7, 8),
            new SequenceSourceStatus.RuntimeProfile(1, 0, 3, 4, 5, 6, List.of(11L), List.of(21L), 7, 8),
            new SequenceSourceStatus.RuntimeProfile(1, 2, 0, 4, 5, 6, List.of(11L), List.of(21L), 7, 8),
            new SequenceSourceStatus.RuntimeProfile(1, 2, 3, 0, 5, 6, List.of(11L), List.of(21L), 7, 8),
            new SequenceSourceStatus.RuntimeProfile(1, 2, 3, 4, 0, 6, List.of(11L), List.of(21L), 7, 8),
            new SequenceSourceStatus.RuntimeProfile(1, 2, 3, 4, 5, 0, List.of(11L), List.of(21L), 7, 8),
            new SequenceSourceStatus.RuntimeProfile(1, 2, 3, 4, 5, 6, List.of(12L), List.of(21L), 7, 8),
            new SequenceSourceStatus.RuntimeProfile(1, 2, 3, 4, 5, 6, List.of(11L), List.of(22L), 7, 8),
            new SequenceSourceStatus.RuntimeProfile(1, 2, 3, 4, 5, 6, List.of(11L), List.of(21L), 0, 8),
            new SequenceSourceStatus.RuntimeProfile(1, 2, 3, 4, 5, 6, List.of(11L), List.of(21L), 7, 0)
        );
        for (var variant : variants) {
            assertNotEquals(expected, variant);
            assertNotEquals(status(new long[12], counters(new long[7]), expected), status(new long[12], counters(new long[7]), variant));
        }
    }

    public void testRuntimeRejectsInvalidMeasurements() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new SequenceSourceStatus.RuntimeProfile(-1, 0, 0, 0, 0, 0, List.of(0L), List.of(0L), 0, 0)
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new SequenceSourceStatus.RuntimeProfile(0, 0, 0, 0, 0, 0, List.of(-1L), List.of(0L), 0, 0)
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new SequenceSourceStatus.RuntimeProfile(0, 0, 0, 0, 0, 0, List.of(0L), List.of(), 0, 0)
        );
    }

    private static SequenceSourceStatus.RuntimeProfile runtime() {
        return new SequenceSourceStatus.RuntimeProfile(1, 2, 3, 4, 5, 6, List.of(11L, 12L), List.of(21L, 22L), 7, 8);
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

    private static long[] workValues(SequenceSourceStatus.LatestValidCounters counters) {
        return new long[] {
            counters.candidatesDroppedAtBound(),
            counters.searchesAbandonedAtWorkLimit(),
            counters.keysThatHitBound(),
            counters.searchSteps(),
            counters.maxSearchStepsPerFinalRow(),
            counters.conditionEvaluations(),
            counters.largestRingFill() };
    }

    private static SequenceSourceStatus.LatestValidCounters counters(long[] values) {
        return new SequenceSourceStatus.LatestValidCounters(values[0], values[1], values[2], values[3], values[4], values[5], values[6]);
    }

    private static SequenceSourceStatus status(long[] values, SequenceSourceStatus.LatestValidCounters work) {
        return status(values, work, null);
    }

    private static SequenceSourceStatus status(
        long[] values,
        SequenceSourceStatus.LatestValidCounters work,
        SequenceSourceStatus.RuntimeProfile runtime
    ) {
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
            work,
            runtime
        );
    }
}

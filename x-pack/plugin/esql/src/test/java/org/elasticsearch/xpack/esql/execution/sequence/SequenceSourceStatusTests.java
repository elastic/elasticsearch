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
        long[] values = new long[12];
        for (int counter = 0; counter < values.length; counter++) {
            values[counter] = randomNonNegativeLong();
        }
        return status(values);
    }

    @Override
    protected SequenceSourceStatus mutateInstance(SequenceSourceStatus instance) {
        long[] values = new long[] {
            instance.candidateSearchRequests(),
            instance.payloadSearchRequests(),
            instance.candidateRowsFetched(),
            instance.payloadRowsFetched(),
            instance.completedMatches(),
            instance.rowsEmitted(),
            instance.stageDrivers(),
            instance.stageDriverCpuNanos(),
            instance.stageDriverIterations(),
            instance.candidateSearchTookMillis(),
            instance.payloadSearchTookMillis(),
            instance.maxPendingCandidates() };
        int counter = between(0, values.length - 1);
        values[counter] = randomValueOtherThan(values[counter], ESTestCase::randomNonNegativeLong);
        return status(values);
    }

    @Override
    protected Writeable.Reader<SequenceSourceStatus> instanceReader() {
        return SequenceSourceStatus::readFrom;
    }

    public void testToXContent() {
        var status = new SequenceSourceStatus(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
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

    public void testNamedWriteableRegisteredByPlugin() throws IOException {
        var registry = new NamedWriteableRegistry(new EsqlPlugin().getNamedWriteables());
        var original = createTestInstance();
        try (var out = new BytesStreamOutput()) {
            out.setTransportVersion(Sequence.ESQL_SEQUENCE);
            out.writeNamedWriteable(original);
            try (var in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry)) {
                in.setTransportVersion(Sequence.ESQL_SEQUENCE);
                assertEquals(original, in.readNamedWriteable(Operator.Status.class));
                assertEquals(0, in.available());
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
        var original = new OperatorStatus("SequenceSource[stages=2]", createTestInstance());
        for (TransportVersion version : new TransportVersion[] {
            TransportVersionUtils.getPreviousVersion(Sequence.ESQL_SEQUENCE),
            Sequence.ESQL_SEQUENCE }) {
            try (var out = new BytesStreamOutput()) {
                out.setTransportVersion(version);
                original.writeTo(out);
                try (var in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry)) {
                    in.setTransportVersion(version);
                    var restored = OperatorStatus.readFrom(in);
                    assertEquals(original.operator(), restored.operator());
                    assertEquals(version.supports(Sequence.ESQL_SEQUENCE) ? original.status() : null, restored.status());
                    assertEquals(0, in.available());
                }
            }
        }
    }

    private static SequenceSourceStatus status(long[] values) {
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
            values[11]
        );
    }
}

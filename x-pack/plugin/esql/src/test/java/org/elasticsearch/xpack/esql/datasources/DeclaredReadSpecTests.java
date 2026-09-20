/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DeclaredReadSpecTests extends AbstractWireSerializingTestCase<DeclaredReadSpec> {

    public static DeclaredReadSpec randomDeclaredReadSpec() {
        // Always non-empty so a round-trip yields a distinct-but-equal instance (an all-empty spec collapses to the
        // NONE singleton, which the wire test-harness flags as reference-equal to the original). The empty/NONE
        // collapse is covered directly by testNoneIsEmpty.
        Map<String, String> renames = new HashMap<>();
        int count = between(1, 3);
        for (int i = 0; i < count; i++) {
            renames.put(randomAlphaOfLength(4) + i, randomAlphaOfLength(5));
        }
        Map<String, String> dateFormats = new HashMap<>();
        int formatCount = between(0, 2);
        for (int i = 0; i < formatCount; i++) {
            dateFormats.put("ts" + i, randomFrom("epoch_millis", "yyyy-MM-dd", "dd/MMM/yyyy:HH:mm:ss Z"));
        }
        Set<String> declaredTypeColumns = new HashSet<>();
        int declaredCount = between(0, 3);
        for (int i = 0; i < declaredCount; i++) {
            declaredTypeColumns.add("col" + i);
        }
        // One draw for both flags: discovery sets them together, and the wire carries one bit for the pair at this
        // version, so an instance whose flags differ is unrepresentable and would red the round trip.
        boolean declared = randomBoolean();
        return DeclaredReadSpec.of(renames, dateFormats, declaredTypeColumns, declared, declared);
    }

    @Override
    protected Writeable.Reader<DeclaredReadSpec> instanceReader() {
        return DeclaredReadSpec::readFrom;
    }

    @Override
    protected DeclaredReadSpec createTestInstance() {
        return randomDeclaredReadSpec();
    }

    @Override
    protected DeclaredReadSpec mutateInstance(DeclaredReadSpec instance) throws IOException {
        Map<String, String> renames = new HashMap<>(instance.renames());
        Map<String, String> dateFormats = new HashMap<>(instance.dateFormats());
        Set<String> declaredTypeColumns = new HashSet<>(instance.declaredTypeColumns());
        boolean declared = instance.bindsByName();
        switch (between(0, 3)) {
            case 0 -> renames.put(randomAlphaOfLength(6), randomAlphaOfLength(6));
            case 1 -> dateFormats.put(randomAlphaOfLength(6), randomFrom("epoch_millis", "yyyy-MM-dd"));
            case 2 -> declaredTypeColumns.add(randomAlphaOfLength(6));
            default -> declared = declared == false; // both flags move together; see randomDeclaredReadSpec
        }
        return DeclaredReadSpec.of(renames, dateFormats, declaredTypeColumns, declared, declared);
    }

    public void testNoneIsEmpty() {
        assertTrue(DeclaredReadSpec.NONE.isEmpty());
        assertTrue(DeclaredReadSpec.of(Map.of()).isEmpty());
        assertSame(DeclaredReadSpec.NONE, DeclaredReadSpec.of(Map.of()));
        assertFalse(DeclaredReadSpec.of(Map.of("a", "b")).isEmpty());
        assertFalse(DeclaredReadSpec.of(Map.of(), Map.of(), Set.of("age")).isEmpty());
        // Either read instruction is itself an instruction: an otherwise-empty spec must NOT collapse to NONE, or
        // the signal would be silently dropped on the wire.
        assertFalse(DeclaredReadSpec.of(Map.of(), Map.of(), Set.of(), true, true).isEmpty());
        assertFalse(DeclaredReadSpec.of(Map.of(), Map.of(), Set.of(), true, false).isEmpty());
        assertFalse(DeclaredReadSpec.of(Map.of(), Map.of(), Set.of(), false, true).isEmpty());
        assertTrue(DeclaredReadSpec.of(Map.of(), Map.of(), Set.of(), false, false).isEmpty());
    }

    /**
     * A peer that predates the read-instruction transport version reads the three live fields plus the unused
     * {@code _id.path} slot; the instruction slot is skipped and both flags default false (= today's positional read
     * whose blank string cells are null), which is the safe mixed-cluster degradation. The other fields must survive
     * the downlevel round-trip unchanged.
     */
    public void testPreInstructionVersionDegradesToPositionalNullBlank() throws IOException {
        DeclaredReadSpec declared = DeclaredReadSpec.of(Map.of("id", "emp_no"), Map.of("ts", "epoch_millis"), Set.of("id"), true, true);
        // The version that added DeclaredReadSpec but NOT the read-instruction slot.
        TransportVersion preProvenance = TransportVersion.fromName("dataset_declared_schema");
        DeclaredReadSpec downlevel = copyInstance(declared, preProvenance);
        assertFalse(downlevel.bindsByName());
        assertFalse(downlevel.blankStringCellIsEmptyString());
        assertEquals(declared.renames(), downlevel.renames());
        assertEquals(declared.dateFormats(), downlevel.dateFormats());
        assertEquals(declared.declaredTypeColumns(), downlevel.declaredTypeColumns());
    }

    /**
     * A symmetric round-trip cannot catch the write and the read being dropped together, which desynchronises the
     * stream against a 9.5 peer rather than producing a wrong value. Hence a hand-written 9.5 stream: the trailing
     * marker only reads back if the slot was consumed.
     */
    public void testIdPathSlotIsReadAndDiscarded() throws IOException {
        TransportVersion preProvenance = TransportVersion.fromName("dataset_declared_schema");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(preProvenance);
            out.writeMap(Map.of("id", "emp_no"), StreamOutput::writeString, StreamOutput::writeString);
            out.writeOptionalString("request_id"); // the _id.path slot, as a 9.5 peer writes it
            out.writeMap(Map.of("ts", "epoch_millis"), StreamOutput::writeString, StreamOutput::writeString);
            out.writeCollection(Set.of("id"), StreamOutput::writeString);
            out.writeString("marker"); // whatever the enclosing message writes next

            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(preProvenance);
                DeclaredReadSpec spec = DeclaredReadSpec.readFrom(in);
                assertEquals(Map.of("id", "emp_no"), spec.renames());
                assertEquals(Map.of("ts", "epoch_millis"), spec.dateFormats());
                assertEquals(Set.of("id"), spec.declaredTypeColumns());
                assertEquals("the slot must be consumed, leaving the stream aligned", "marker", in.readString());
            }
        }
    }

    /**
     * At this version one wire bit carries both read instructions: discovery sets them together, so the slot that
     * used to hold the schema-provenance enum holds {@code bindsByName} and the reader derives the blank rule from
     * it. The change that first lets the two differ must add its own slot under a new transport version — this test
     * is what fails when someone lets them diverge without doing that.
     */
    public void testOneWireBitCarriesBothBehavioursAtThisVersion() throws IOException {
        for (boolean declared : new boolean[] { false, true }) {
            DeclaredReadSpec spec = DeclaredReadSpec.of(Map.of("id", "emp_no"), Map.of(), Set.of(), declared, declared);
            DeclaredReadSpec roundTripped = copyInstance(spec, TransportVersion.current());
            assertEquals(declared, roundTripped.bindsByName());
            assertEquals(declared, roundTripped.blankStringCellIsEmptyString());
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.TransportVersion;
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
        String idPath = randomBoolean() ? randomAlphaOfLength(4) : null;
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
        SchemaProvenance provenance = randomFrom(SchemaProvenance.values());
        return DeclaredReadSpec.of(renames, idPath, dateFormats, declaredTypeColumns, provenance);
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
        String idPath = instance.idPath();
        Map<String, String> dateFormats = new HashMap<>(instance.dateFormats());
        Set<String> declaredTypeColumns = new HashSet<>(instance.declaredTypeColumns());
        SchemaProvenance provenance = instance.provenance();
        switch (between(0, 4)) {
            case 0 -> renames.put(randomAlphaOfLength(6), randomAlphaOfLength(6));
            case 1 -> idPath = randomValueOtherThan(idPath, () -> randomBoolean() ? randomAlphaOfLength(5) : null);
            case 2 -> dateFormats.put(randomAlphaOfLength(6), randomFrom("epoch_millis", "yyyy-MM-dd"));
            case 3 -> declaredTypeColumns.add(randomAlphaOfLength(6));
            default -> provenance = provenance == SchemaProvenance.INFERRED ? SchemaProvenance.DECLARED : SchemaProvenance.INFERRED;
        }
        return DeclaredReadSpec.of(renames, idPath, dateFormats, declaredTypeColumns, provenance);
    }

    public void testNoneIsEmpty() {
        assertTrue(DeclaredReadSpec.NONE.isEmpty());
        assertTrue(DeclaredReadSpec.of(Map.of(), null).isEmpty());
        assertSame(DeclaredReadSpec.NONE, DeclaredReadSpec.of(Map.of(), null));
        assertFalse(DeclaredReadSpec.of(Map.of("a", "b"), null).isEmpty());
        assertFalse(DeclaredReadSpec.of(Map.of(), "id").isEmpty());
        assertFalse(DeclaredReadSpec.of(Map.of(), null, Map.of(), Set.of("age")).isEmpty());
        // DECLARED provenance is itself an instruction: an otherwise-empty spec must NOT collapse to NONE, or the
        // "bind by name" signal would be silently dropped on the wire.
        assertFalse(DeclaredReadSpec.of(Map.of(), null, Map.of(), Set.of(), SchemaProvenance.DECLARED).isEmpty());
        assertTrue(DeclaredReadSpec.of(Map.of(), null, Map.of(), Set.of(), SchemaProvenance.INFERRED).isEmpty());
    }

    /**
     * A peer that predates the provenance transport version reads only the four original fields; the enum is skipped
     * and defaults INFERRED (= today's positional behaviour), which is the safe mixed-cluster degradation. The other
     * fields must survive the downlevel round-trip unchanged.
     */
    public void testPreProvenanceVersionDegradesToInferred() throws IOException {
        DeclaredReadSpec declared = DeclaredReadSpec.of(
            Map.of("id", "emp_no"),
            "id",
            Map.of("ts", "epoch_millis"),
            Set.of("id"),
            SchemaProvenance.DECLARED
        );
        // The version that added DeclaredReadSpec but NOT the provenance field.
        TransportVersion preProvenance = TransportVersion.fromName("dataset_declared_schema");
        DeclaredReadSpec downlevel = copyInstance(declared, preProvenance);
        assertEquals(SchemaProvenance.INFERRED, downlevel.provenance());
        assertEquals(declared.renames(), downlevel.renames());
        assertEquals(declared.idPath(), downlevel.idPath());
        assertEquals(declared.dateFormats(), downlevel.dateFormats());
        assertEquals(declared.declaredTypeColumns(), downlevel.declaredTypeColumns());
    }
}

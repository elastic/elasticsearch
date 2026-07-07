/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

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
        return DeclaredReadSpec.of(renames, idPath, dateFormats, declaredTypeColumns);
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
        switch (between(0, 3)) {
            case 0 -> renames.put(randomAlphaOfLength(6), randomAlphaOfLength(6));
            case 1 -> idPath = randomValueOtherThan(idPath, () -> randomBoolean() ? randomAlphaOfLength(5) : null);
            case 2 -> dateFormats.put(randomAlphaOfLength(6), randomFrom("epoch_millis", "yyyy-MM-dd"));
            default -> declaredTypeColumns.add(randomAlphaOfLength(6));
        }
        return DeclaredReadSpec.of(renames, idPath, dateFormats, declaredTypeColumns);
    }

    public void testNoneIsEmpty() {
        assertTrue(DeclaredReadSpec.NONE.isEmpty());
        assertTrue(DeclaredReadSpec.of(Map.of(), null).isEmpty());
        assertSame(DeclaredReadSpec.NONE, DeclaredReadSpec.of(Map.of(), null));
        assertFalse(DeclaredReadSpec.of(Map.of("a", "b"), null).isEmpty());
        assertFalse(DeclaredReadSpec.of(Map.of(), "id").isEmpty());
        assertFalse(DeclaredReadSpec.of(Map.of(), null, Map.of(), Set.of("age")).isEmpty());
    }
}

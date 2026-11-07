/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.core.type.UnionEsField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class UnionEsFieldTests extends AbstractEsFieldTypeTests<UnionEsField> {

    static UnionEsField randomUnionEsField() {
        List<DataType> candidates = List.of(
            DataType.BOOLEAN,
            DataType.DATETIME,
            DataType.DOUBLE,
            DataType.INTEGER,
            DataType.IP,
            DataType.KEYWORD,
            DataType.LONG,
            DataType.VERSION
        );
        List<DataType> legs = new ArrayList<>(randomSubsetOf(between(2, candidates.size()), candidates));
        legs.sort(Comparator.comparing(DataType::typeName));
        return new UnionEsField(randomAlphaOfLength(4), legs);
    }

    @Override
    protected UnionEsField createTestInstance() {
        return randomUnionEsField();
    }

    @Override
    protected UnionEsField mutateInstance(UnionEsField instance) {
        return new UnionEsField(randomAlphaOfLength(5), instance.legTypes());
    }

    public void testFromAConflictedFieldTakesTheCanonicalLegOrder() {
        InvalidMappedField conflicted = new InvalidMappedField(
            "flow",
            new TreeMap<>(Map.of("long", Set.of("a"), "keyword", Set.of("b"), "float", Set.of("c")))
        );
        UnionEsField union = UnionEsField.from(conflicted);
        assertEquals("flow", union.getName());
        assertEquals(DataType.UNION, union.getDataType());
        assertEquals(conflicted.unionLegTypes(), union.legTypes());
        assertEquals(List.of(DataType.DOUBLE, DataType.KEYWORD, DataType.LONG), union.legTypes());
        assertFalse("there is no one doc-values type to aggregate over", union.isAggregatable());
    }

    /** A leg is found by its widened type, since a float value loads as a double. */
    public void testLegOfWidensBeforeLookingUp() {
        UnionEsField union = new UnionEsField("flow", List.of(DataType.DOUBLE, DataType.KEYWORD));
        assertEquals(0, union.legOf(DataType.DOUBLE));
        assertEquals(0, union.legOf(DataType.FLOAT));
        assertEquals(1, union.legOf(DataType.KEYWORD));
        assertEquals(-1, union.legOf(DataType.BOOLEAN));
    }

    public void testAUnionNeedsTwoLegs() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new UnionEsField("flow", List.of(DataType.LONG)));
        assertEquals("a union field needs at least two legs, got [LONG]", e.getMessage());
    }

    public void testSerializationKeepsTheLegOrder() throws IOException {
        UnionEsField union = new UnionEsField("flow", List.of(DataType.DATETIME, DataType.KEYWORD, DataType.LONG));
        UnionEsField roundTripped = copyInstance(union);
        assertEquals(union.legTypes(), roundTripped.legTypes());
        assertEquals(union, roundTripped);
    }
}

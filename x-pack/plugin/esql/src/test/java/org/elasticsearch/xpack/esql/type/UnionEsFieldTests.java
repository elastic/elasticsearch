/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.UnionBlockBuilder;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.core.type.UnionEsField;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.hamcrest.Matchers.hasKey;

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
        // Constructed as UnionTypesCleanup constructs it, since that is the only production site.
        UnionEsField union = new UnionEsField(conflicted.getName(), conflicted.unionLegTypes());
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

    public void testAUnionNeedsTwoDistinctLegTypes() {
        assertTrue(UnionEsField.canUnion(List.of(DataType.KEYWORD, DataType.LONG)));
        assertFalse("one leg is no conflict", UnionEsField.canUnion(List.of(DataType.LONG)));
        assertFalse(UnionEsField.canUnion(List.of()));
    }

    public void testAUnionRefusesALegTypeNoBlockCanHold() {
        assertFalse(UnionEsField.canUnion(List.of(DataType.KEYWORD, DataType.GEO_POINT)));
        assertFalse(UnionEsField.canUnion(List.of(DataType.AGGREGATE_METRIC_DOUBLE, DataType.DOUBLE)));
        assertFalse(
            "an unsupported type is not one of a field's value types",
            UnionEsField.canUnion(List.of(DataType.KEYWORD, DataType.UNSUPPORTED))
        );
    }

    /**
     * Types a leg could physically hold that are deliberately not leg types, with the reason. Each is a decision rather than an
     * oversight, so {@link #testALegTypeIsEitherAdmittedOrDeliberatelyExcluded} can tell the two apart.
     */
    private static final Map<DataType, String> NOT_LEG_TYPES_ON_PURPOSE = Map.ofEntries(
        Map.entry(DataType.UNSUPPORTED, "a value type a field never has; it is what a field becomes when it has none"),
        Map.entry(DataType.SOURCE, "_source is not a mapped field, so no mapping can disagree about it"),
        Map.entry(DataType.TSID_DATA_TYPE, "generated, not mapped"),
        Map.entry(DataType.HISTOGRAM, "not one scalar value"),
        Map.entry(DataType.DENSE_VECTOR, "not one scalar value"),
        Map.entry(DataType.GEO_POINT, "its element type follows the field-extract preference, not the type"),
        Map.entry(DataType.CARTESIAN_POINT, "its element type follows the field-extract preference, not the type"),
        Map.entry(DataType.GEO_SHAPE, "its element type follows the field-extract preference, not the type"),
        Map.entry(DataType.CARTESIAN_SHAPE, "its element type follows the field-extract preference, not the type"),
        Map.entry(DataType.GEOHASH, "a grid type, out of scope while the union type is being built"),
        Map.entry(DataType.GEOTILE, "a grid type, out of scope while the union type is being built"),
        Map.entry(DataType.GEOHEX, "a grid type, out of scope while the union type is being built"),
        Map.entry(DataType.COUNTER_LONG, "a metric type with its own rules, out of scope while the union type is being built"),
        Map.entry(DataType.COUNTER_INTEGER, "a metric type with its own rules, out of scope while the union type is being built"),
        Map.entry(DataType.COUNTER_DOUBLE, "a metric type with its own rules, out of scope while the union type is being built")
    );

    /**
     * The logical leg list and the physical one are two enumerations over the same ground, and nothing but this makes them agree.
     * Every type {@link UnionEsField#isLegType} admits must map to an element type a leg can actually hold, or the analyzer would
     * offer a union the block refuses to build. The converse is weaker on purpose: the logical list is the narrower of the two, so a
     * type it refuses that a leg could hold has to be named above.
     */
    public void testALegTypeIsEitherAdmittedOrDeliberatelyExcluded() {
        for (DataType type : DataType.values()) {
            ElementType elementType;
            try {
                elementType = PlannerUtils.toElementType(type);
            } catch (Exception e) {
                assertFalse(type + " has no element type at all, so it cannot be a leg", UnionEsField.isLegType(type));
                continue;
            }
            if (UnionEsField.isLegType(type)) {
                assertTrue(type + " is a leg type but a leg cannot hold " + elementType, UnionBlockBuilder.canHoldLeg(elementType));
            } else if (UnionBlockBuilder.canHoldLeg(elementType)) {
                assertThat(
                    type + " could be a leg of " + elementType + " but is not one; decide and say why, or admit it",
                    NOT_LEG_TYPES_ON_PURPOSE,
                    hasKey(type)
                );
            }
        }
    }

    /**
     * A union is not sortable, which is a statement about the type rather than about any one plan: two values of different types
     * have no order between them. For a field the exactness check refuses {@code SORT} first, so this is the gate that will carry
     * it once a union can be the type of something that is not a field.
     */
    public void testAUnionIsNotSortable() {
        assertFalse(DataType.isSortable(DataType.UNION));
    }

    /** Nothing in the exclusion list may be a leg type: the two halves would then disagree about the same type. */
    public void testTheExclusionListHoldsOnlyNonLegTypes() {
        for (DataType type : NOT_LEG_TYPES_ON_PURPOSE.keySet()) {
            assertFalse(type + " is excluded on purpose and admitted at the same time", UnionEsField.isLegType(type));
        }
    }

    public void testSerializationKeepsTheLegOrder() throws IOException {
        UnionEsField union = new UnionEsField("flow", List.of(DataType.DATETIME, DataType.KEYWORD, DataType.LONG));
        UnionEsField roundTripped = copyInstance(union);
        assertEquals(union.legTypes(), roundTripped.legTypes());
        assertEquals(union, roundTripped);
    }
}

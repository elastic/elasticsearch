/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.hamcrest.Matchers.containsString;

public class InvalidMappedFieldTests extends AbstractEsFieldTypeTests<InvalidMappedField> {
    static InvalidMappedField randomInvalidMappedField(int maxPropertiesDepth) {
        String name = randomAlphaOfLength(4);
        Map<String, Set<String>> typesToIndices = randomTypesToIndices();
        // potentiallyUnmapped always uses empty properties; the non-potentially-unmapped variant may carry properties.
        if (randomBoolean()) {
            return InvalidMappedField.potentiallyUnmapped(name, typesToIndices);
        }
        Map<String, EsField> properties = randomProperties(maxPropertiesDepth);
        return properties.isEmpty() && randomBoolean()
            ? new InvalidMappedField(name, typesToIndices)
            : new InvalidMappedField(name, typesToIndices, properties);
    }

    static Map<String, Set<String>> randomTypesToIndices() {
        int typeCount = between(1, 4);
        Map<String, Set<String>> typesToIndices = new TreeMap<>();
        while (typesToIndices.size() < typeCount) {
            String type = randomAlphaOfLengthBetween(3, 8);
            int indexCount = between(1, 3);
            Set<String> indices = new TreeSet<>();
            while (indices.size() < indexCount) {
                indices.add(randomAlphaOfLengthBetween(3, 10));
            }
            typesToIndices.put(type, indices);
        }
        return typesToIndices;
    }

    @Override
    protected InvalidMappedField createTestInstance() {
        return randomInvalidMappedField(4);
    }

    @Override
    protected InvalidMappedField copyInstance(InvalidMappedField instance, TransportVersion version) {
        // writeContent throws UnsupportedOperationException; copy directly without going through the wire.
        if (instance.isPotentiallyUnmapped()) {
            return InvalidMappedField.potentiallyUnmapped(instance.getName(), instance.getTypesToIndices());
        }
        return new InvalidMappedField(instance.getName(), instance.getTypesToIndices(), instance.getProperties());
    }

    @Override
    protected InvalidMappedField mutateInstance(InvalidMappedField instance) {
        String name = instance.getName();
        Map<String, EsField> properties = instance.getProperties();
        Map<String, Set<String>> typesToIndices = instance.getTypesToIndices();
        boolean isPotentiallyUnmapped = instance.isPotentiallyUnmapped();
        // Pick a mutation that's actually applicable: the potentiallyUnmapped variant always has empty properties, so we can't
        // mutate properties on it without first flipping isPotentiallyUnmapped.
        int max = isPotentiallyUnmapped ? 2 : 3;
        switch (between(0, max)) {
            case 0 -> name = randomAlphaOfLength(name.length() + 1);
            case 1 -> typesToIndices = randomValueOtherThan(typesToIndices, InvalidMappedFieldTests::randomTypesToIndices);
            case 2 -> isPotentiallyUnmapped = isPotentiallyUnmapped == false;
            case 3 -> properties = randomValueOtherThan(properties, () -> randomProperties(4));
            default -> throw new IllegalArgumentException();
        }
        if (isPotentiallyUnmapped) {
            // Reconstruction discards properties on this variant; we only get here if properties was already empty (or we just flipped
            // isPotentiallyUnmapped from false→true, in which case discarding any prior properties is intended).
            return InvalidMappedField.potentiallyUnmapped(name, typesToIndices);
        }
        return new InvalidMappedField(name, typesToIndices, properties);
    }

    /**
     * The leg order is a function of the leg types, so the block and the response each derive it and cannot
     * disagree. Small numerics are widened first: a {@code float} is widened on load and never reaches a value,
     * so a leg declared as one could not be rendered.
     */
    public void testUnionLegTypesAreWidenedDedupedAndOrderedByName() {
        InvalidMappedField field = new InvalidMappedField(
            "flow",
            new TreeMap<>(
                Map.of(
                    "long",
                    Set.of("index-1"),
                    "keyword",
                    Set.of("index-2"),
                    "float",
                    Set.of("index-3"),
                    "double",
                    Set.of("index-4"),
                    "date",
                    Set.of("index-5")
                )
            )
        );
        // float widens to double and collapses into the double leg; the rest sort by type name.
        assertEquals(List.of(DataType.DATETIME, DataType.DOUBLE, DataType.KEYWORD, DataType.LONG), field.unionLegTypes());
    }

    /** The order does not depend on the order the types were discovered in. */
    public void testUnionLegTypeOrderIsIndependentOfDiscoveryOrder() {
        // A TreeMap iterates boolean, keyword, long; the LinkedHashMap below iterates long, keyword, boolean.
        Map<String, Set<String>> sorted = new TreeMap<>(Map.of("long", Set.of("a"), "keyword", Set.of("b"), "boolean", Set.of("c")));
        Map<String, Set<String>> insertionOrdered = new LinkedHashMap<>();
        insertionOrdered.put("long", Set.of("a"));
        insertionOrdered.put("keyword", Set.of("b"));
        insertionOrdered.put("boolean", Set.of("c"));

        List<DataType> fromSorted = new InvalidMappedField("flow", sorted).unionLegTypes();
        assertEquals(fromSorted, new InvalidMappedField("flow", insertionOrdered).unionLegTypes());
        assertEquals(List.of(DataType.BOOLEAN, DataType.KEYWORD, DataType.LONG), fromSorted);
    }

    /**
     * A field unmapped in some of its indices gets a keyword leg for what those indices load from {@code _source}. Without it a
     * value loaded there would have no leg to go in, and the error message for the same field already counts that type.
     */
    public void testAPotentiallyUnmappedConflictGetsAKeywordLeg() {
        InvalidMappedField field = InvalidMappedField.potentiallyUnmapped(
            "flow",
            new TreeMap<>(Map.of("ip", Set.of("index-1"), "long", Set.of("index-2")))
        );
        assertEquals(List.of(DataType.IP, DataType.KEYWORD, DataType.LONG), field.unionLegTypes());
        assertThat(field.errorMessage(), containsString("[keyword] due to loading from _source"));
    }

    /** The keyword leg is declared once, not twice, when the field is also mapped as a keyword somewhere. */
    public void testTheUnmappedKeywordLegIsNotDuplicated() {
        InvalidMappedField field = InvalidMappedField.potentiallyUnmapped(
            "flow",
            new TreeMap<>(Map.of("keyword", Set.of("index-1"), "long", Set.of("index-2")))
        );
        assertEquals(List.of(DataType.KEYWORD, DataType.LONG), field.unionLegTypes());
    }

}

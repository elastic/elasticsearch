/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

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
}

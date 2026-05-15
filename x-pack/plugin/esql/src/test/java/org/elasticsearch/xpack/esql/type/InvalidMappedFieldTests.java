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

import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;

public class InvalidMappedFieldTests extends AbstractEsFieldTypeTests<InvalidMappedField> {
    static InvalidMappedField randomInvalidMappedField(int maxPropertiesDepth) {
        String name = randomAlphaOfLength(4);
        String errorMessage = randomAlphaOfLengthBetween(1, 100);
        Map<String, EsField> properties = randomProperties(maxPropertiesDepth);
        return new InvalidMappedField(name, errorMessage, properties);
    }

    @Override
    protected InvalidMappedField createTestInstance() {
        return randomInvalidMappedField(4);
    }

    @Override
    protected InvalidMappedField mutateInstance(InvalidMappedField instance) {
        String name = instance.getName();
        String errorMessage = instance.errorMessage();
        Map<String, EsField> properties = instance.getProperties();
        switch (between(0, 1)) {
            case 0 -> name = randomAlphaOfLength(name.length() + 1);
            case 1 -> properties = randomValueOtherThan(properties, () -> randomProperties(4));
            default -> throw new IllegalArgumentException();
        }
        return new InvalidMappedField(name, errorMessage, properties);
    }

    public void testEqualsAndHashCodeIncludeConflictMetadata() {
        InvalidMappedField first = new InvalidMappedField("field", Map.of(INTEGER.typeName(), Set.of("index-a")));
        InvalidMappedField second = new InvalidMappedField("field", Map.of(LONG.typeName(), Set.of("index-b")));
        InvalidMappedField potentiallyUnmapped = InvalidMappedField.potentiallyUnmapped(
            "field",
            Map.of(INTEGER.typeName(), Set.of("index-a"))
        );

        assertNotEquals(first, second);
        assertNotEquals(first.hashCode(), second.hashCode());
        assertNotEquals(first, potentiallyUnmapped);
        assertNotEquals(first.hashCode(), potentiallyUnmapped.hashCode());
    }

    public void testErrorMessageIsDerivedFromConflictMetadata() {
        InvalidMappedField field = InvalidMappedField.potentiallyUnmapped("field", Map.of(KEYWORD.typeName(), Set.of("index-a")));

        assertEquals("mapped as [1] incompatible types: [keyword] due to loading from _source and in [index-a]", field.errorMessage());
    }
}

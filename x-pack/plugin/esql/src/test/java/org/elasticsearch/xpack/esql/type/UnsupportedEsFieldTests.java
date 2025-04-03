/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.UnsupportedEsField;

import java.util.List;
import java.util.Map;

public class UnsupportedEsFieldTests extends AbstractEsFieldTypeTests<UnsupportedEsField> {
    public static UnsupportedEsField randomUnsupportedEsField(int maxPropertiesDepth) {
        String name = randomAlphaOfLength(4);
        List<String> originalTypes = randomOriginalTypes();
        String inherited = randomBoolean() ? null : randomAlphaOfLength(5);
        Map<String, EsField> properties = randomProperties(maxPropertiesDepth);
        return new UnsupportedEsField(name, originalTypes, inherited, properties);
    }

    public static List<String> randomOriginalTypes() {
        return randomBoolean() ? List.of(randomAlphaOfLength(5)) : randomList(4, 4, () -> randomAlphaOfLength(5));
    }

    @Override
    protected UnsupportedEsField createTestInstance() {
        return randomUnsupportedEsField(4);
    }

    @Override
    protected UnsupportedEsField mutate(UnsupportedEsField instance) {
        String name = instance.getName();
        List<String> originalTypes = instance.getOriginalTypes();
        String inherited = instance.getInherited();
        Map<String, EsField> properties = instance.getProperties();
        switch (between(0, 3)) {
            case 0 -> name = randomAlphaOfLength(name.length() + 1);
            case 1 -> originalTypes = randomValueOtherThan(originalTypes, UnsupportedEsFieldTests::randomOriginalTypes);
            case 2 -> inherited = randomValueOtherThan(inherited, () -> randomBoolean() ? null : randomAlphaOfLength(4));
            case 3 -> properties = randomValueOtherThan(properties, () -> randomProperties(4));
            default -> throw new IllegalArgumentException();
        }
        return new UnsupportedEsField(name, originalTypes, inherited, properties);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.SemanticTextEsField;

import java.util.Map;
import java.util.Set;

public class SemanticTextEsFieldTests extends AbstractEsFieldTypeTests<SemanticTextEsField> {
    @Override
    protected SemanticTextEsField createTestInstance() {
        return randomSemanticTextEsField(4);
    }

    static SemanticTextEsField randomSemanticTextEsField(int maxPropertiesDepth) {
        String name = randomAlphaOfLength(4);
        Map<String, EsField> properties = randomProperties(maxPropertiesDepth);
        boolean hasDocValues = randomBoolean();
        boolean isAlias = randomBoolean();
        Set<String> inferenceIds = randomSet(1, 10, () -> randomAlphaOfLength(8));
        return new SemanticTextEsField(name, properties, hasDocValues, isAlias, inferenceIds);
    }

    @Override
    protected SemanticTextEsField mutate(SemanticTextEsField instance) {
        String name = instance.getName();
        Map<String, EsField> properties = instance.getProperties();
        boolean hasDocValues = instance.isAggregatable();
        boolean isAlias = instance.isAlias();
        Set<String> inferenceIds = instance.inferenceIds();
        switch (between(0, 4)) {
            case 0 -> name = randomAlphaOfLength(5);
            case 1 -> properties = randomValueOtherThan(properties, () -> randomProperties(4));
            case 2 -> hasDocValues = false == hasDocValues;
            case 3 -> isAlias = false == isAlias;
            case 4 -> inferenceIds = randomSet(1, 10, () -> randomAlphaOfLength(7));
            default -> throw new IllegalArgumentException();
        }

        return new SemanticTextEsField(name, properties, hasDocValues, isAlias, inferenceIds);
    }
}

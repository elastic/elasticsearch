/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;

import java.util.Map;

public class KeywordEsFieldTests extends AbstractEsFieldTypeTests<KeywordEsField> {
    static KeywordEsField randomKeywordEsField(int maxPropertiesDepth) {
        String name = randomAlphaOfLength(4);
        Map<String, EsField> properties = randomProperties(maxPropertiesDepth);
        boolean hasDocValues = randomBoolean();
        int precision = randomInt();
        boolean normalized = randomBoolean();
        boolean isAlias = randomBoolean();
        return new KeywordEsField(name, properties, hasDocValues, precision, normalized, isAlias);
    }

    @Override
    protected KeywordEsField createTestInstance() {
        return randomKeywordEsField(4);
    }

    @Override
    protected KeywordEsField mutate(KeywordEsField instance) {
        String name = instance.getName();
        Map<String, EsField> properties = instance.getProperties();
        boolean hasDocValues = instance.isAggregatable();
        int precision = instance.getPrecision();
        boolean normalized = instance.getNormalized();
        boolean isAlias = instance.isAlias();
        switch (between(0, 5)) {
            case 0 -> name = randomAlphaOfLength(name.length() + 1);
            case 1 -> properties = randomValueOtherThan(properties, () -> randomProperties(4));
            case 2 -> hasDocValues = false == hasDocValues;
            case 3 -> precision = randomValueOtherThan(precision, ESTestCase::randomInt);
            case 4 -> normalized = false == normalized;
            case 5 -> isAlias = false == isAlias;
            default -> throw new IllegalArgumentException();
        }
        return new KeywordEsField(name, properties, hasDocValues, precision, normalized, isAlias);
    }
}

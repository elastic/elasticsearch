/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.TextEsField;

import java.util.Map;

import static org.elasticsearch.xpack.esql.type.EsFieldTestUtils.randomProperties;
import static org.elasticsearch.xpack.esql.type.EsFieldTestUtils.randomTextEsField;

public class TextEsFieldTests extends AbstractEsFieldTypeTests<TextEsField> {
    @Override
    protected TextEsField createTestInstance() {
        return randomTextEsField(4);
    }

    @Override
    protected TextEsField mutateInstance(TextEsField instance) {
        String name = instance.getName();
        Map<String, EsField> properties = instance.getProperties();
        boolean hasDocValues = instance.isAggregatable();
        boolean isAlias = instance.isAlias();
        EsField.TimeSeriesFieldType tsType = instance.getTimeSeriesFieldType();
        switch (between(0, 4)) {
            case 0 -> name = randomAlphaOfLength(name.length() + 1);
            case 1 -> properties = randomValueOtherThan(properties, () -> randomProperties(4));
            case 2 -> hasDocValues = false == hasDocValues;
            case 3 -> isAlias = false == isAlias;
            case 4 -> tsType = randomValueOtherThan(tsType, () -> randomFrom(EsField.TimeSeriesFieldType.values()));
            default -> throw new IllegalArgumentException();
        }
        return new TextEsField(name, properties, hasDocValues, isAlias, tsType);
    }
}

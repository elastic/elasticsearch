/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.xpack.esql.core.type.DateEsField;
import org.elasticsearch.xpack.esql.core.type.EsField;

import java.util.Map;

public class DateEsFieldTests extends AbstractEsFieldTypeTests<DateEsField> {
    static DateEsField randomDateEsField(int maxPropertiesDepth) {
        return DateEsField.dateEsField(
            randomAlphaOfLength(5),
            randomProperties(maxPropertiesDepth),
            randomBoolean(),
            randomFrom(EsField.TimeSeriesFieldType.values())
        );
    }

    @Override
    protected DateEsField createTestInstance() {
        return randomDateEsField(4);
    }

    @Override
    protected DateEsField mutateInstance(DateEsField instance) {
        String name = instance.getName();
        Map<String, EsField> properties = instance.getProperties();
        boolean aggregatable = instance.isAggregatable();
        EsField.TimeSeriesFieldType tsType = instance.getTimeSeriesFieldType();
        switch (between(0, 3)) {
            case 0 -> name = randomAlphaOfLength(name.length() + 1);
            case 1 -> properties = randomValueOtherThan(properties, () -> randomProperties(4));
            case 2 -> aggregatable = false == aggregatable;
            case 3 -> tsType = randomValueOtherThan(tsType, () -> randomFrom(EsField.TimeSeriesFieldType.values()));
            default -> throw new IllegalArgumentException();
        }
        return DateEsField.dateEsField(name, properties, aggregatable, tsType);
    }
}

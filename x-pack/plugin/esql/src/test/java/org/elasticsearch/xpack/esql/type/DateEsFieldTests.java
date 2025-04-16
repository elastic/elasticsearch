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
        return DateEsField.dateEsField(randomAlphaOfLength(5), randomProperties(maxPropertiesDepth), randomBoolean());
    }

    @Override
    protected DateEsField createTestInstance() {
        return randomDateEsField(4);
    }

    @Override
    protected DateEsField mutate(DateEsField instance) {
        String name = instance.getName();
        Map<String, EsField> properties = instance.getProperties();
        boolean aggregatable = instance.isAggregatable();
        switch (between(0, 2)) {
            case 0 -> name = randomAlphaOfLength(name.length() + 1);
            case 1 -> properties = randomValueOtherThan(properties, () -> randomProperties(4));
            case 2 -> aggregatable = false == aggregatable;
            default -> throw new IllegalArgumentException();
        }
        return DateEsField.dateEsField(name, properties, aggregatable);
    }
}

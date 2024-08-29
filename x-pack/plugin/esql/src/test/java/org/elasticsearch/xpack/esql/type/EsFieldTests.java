/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;

import java.util.Map;

public class EsFieldTests extends AbstractEsFieldTypeTests<EsField> {
    static EsField randomEsField(int maxPropertiesDepth) {
        String name = randomAlphaOfLength(4);
        DataType esDataType = randomFrom(DataType.types());
        Map<String, EsField> properties = randomProperties(maxPropertiesDepth);
        boolean aggregatable = randomBoolean();
        boolean isAlias = randomBoolean();
        return new EsField(name, esDataType, properties, aggregatable, isAlias);
    }

    @Override
    protected EsField createTestInstance() {
        return randomEsField(4);
    }

    @Override
    protected EsField mutate(EsField instance) {
        String name = instance.getName();
        DataType esDataType = instance.getDataType();
        Map<String, EsField> properties = instance.getProperties();
        boolean aggregatable = instance.isAggregatable();
        boolean isAlias = instance.isAlias();
        switch (between(0, 4)) {
            case 0 -> name = randomAlphaOfLength(name.length() + 1);
            case 1 -> esDataType = randomValueOtherThan(esDataType, () -> randomFrom(DataType.types()));
            case 2 -> properties = randomValueOtherThan(properties, () -> randomProperties(4));
            case 3 -> aggregatable = false == aggregatable;
            case 4 -> isAlias = false == isAlias;
            default -> throw new IllegalArgumentException();
        }
        return new EsField(name, esDataType, properties, aggregatable, isAlias);
    }
}

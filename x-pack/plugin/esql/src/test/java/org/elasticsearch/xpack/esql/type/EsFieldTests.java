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
    public static EsField randomEsField(int maxPropertiesDepth) {
        String name = randomAlphaOfLength(4);
        DataType esDataType = randomValueOtherThanMany(
            t -> false == t.supportedVersion().supportedLocally(),
            () -> randomFrom(DataType.types())
        );
        Map<String, EsField> properties = randomProperties(maxPropertiesDepth);
        boolean aggregatable = randomBoolean();
        boolean isAlias = randomBoolean();
        EsField.TimeSeriesFieldType tsType = randomFrom(EsField.TimeSeriesFieldType.values());
        return new EsField(name, esDataType, properties, aggregatable, isAlias, tsType);
    }

    @Override
    protected EsField createTestInstance() {
        return randomEsField(4);
    }

    @Override
    protected EsField mutateInstance(EsField instance) {
        String name = instance.getName();
        DataType esDataType = instance.getDataType();
        Map<String, EsField> properties = instance.getProperties();
        boolean aggregatable = instance.isAggregatable();
        boolean isAlias = instance.isAlias();
        EsField.TimeSeriesFieldType tsType = instance.getTimeSeriesFieldType();
        switch (between(0, 5)) {
            case 0 -> name = randomAlphaOfLength(name.length() + 1);
            case 1 -> esDataType = randomValueOtherThan(esDataType, () -> randomFrom(DataType.types()));
            case 2 -> properties = randomValueOtherThan(properties, () -> randomProperties(4));
            case 3 -> aggregatable = false == aggregatable;
            case 4 -> isAlias = false == isAlias;
            case 5 -> tsType = randomValueOtherThan(tsType, () -> randomFrom(EsField.TimeSeriesFieldType.values()));
            default -> throw new IllegalArgumentException();
        }
        return new EsField(name, esDataType, properties, aggregatable, isAlias, tsType);
    }
}

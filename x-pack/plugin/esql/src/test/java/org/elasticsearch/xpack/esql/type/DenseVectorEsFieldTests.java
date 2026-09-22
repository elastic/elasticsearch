/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.xpack.esql.core.type.DenseVectorEsField;
import org.elasticsearch.xpack.esql.core.type.EsField;

import java.util.Map;

import static org.elasticsearch.xpack.esql.type.EsFieldTestUtils.randomDenseVectorEsField;
import static org.elasticsearch.xpack.esql.type.EsFieldTestUtils.randomProperties;

public class DenseVectorEsFieldTests extends AbstractEsFieldTypeTests<DenseVectorEsField> {
    @Override
    protected DenseVectorEsField createTestInstance() {
        return randomDenseVectorEsField(4, null);
    }

    @Override
    protected DenseVectorEsField mutateInstance(DenseVectorEsField instance) {
        String name = instance.getName();
        Map<String, EsField> properties = instance.getProperties();
        boolean aggregatable = instance.isAggregatable();
        boolean isAlias = instance.isAlias();
        EsField.TimeSeriesFieldType tsType = instance.getTimeSeriesFieldType();
        boolean indexed = instance.isIndexed();
        switch (between(0, 5)) {
            case 0 -> name = randomAlphaOfLength(name.length() + 1);
            case 1 -> properties = randomValueOtherThan(properties, () -> randomProperties(4));
            case 2 -> aggregatable = false == aggregatable;
            case 3 -> isAlias = false == isAlias;
            case 4 -> tsType = randomValueOtherThan(tsType, () -> randomFrom(EsField.TimeSeriesFieldType.values()));
            case 5 -> indexed = false == indexed;
            default -> throw new IllegalArgumentException();
        }
        return new DenseVectorEsField(name, properties, aggregatable, isAlias, tsType, indexed);
    }
}

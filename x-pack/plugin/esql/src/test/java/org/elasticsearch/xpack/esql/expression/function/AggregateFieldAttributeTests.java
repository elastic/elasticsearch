/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;

import java.util.Map;

public class AggregateFieldAttributeTests extends AbstractAttributeTestCase<FieldAttribute> {
    static FieldAttribute createFieldAttribute() {
        Source source = Source.EMPTY;
        String name = randomAlphaOfLength(5);
        EsField field = new EsField(name, DataType.AGGREGATE_DOUBLE_METRIC, Map.of(), true);
        var aggregatedAttr = FieldAttribute.createAggregatedFieldAttribute(source, name, field);
        switch (randomInt(4)) {
            case 0:
                return aggregatedAttr;
            case 1:
                return aggregatedAttr.getAggregatedMinSubField();
            case 2:
                return aggregatedAttr.getAggregatedMaxSubField();
            case 3:
                return aggregatedAttr.getAggregatedSumSubField();
            case 4:
                return aggregatedAttr.getAggregatedValueCountSubField();
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
    }

    @Override
    protected FieldAttribute create() {
        return createFieldAttribute();
    }

    @Override
    protected FieldAttribute mutate(FieldAttribute instance) {
        Source source = instance.source();
        String name = instance.name();
        String qualifier = instance.qualifier();
        Nullability nullability = instance.nullable();
        boolean synthetic = instance.synthetic();
        switch (between(0, 3)) {
            case 0 -> name = randomAlphaOfLength(name.length() + 1);
            case 1 -> qualifier = randomValueOtherThan(qualifier, () -> randomBoolean() ? null : randomAlphaOfLength(3));
            case 2 -> nullability = randomValueOtherThan(nullability, () -> randomFrom(Nullability.values()));
            case 3 -> synthetic = false == synthetic;
        }
        return new FieldAttribute(
            source,
            instance.parent(),
            name,
            instance.dataType(),
            instance.field(),
            qualifier,
            nullability,
            new NameId(),
            synthetic,
            null,
            null,
            null,
            null
        );
    }
}

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
import org.elasticsearch.xpack.esql.type.AbstractEsFieldTypeTests;

import static org.elasticsearch.xpack.esql.core.expression.FieldAttributeTestUtils.newFieldAttributeWithType;

public class FieldAttributeTests extends AbstractAttributeTestCase<FieldAttribute> {
    public static FieldAttribute createFieldAttribute(int maxDepth, boolean onlyRepresentable) {
        Source source = Source.EMPTY;
        String parentName = maxDepth == 0 || randomBoolean() ? null : randomAlphaOfLength(3);
        String name = randomAlphaOfLength(5);
        DataType type = onlyRepresentable
            ? randomValueOtherThanMany(t -> false == DataType.isRepresentable(t), () -> randomFrom(DataType.types()))
            : randomFrom(DataType.types());
        EsField field = AbstractEsFieldTypeTests.randomAnyEsField(maxDepth);
        Nullability nullability = randomFrom(Nullability.values());
        boolean synthetic = randomBoolean();
        return newFieldAttributeWithType(source, parentName, name, type, field, nullability, new NameId(), synthetic);
    }

    @Override
    protected FieldAttribute create() {
        return createFieldAttribute(3, false);
    }

    @Override
    protected FieldAttribute mutate(FieldAttribute instance) {
        Source source = instance.source();
        String parentName = instance.parentName();
        String name = instance.name();
        DataType type = instance.dataType();
        EsField field = instance.field();
        Nullability nullability = instance.nullable();
        boolean synthetic = instance.synthetic();
        switch (between(0, 5)) {
            case 0 -> parentName = randomValueOtherThan(parentName, () -> randomBoolean() ? null : randomAlphaOfLength(2));
            case 1 -> name = randomAlphaOfLength(name.length() + 1);
            case 2 -> type = randomValueOtherThan(type, () -> randomFrom(DataType.types()));
            case 3 -> field = randomValueOtherThan(field, () -> AbstractEsFieldTypeTests.randomAnyEsField(3));
            case 4 -> nullability = randomValueOtherThan(nullability, () -> randomFrom(Nullability.values()));
            case 5 -> synthetic = false == synthetic;
        }
        return newFieldAttributeWithType(source, parentName, name, type, field, nullability, new NameId(), synthetic);
    }
}

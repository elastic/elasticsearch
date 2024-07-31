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
import org.elasticsearch.xpack.esql.core.type.AbstractEsFieldTypeTests;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;

public class FieldAttributeTests extends AbstractAttributeTestCase<FieldAttribute> {
    public static FieldAttribute createFieldAttribute(int maxDepth, boolean onlyRepresentable) {
        Source source = Source.EMPTY;
        FieldAttribute parent = maxDepth == 0 || randomBoolean() ? null : createFieldAttribute(maxDepth - 1, onlyRepresentable);
        String name = randomAlphaOfLength(5);
        DataType type = onlyRepresentable
            ? randomValueOtherThanMany(t -> false == DataType.isRepresentable(t), () -> randomFrom(DataType.types()))
            : randomFrom(DataType.types());
        EsField field = AbstractEsFieldTypeTests.randomAnyEsField(maxDepth);
        String qualifier = randomBoolean() ? null : randomAlphaOfLength(3);
        Nullability nullability = randomFrom(Nullability.values());
        boolean synthetic = randomBoolean();
        return new FieldAttribute(source, parent, name, type, field, qualifier, nullability, new NameId(), synthetic);
    }

    @Override
    protected FieldAttribute create() {
        return createFieldAttribute(3, false);
    }

    @Override
    protected FieldAttribute mutate(FieldAttribute instance) {
        Source source = instance.source();
        FieldAttribute parent = instance.parent();
        String name = instance.name();
        DataType type = instance.dataType();
        EsField field = instance.field();
        String qualifier = instance.qualifier();
        Nullability nullability = instance.nullable();
        boolean synthetic = instance.synthetic();
        switch (between(0, 6)) {
            case 0 -> parent = randomValueOtherThan(parent, () -> randomBoolean() ? null : createFieldAttribute(2, false));
            case 1 -> name = randomAlphaOfLength(name.length() + 1);
            case 2 -> type = randomValueOtherThan(type, () -> randomFrom(DataType.types()));
            case 3 -> field = randomValueOtherThan(field, () -> AbstractEsFieldTypeTests.randomAnyEsField(3));
            case 4 -> qualifier = randomValueOtherThan(qualifier, () -> randomBoolean() ? null : randomAlphaOfLength(3));
            case 5 -> nullability = randomValueOtherThan(nullability, () -> randomFrom(Nullability.values()));
            case 6 -> synthetic = false == synthetic;
        }
        return new FieldAttribute(source, parent, name, type, field, qualifier, nullability, new NameId(), synthetic);
    }
}

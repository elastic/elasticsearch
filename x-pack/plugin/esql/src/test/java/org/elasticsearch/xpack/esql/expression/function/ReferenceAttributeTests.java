/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.AbstractNamedExpressionSerializationTests;

import java.util.function.Supplier;

public class ReferenceAttributeTests extends AbstractNamedExpressionSerializationTests<ReferenceAttribute> {
    public static ReferenceAttribute randomReferenceAttribute(boolean onlyRepresentable) {
        Source source = Source.EMPTY;
        String qualifier = randomBoolean() ? null : randomAlphaOfLength(3);
        String name = randomAlphaOfLength(5);
        Supplier<DataType> randomType = () -> randomValueOtherThanMany(
            t -> false == t.supportedVersion().supportedLocally(),
            () -> randomFrom(DataType.types())
        );
        DataType type = onlyRepresentable
            ? randomValueOtherThanMany(t -> false == DataType.isRepresentable(t), randomType)
            : randomType.get();
        Nullability nullability = randomFrom(Nullability.values());
        boolean synthetic = randomBoolean();
        return new ReferenceAttribute(source, qualifier, name, type, nullability, new NameId(), synthetic);
    }

    @Override
    protected ReferenceAttribute createTestInstance() {
        return randomReferenceAttribute(false);
    }

    @Override
    protected ReferenceAttribute mutateInstance(ReferenceAttribute instance) {
        Source source = instance.source();
        String qualifier = instance.qualifier();
        String name = instance.name();
        DataType type = instance.dataType();
        Nullability nullability = instance.nullable();
        NameId id = instance.id();
        boolean synthetic = instance.synthetic();
        switch (between(0, 5)) {
            case 0 -> qualifier = randomAlphaOfLength(qualifier == null ? 3 : qualifier.length() + 1);
            case 1 -> name = randomAlphaOfLength(name.length() + 1);
            case 2 -> type = randomValueOtherThan(type, () -> randomFrom(DataType.types()));
            case 3 -> nullability = randomValueOtherThan(nullability, () -> randomFrom(Nullability.values()));
            case 4 -> id = new NameId();
            case 5 -> synthetic = false == synthetic;
        }
        return new ReferenceAttribute(source, qualifier, name, type, nullability, id, synthetic);
    }

    @Override
    protected ReferenceAttribute mutateNameId(ReferenceAttribute instance) {
        return (ReferenceAttribute) instance.withId(new NameId());
    }

    @Override
    protected boolean equalityIgnoresId() {
        return false;
    }
}

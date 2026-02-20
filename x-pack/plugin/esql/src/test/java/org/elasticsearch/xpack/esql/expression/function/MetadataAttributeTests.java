/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.AbstractNamedExpressionSerializationTests;

public class MetadataAttributeTests extends AbstractNamedExpressionSerializationTests<MetadataAttribute> {
    @Override
    protected MetadataAttribute createTestInstance() {
        return randomMetadataAttribute();
    }

    public static MetadataAttribute randomMetadataAttribute() {
        Source source = Source.EMPTY;
        String name = randomAlphaOfLength(5);
        DataType type = randomValueOtherThanMany(t -> false == t.supportedVersion().supportedLocally(), () -> randomFrom(DataType.types()));
        Nullability nullability = randomFrom(Nullability.values());
        boolean synthetic = randomBoolean();
        boolean searchable = randomBoolean();
        return new MetadataAttribute(source, name, type, nullability, new NameId(), synthetic, searchable);
    }

    @Override
    protected MetadataAttribute mutateInstance(MetadataAttribute instance) {
        Source source = instance.source();
        String name = instance.name();
        DataType type = instance.dataType();
        Nullability nullability = instance.nullable();
        NameId id = instance.id();
        boolean synthetic = instance.synthetic();
        boolean searchable = instance.searchable();
        switch (between(0, 5)) {
            case 0 -> name = randomAlphaOfLength(name.length() + 1);
            case 1 -> type = randomValueOtherThan(type, () -> randomFrom(DataType.types()));
            case 2 -> nullability = randomValueOtherThan(nullability, () -> randomFrom(Nullability.values()));
            case 3 -> id = new NameId();
            case 4 -> synthetic = false == synthetic;
            case 5 -> searchable = false == searchable;
        }
        return new MetadataAttribute(source, name, type, nullability, id, synthetic, searchable);
    }

    @Override
    protected MetadataAttribute mutateNameId(MetadataAttribute instance) {
        return (MetadataAttribute) instance.withId(new NameId());
    }

    @Override
    protected boolean equalityIgnoresId() {
        return false;
    }
}

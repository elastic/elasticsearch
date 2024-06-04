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
import org.elasticsearch.xpack.esql.core.type.DataTypes;

public class MetadataAttributeTests extends AbstractAttributeTestCase<MetadataAttribute> {
    @Override
    protected MetadataAttribute create() {
        Source source = Source.EMPTY;
        String name = randomAlphaOfLength(5);
        DataType type = randomFrom(DataTypes.types());
        String qualifier = randomBoolean() ? null : randomAlphaOfLength(3);
        Nullability nullability = randomFrom(Nullability.values());
        boolean synthetic = randomBoolean();
        boolean searchable = randomBoolean();
        return new MetadataAttribute(source, name, type, qualifier, nullability, new NameId(), synthetic, searchable);
    }

    @Override
    protected MetadataAttribute mutate(MetadataAttribute instance) {
        Source source = instance.source();
        String name = instance.name();
        DataType type = instance.dataType();
        String qualifier = instance.qualifier();
        Nullability nullability = instance.nullable();
        boolean synthetic = instance.synthetic();
        boolean searchable = instance.searchable();
        switch (between(0, 5)) {
            case 0 -> name = randomAlphaOfLength(name.length() + 1);
            case 1 -> type = randomValueOtherThan(type, () -> randomFrom(DataTypes.types()));
            case 2 -> qualifier = randomValueOtherThan(qualifier, () -> randomBoolean() ? null : randomAlphaOfLength(3));
            case 3 -> nullability = randomValueOtherThan(nullability, () -> randomFrom(Nullability.values()));
            case 4 -> synthetic = false == synthetic;
            case 5 -> searchable = false == searchable;
        }
        return new MetadataAttribute(source, name, type, qualifier, nullability, new NameId(), synthetic, searchable);
    }
}

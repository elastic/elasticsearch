/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.UnsupportedEsField;
import org.elasticsearch.xpack.esql.core.type.UnsupportedEsFieldTests;

public class UnsupportedAttributeTests extends AbstractAttributeTestCase<UnsupportedAttribute> {
    @Override
    protected UnsupportedAttribute create() {
        return randomUnsupportedAttribute();
    }

    public static UnsupportedAttribute randomUnsupportedAttribute() {
        String name = randomAlphaOfLength(5);
        UnsupportedEsField field = UnsupportedEsFieldTests.randomUnsupportedEsField(4);
        String customMessage = randomBoolean() ? null : randomAlphaOfLength(9);
        NameId id = new NameId();
        return new UnsupportedAttribute(Source.EMPTY, name, field, customMessage, id);
    }

    @Override
    protected UnsupportedAttribute mutate(UnsupportedAttribute instance) {
        Source source = instance.source();
        String name = instance.name();
        UnsupportedEsField field = instance.field();
        String customMessage = instance.hasCustomMessage() ? instance.unresolvedMessage() : null;
        switch (between(0, 2)) {
            case 0 -> name = randomAlphaOfLength(name.length() + 1);
            case 1 -> field = randomValueOtherThan(field, () -> UnsupportedEsFieldTests.randomUnsupportedEsField(4));
            case 2 -> customMessage = randomValueOtherThan(customMessage, () -> randomBoolean() ? null : randomAlphaOfLength(9));
            default -> throw new IllegalArgumentException();
        }
        return new UnsupportedAttribute(source, name, field, customMessage, new NameId());
    }
}

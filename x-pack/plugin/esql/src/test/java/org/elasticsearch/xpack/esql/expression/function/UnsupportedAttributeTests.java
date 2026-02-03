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
import org.elasticsearch.xpack.esql.expression.AbstractNamedExpressionSerializationTests;
import org.elasticsearch.xpack.esql.type.UnsupportedEsFieldTests;

public class UnsupportedAttributeTests extends AbstractNamedExpressionSerializationTests<UnsupportedAttribute> {
    @Override
    protected UnsupportedAttribute createTestInstance() {
        return randomUnsupportedAttribute();
    }

    public static UnsupportedAttribute randomUnsupportedAttribute() {
        String qualifier = randomBoolean() ? null : randomAlphaOfLength(3);
        String name = randomAlphaOfLength(5);
        UnsupportedEsField field = UnsupportedEsFieldTests.randomUnsupportedEsField(4);
        String customMessage = randomBoolean() ? null : randomAlphaOfLength(9);
        NameId id = new NameId();
        return new UnsupportedAttribute(Source.EMPTY, qualifier, name, field, customMessage, id);
    }

    @Override
    protected UnsupportedAttribute mutateInstance(UnsupportedAttribute instance) {
        Source source = instance.source();
        String qualifier = instance.qualifier();
        String name = instance.name();
        UnsupportedEsField field = instance.field();
        String customMessage = instance.hasCustomMessage() ? instance.unresolvedMessage() : null;
        NameId id = instance.id();
        switch (between(0, 4)) {
            case 0 -> qualifier = randomAlphaOfLength(qualifier == null ? 3 : qualifier.length() + 1);
            case 1 -> name = randomAlphaOfLength(name.length() + 1);
            case 2 -> field = randomValueOtherThan(field, () -> UnsupportedEsFieldTests.randomUnsupportedEsField(4));
            case 3 -> customMessage = randomValueOtherThan(customMessage, () -> randomBoolean() ? null : randomAlphaOfLength(9));
            case 4 -> id = new NameId();
        }
        return new UnsupportedAttribute(source, qualifier, name, field, customMessage, id);
    }

    @Override
    protected UnsupportedAttribute mutateNameId(UnsupportedAttribute instance) {
        return (UnsupportedAttribute) instance.withId(new NameId());
    }

    @Override
    protected boolean equalityIgnoresId() {
        return false;
    }
}

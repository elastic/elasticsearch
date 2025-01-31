/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class FirstValueSerializationTests extends AbstractExpressionSerializationTests<FirstValue> {

    @Override
    protected FirstValue createTestInstance() {
        return new FirstValue(randomSource(), randomChild(), randomBoolean() ? randomChild() : null);
    }

    @Override
    protected FirstValue mutateInstance(FirstValue instance) throws IOException {
        var source = instance.source();
        var field = instance.field();
        var precision = instance.by();
        if (randomBoolean()) {
            field = mutateExpression(field);
        } else {
            precision = mutateNullableExpression(precision);
        }
        return new FirstValue(source, field, precision);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}

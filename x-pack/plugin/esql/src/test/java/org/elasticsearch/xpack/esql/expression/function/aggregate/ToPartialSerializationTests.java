/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class ToPartialSerializationTests extends AbstractExpressionSerializationTests<ToPartial> {
    @Override
    protected ToPartial createTestInstance() {
        return new ToPartial(randomSource(), randomChild(), randomChild(), randomChild(), randomChild());
    }

    @Override
    protected ToPartial mutateInstance(ToPartial instance) throws IOException {
        Expression field = instance.field();
        Expression filter = instance.filter();
        Expression window = instance.window();
        Expression function = instance.function();
        switch (randomIntBetween(0, 3)) {
            case 0 -> field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
            case 1 -> filter = randomValueOtherThan(filter, AbstractExpressionSerializationTests::randomChild);
            case 2 -> window = randomValueOtherThan(window, AbstractExpressionSerializationTests::randomChild);
            case 3 -> function = randomValueOtherThan(function, AbstractExpressionSerializationTests::randomChild);
            default -> throw new AssertionError("unexpected value");
        }
        return new ToPartial(instance.source(), field, filter, window, function);
    }
}

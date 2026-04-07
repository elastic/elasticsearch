/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;

import java.io.IOException;

public abstract class AbstractUnaryScalarSerializationTests<T extends UnaryScalarFunction> extends AbstractExpressionSerializationTests<T> {
    protected abstract T create(Source source, Expression child);

    @Override
    protected final T createTestInstance() {
        return create(randomSource(), randomChild());
    }

    @Override
    protected final T mutateInstance(T instance) throws IOException {
        Expression child = randomValueOtherThan(instance.field(), AbstractExpressionSerializationTests::randomChild);
        return create(instance.source(), child);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public abstract class AbstractBinarySpatialFunctionSerializationTestCase<T extends BinarySpatialFunction> extends
    AbstractExpressionSerializationTests<T> {

    protected abstract T build(Source source, Expression left, Expression right);

    @Override
    protected final T createTestInstance() {
        Source source = randomSource();
        Expression left = randomChild();
        Expression right = randomChild();
        return build(source, left, right);
    }

    @Override
    protected final T mutateInstance(T instance) throws IOException {
        Source source = instance.source();
        Expression left = instance.left();
        Expression right = instance.right();
        if (randomBoolean()) {
            left = randomValueOtherThan(left, AbstractExpressionSerializationTests::randomChild);
        } else {
            right = randomValueOtherThan(right, AbstractExpressionSerializationTests::randomChild);
        }
        return build(source, left, right);
    }
}

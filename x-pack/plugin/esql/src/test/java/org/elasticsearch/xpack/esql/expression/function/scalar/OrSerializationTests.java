/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;
import java.util.List;

public class OrSerializationTests extends AbstractExpressionSerializationTests<Or> {
    @Override
    protected List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return EsqlScalarFunction.getNamedWriteables();
    }

    @Override
    protected Or createTestInstance() {
        Source source = randomSource();
        Expression left = randomChild();
        Expression right = randomChild();
        return new Or(source, left, right);
    }

    @Override
    protected Or mutateInstance(Or instance) throws IOException {
        Source source = instance.source();
        Expression left = instance.left();
        Expression right = instance.right();
        if (randomBoolean()) {
            left = randomValueOtherThan(left, AbstractExpressionSerializationTests::randomChild);
        } else {
            right = randomValueOtherThan(right, AbstractExpressionSerializationTests::randomChild);
        }
        return new Or(source, left, right);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}

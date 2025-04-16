/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;

public abstract class AbstractVarargsSerializationTests<T extends Expression> extends AbstractExpressionSerializationTests<T> {
    protected abstract T create(Source source, Expression first, List<Expression> rest);

    @Override
    protected final T createTestInstance() {
        Source source = randomSource();
        Expression first = randomChild();
        List<Expression> rest = randomList(0, 10, AbstractExpressionSerializationTests::randomChild);
        return create(source, first, rest);
    }

    @Override
    protected final T mutateInstance(T instance) throws IOException {
        Source source = instance.source();
        Expression first = instance.children().get(0);
        List<Expression> rest = instance.children().subList(1, instance.children().size());
        if (randomBoolean()) {
            first = randomValueOtherThan(first, AbstractExpressionSerializationTests::randomChild);
        } else {
            rest = randomValueOtherThan(rest, () -> randomList(0, 10, AbstractExpressionSerializationTests::randomChild));
        }
        return create(instance.source(), first, rest);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;
import java.util.List;

public class InSerializationTests extends AbstractExpressionSerializationTests<In> {
    @Override
    protected In createTestInstance() {
        Source source = randomSource();
        Expression value = randomChild();
        List<Expression> list = randomList(10, AbstractExpressionSerializationTests::randomChild);
        return new In(source, value, list);
    }

    @Override
    protected In mutateInstance(In instance) throws IOException {
        Source source = instance.source();
        Expression value = instance.value();
        List<Expression> list = instance.list();
        if (randomBoolean()) {
            value = randomValueOtherThan(value, AbstractExpressionSerializationTests::randomChild);
        } else {
            list = randomValueOtherThan(list, () -> randomList(10, AbstractExpressionSerializationTests::randomChild));
        }
        return new In(source, value, list);
    }
}

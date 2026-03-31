/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class MvSortSerializationTests extends AbstractExpressionSerializationTests<MvSort> {
    @Override
    protected MvSort createTestInstance() {
        Source source = randomSource();
        Expression field = randomChild();
        Expression order = randomBoolean() ? null : randomChild();
        return new MvSort(source, field, order);
    }

    @Override
    protected MvSort mutateInstance(MvSort instance) throws IOException {
        Source source = instance.source();
        Expression field = instance.field();
        Expression order = instance.order();
        if (randomBoolean()) {
            field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
        } else {
            order = randomValueOtherThan(order, () -> randomBoolean() ? null : randomChild());
        }
        return new MvSort(source, field, order);
    }
}

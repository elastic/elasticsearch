/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;
import org.elasticsearch.xpack.esql.expression.AbstractUnaryScalarSerializationTests;

import java.io.IOException;

public class ClampMinSerializationTests extends AbstractExpressionSerializationTests<ClampMin> {
    @Override
    protected ClampMin createTestInstance() {
        Source source = randomSource();
        Expression field = randomChild();
        Expression min = randomChild();
        return new ClampMin(source, field, min);
    }

    @Override
    protected ClampMin mutateInstance(ClampMin instance) throws IOException {
        Source source = instance.source();
        Expression field = instance.children().get(0);
        Expression min = instance.children().get(1);
        if (randomBoolean()) {
            field = randomValueOtherThan(field, AbstractUnaryScalarSerializationTests::randomChild);
        } else {
            min = randomValueOtherThan(min, AbstractUnaryScalarSerializationTests::randomChild);
        }
        return new ClampMin(source, field, min);
    }
}

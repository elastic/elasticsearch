/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;
import org.elasticsearch.xpack.esql.expression.AbstractUnaryScalarSerializationTests;

import java.io.IOException;

public class Atan2SerializationTests extends AbstractExpressionSerializationTests<Atan2> {
    @Override
    protected Atan2 createTestInstance() {
        Source source = randomSource();
        Expression y = randomChild();
        Expression x = randomChild();
        return new Atan2(source, y, x);
    }

    @Override
    protected Atan2 mutateInstance(Atan2 instance) throws IOException {
        Source source = instance.source();
        Expression y = instance.y();
        Expression x = instance.x();
        if (randomBoolean()) {
            y = randomValueOtherThan(y, AbstractUnaryScalarSerializationTests::randomChild);
        } else {
            x = randomValueOtherThan(x, AbstractUnaryScalarSerializationTests::randomChild);
        }
        return new Atan2(source, y, x);
    }
}

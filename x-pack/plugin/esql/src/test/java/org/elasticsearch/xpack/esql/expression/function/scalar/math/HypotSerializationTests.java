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

public class HypotSerializationTests extends AbstractExpressionSerializationTests<Hypot> {

    @Override
    protected Hypot createTestInstance() {
        Source source = randomSource();
        Expression n1 = randomChild();
        Expression n2 = randomChild();
        return new Hypot(source, n1, n2);
    }

    @Override
    protected Hypot mutateInstance(Hypot instance) throws IOException {
        Source source = instance.source();
        Expression n1 = instance.n1();
        Expression n2 = instance.n2();
        if (randomBoolean()) {
            n1 = randomValueOtherThan(n1, AbstractUnaryScalarSerializationTests::randomChild);
        } else {
            n2 = randomValueOtherThan(n2, AbstractUnaryScalarSerializationTests::randomChild);
        }
        return new Hypot(source, n1, n2);
    }
}

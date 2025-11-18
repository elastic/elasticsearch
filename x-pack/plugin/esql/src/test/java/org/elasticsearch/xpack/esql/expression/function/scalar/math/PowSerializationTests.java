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

import java.io.IOException;

public class PowSerializationTests extends AbstractExpressionSerializationTests<Pow> {
    @Override
    protected Pow createTestInstance() {
        Source source = randomSource();
        Expression base = randomChild();
        Expression exponent = randomChild();
        return new Pow(source, base, exponent);
    }

    @Override
    protected Pow mutateInstance(Pow instance) throws IOException {
        Source source = instance.source();
        Expression base = instance.base();
        Expression exponent = instance.exponent();
        if (randomBoolean()) {
            base = randomValueOtherThan(base, AbstractExpressionSerializationTests::randomChild);
        } else {
            exponent = randomValueOtherThan(exponent, AbstractExpressionSerializationTests::randomChild);
        }
        return new Pow(source, base, exponent);
    }
}

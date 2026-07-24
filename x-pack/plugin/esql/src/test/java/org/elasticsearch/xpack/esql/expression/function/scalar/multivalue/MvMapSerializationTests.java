/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class MvMapSerializationTests extends AbstractExpressionSerializationTests<MvMap> {
    @Override
    protected MvMap createTestInstance() {
        return new MvMap(randomSource(), randomChild(), AnyMatchSerializationTests.randomLambda());
    }

    @Override
    protected MvMap mutateInstance(MvMap instance) throws IOException {
        Expression field = instance.field();
        Expression lambda = instance.children().get(1);
        if (randomBoolean()) {
            field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
        } else {
            lambda = randomValueOtherThan(lambda, AnyMatchSerializationTests::randomLambda);
        }
        return new MvMap(instance.source(), field, lambda);
    }
}

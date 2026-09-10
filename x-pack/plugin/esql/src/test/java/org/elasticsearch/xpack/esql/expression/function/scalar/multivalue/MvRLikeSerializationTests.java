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

public class MvRLikeSerializationTests extends AbstractExpressionSerializationTests<MvRLike> {
    @Override
    protected MvRLike createTestInstance() {
        return new MvRLike(randomSource(), randomChild(), randomChild());
    }

    @Override
    protected MvRLike mutateInstance(MvRLike instance) throws IOException {
        Expression field = instance.left();
        Expression pattern = instance.right();
        if (randomBoolean()) {
            field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
        } else {
            pattern = randomValueOtherThan(pattern, AbstractExpressionSerializationTests::randomChild);
        }
        return new MvRLike(instance.source(), field, pattern);
    }
}

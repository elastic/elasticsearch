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

/**
 * There is no TransportVersion for {@code mv_like} — a new function is a new NamedWriteable entry and nothing more —
 * so this round trip is the whole cross-node story.
 */
public class MvLikeSerializationTests extends AbstractExpressionSerializationTests<MvLike> {
    @Override
    protected MvLike createTestInstance() {
        return new MvLike(randomSource(), randomChild(), randomChild());
    }

    @Override
    protected MvLike mutateInstance(MvLike instance) throws IOException {
        Expression field = instance.left();
        Expression pattern = instance.right();
        if (randomBoolean()) {
            field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
        } else {
            pattern = randomValueOtherThan(pattern, AbstractExpressionSerializationTests::randomChild);
        }
        return new MvLike(instance.source(), field, pattern);
    }
}

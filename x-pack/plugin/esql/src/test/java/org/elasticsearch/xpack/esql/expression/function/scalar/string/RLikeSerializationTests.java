/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;

import java.io.IOException;

public class RLikeSerializationTests extends AbstractExpressionSerializationTests<RLike> {
    @Override
    protected RLike createTestInstance() {
        Source source = randomSource();
        Expression child = randomChild();
        RLikePattern pattern = new RLikePattern(randomAlphaOfLength(4));
        return new RLike(source, child, pattern);
    }

    @Override
    protected RLike mutateInstance(RLike instance) throws IOException {
        Source source = instance.source();
        Expression child = instance.field();
        RLikePattern pattern = instance.pattern();
        if (randomBoolean()) {
            child = randomValueOtherThan(child, AbstractExpressionSerializationTests::randomChild);
        } else {
            pattern = randomValueOtherThan(pattern, () -> new RLikePattern(randomAlphaOfLength(4)));
        }
        return new RLike(source, child, pattern);
    }
}

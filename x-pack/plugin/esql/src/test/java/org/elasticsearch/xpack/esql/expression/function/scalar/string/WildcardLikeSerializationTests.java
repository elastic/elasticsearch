/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;

import java.io.IOException;

public class WildcardLikeSerializationTests extends AbstractExpressionSerializationTests<WildcardLike> {
    @Override
    protected WildcardLike createTestInstance() {
        Source source = randomSource();
        Expression child = randomChild();
        WildcardPattern pattern = new WildcardPattern(randomAlphaOfLength(4));
        return new WildcardLike(source, child, pattern);
    }

    @Override
    protected WildcardLike mutateInstance(WildcardLike instance) throws IOException {
        Source source = instance.source();
        Expression child = instance.field();
        WildcardPattern pattern = instance.pattern();
        if (randomBoolean()) {
            child = randomValueOtherThan(child, AbstractExpressionSerializationTests::randomChild);
        } else {
            pattern = randomValueOtherThan(pattern, () -> new WildcardPattern(randomAlphaOfLength(4)));
        }
        return new WildcardLike(source, child, pattern);
    }
}

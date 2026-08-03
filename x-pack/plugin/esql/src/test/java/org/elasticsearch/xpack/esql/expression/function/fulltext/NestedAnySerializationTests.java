/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

/**
 * Round-trips the custom {@code writeTo}/{@code readFrom} of {@link NestedAny} (field + predicate). The
 * {@code queryBuilder} is left null: {@link FullTextFunction#equals} compares it by identity, so a non-null
 * builder would never survive an equality-based round-trip (the other full-text serialization tests do the
 * same).
 */
public class NestedAnySerializationTests extends AbstractExpressionSerializationTests<NestedAny> {
    @Override
    protected NestedAny createTestInstance() {
        return new NestedAny(randomSource(), randomChild(), randomChild(), null);
    }

    @Override
    protected NestedAny mutateInstance(NestedAny instance) throws IOException {
        Expression field = instance.field();
        Expression predicate = instance.predicate();
        if (randomBoolean()) {
            field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
        } else {
            predicate = randomValueOtherThan(predicate, AbstractExpressionSerializationTests::randomChild);
        }
        return new NestedAny(instance.source(), field, predicate, null);
    }
}

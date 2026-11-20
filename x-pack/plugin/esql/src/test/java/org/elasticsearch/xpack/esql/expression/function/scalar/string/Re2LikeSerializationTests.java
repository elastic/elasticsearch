/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.Re2Pattern;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.Re2Like;

import java.io.IOException;

public class Re2LikeSerializationTests extends AbstractExpressionSerializationTests<Re2Like> {
    @Override
    protected Re2Like createTestInstance() {
        return new Re2Like(randomSource(), randomChild(), new Re2Pattern(randomAlphaOfLength(4)));
    }

    @Override
    protected Re2Like mutateInstance(Re2Like instance) throws IOException {
        Expression child = instance.field();
        Re2Pattern pattern = instance.pattern();
        if (randomBoolean()) {
            child = randomValueOtherThan(child, AbstractExpressionSerializationTests::randomChild);
        } else {
            pattern = randomValueOtherThan(pattern, () -> new Re2Pattern(randomAlphaOfLength(4)));
        }
        return new Re2Like(instance.source(), child, pattern);
    }
}

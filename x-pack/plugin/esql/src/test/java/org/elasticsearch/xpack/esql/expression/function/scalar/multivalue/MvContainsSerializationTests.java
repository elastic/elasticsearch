/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class MvContainsSerializationTests extends AbstractExpressionSerializationTests<MvContains> {
    @Override
    protected MvContains createTestInstance() {
        Source source = randomSource();
        Expression field1 = randomChild();
        Expression field2 = randomChild();
        return new MvContains(source, field1, field2);
    }

    @Override
    protected MvContains mutateInstance(MvContains instance) throws IOException {
        Source source = instance.source();
        Expression left = instance.left();
        Expression right = instance.right();
        return switch (between(0, 1)) {
            case 0 -> new MvContains(source, randomValueOtherThan(left, AbstractExpressionSerializationTests::randomChild), right);
            case 1 -> new MvContains(source, left, randomValueOtherThan(right, AbstractExpressionSerializationTests::randomChild));
            default -> throw new AssertionError("illegal mutate branch");
        };
    }
}

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

public class MvDifferenceSerializationTests extends AbstractExpressionSerializationTests<MvDifference> {
    @Override
    protected MvDifference createTestInstance() {
        Source source = randomSource();
        Expression field1 = randomChild();
        Expression field2 = randomChild();
        return new MvDifference(source, field1, field2);
    }

    @Override
    protected MvDifference mutateInstance(MvDifference instance) throws IOException {
        Source source = instance.source();
        Expression left = instance.left();
        Expression right = instance.right();
        if (randomBoolean()) {
            left = randomValueOtherThan(left, AbstractExpressionSerializationTests::randomChild);
        } else {
            right = randomValueOtherThan(right, AbstractExpressionSerializationTests::randomChild);
        }
        return new MvDifference(source, left, right);
    }
}

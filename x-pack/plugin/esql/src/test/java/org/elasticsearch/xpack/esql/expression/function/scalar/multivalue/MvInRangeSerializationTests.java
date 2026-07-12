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

public class MvInRangeSerializationTests extends AbstractExpressionSerializationTests<MvInRange> {
    @Override
    protected MvInRange createTestInstance() {
        Source source = randomSource();
        return new MvInRange(source, randomChild(), randomChild(), randomChild());
    }

    @Override
    protected MvInRange mutateInstance(MvInRange instance) throws IOException {
        Expression field = instance.field();
        Expression lower = instance.lower();
        Expression upper = instance.upper();
        switch (between(0, 2)) {
            case 0 -> field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
            case 1 -> lower = randomValueOtherThan(lower, AbstractExpressionSerializationTests::randomChild);
            default -> upper = randomValueOtherThan(upper, AbstractExpressionSerializationTests::randomChild);
        }
        return new MvInRange(instance.source(), field, lower, upper);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;
import org.elasticsearch.xpack.esql.expression.AbstractUnaryScalarSerializationTests;

import java.io.IOException;
import java.util.List;

public class TimeSeriesWithoutSerializationTests extends AbstractExpressionSerializationTests<TimeSeriesWithout> {
    @Override
    protected TimeSeriesWithout createTestInstance() {
        return new TimeSeriesWithout(randomSource(), randomChildren());
    }

    @Override
    protected TimeSeriesWithout mutateInstance(TimeSeriesWithout instance) throws IOException {
        List<Expression> children = randomValueOtherThan(instance.children(), TimeSeriesWithoutSerializationTests::randomChildren);
        return new TimeSeriesWithout(instance.source(), children);
    }

    private static List<Expression> randomChildren() {
        return randomList(0, 3, AbstractUnaryScalarSerializationTests::randomChild);
    }
}

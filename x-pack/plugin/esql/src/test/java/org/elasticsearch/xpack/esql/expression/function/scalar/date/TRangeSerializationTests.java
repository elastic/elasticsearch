/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class TRangeSerializationTests extends AbstractExpressionSerializationTests<TRange> {
    @Override
    protected TRange createTestInstance() {
        Source source = randomSource();
        Expression timestamp = randomChild();
        Expression startTime = randomChild();
        Expression endTime = randomChild();
        return new TRange(source, timestamp, startTime, endTime);
    }

    @Override
    protected TRange mutateInstance(TRange instance) throws IOException {
        Source source = instance.source();
        Expression timestamp = randomChild();
        Expression startTime = instance.getStartTime();
        Expression endTime = instance.getEndTime();
        if (randomBoolean()) {
            startTime = randomValueOtherThan(startTime, AbstractExpressionSerializationTests::randomChild);
        } else {
            endTime = randomValueOtherThan(endTime, AbstractExpressionSerializationTests::randomChild);
        }
        return new TRange(source, timestamp, startTime, endTime);
    }
}

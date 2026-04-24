/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class FirstOverTimeSerializationTests extends AbstractExpressionSerializationTests<FirstOverTime> {
    @Override
    protected FirstOverTime createTestInstance() {
        Source source = randomSource();
        Expression field = randomChild();
        Expression filter = randomChild();
        Expression window = randomChild();
        Expression timestamp = randomChild();
        return new FirstOverTime(source, field, filter, window, timestamp);
    }

    @Override
    protected FirstOverTime mutateInstance(FirstOverTime instance) throws IOException {
        Source source = instance.source();
        Expression field = instance.field();
        Expression filter = instance.filter();
        Expression window = instance.window();
        Expression timestamp = instance.timestamp();
        switch (between(0, 3)) {
            case 0 -> field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
            case 1 -> filter = randomValueOtherThan(filter, AbstractExpressionSerializationTests::randomChild);
            case 2 -> window = randomValueOtherThan(window, AbstractExpressionSerializationTests::randomChild);
            case 3 -> timestamp = randomValueOtherThan(timestamp, AbstractExpressionSerializationTests::randomChild);
        }
        return new FirstOverTime(source, field, filter, window, timestamp);
    }
}

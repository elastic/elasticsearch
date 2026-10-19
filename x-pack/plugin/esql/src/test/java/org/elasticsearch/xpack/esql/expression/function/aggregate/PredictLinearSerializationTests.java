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

public class PredictLinearSerializationTests extends AbstractExpressionSerializationTests<PredictLinear> {
    @Override
    protected PredictLinear createTestInstance() {
        Source source = randomSource();
        Expression field = randomChild();
        Expression timestamp = randomChild();
        Expression filter = randomChild();
        Expression window = randomChild();
        return new PredictLinear(source, field, timestamp, filter, window, randomChild());
    }

    @Override
    protected PredictLinear mutateInstance(PredictLinear instance) throws IOException {
        Source source = instance.source();
        Expression field = instance.field();
        Expression timestamp = instance.timestamp();
        Expression filter = instance.filter();
        Expression window = instance.window();
        Expression horizon = instance.horizon();
        switch (between(0, 4)) {
            case 0 -> field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
            case 1 -> timestamp = randomValueOtherThan(timestamp, AbstractExpressionSerializationTests::randomChild);
            case 2 -> filter = randomValueOtherThan(filter, AbstractExpressionSerializationTests::randomChild);
            case 3 -> window = randomValueOtherThan(window, AbstractExpressionSerializationTests::randomChild);
            case 4 -> horizon = randomValueOtherThan(horizon, AbstractExpressionSerializationTests::randomChild);
        }
        return new PredictLinear(source, field, timestamp, filter, window, horizon);
    }

}

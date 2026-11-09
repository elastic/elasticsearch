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

public class WindowFunnelLevelSerializationTests extends AbstractExpressionSerializationTests<WindowFunnelLevel> {
    @Override
    protected WindowFunnelLevel createTestInstance() {
        return new WindowFunnelLevel(
            randomSource(),
            randomChild(),
            randomChild(),
            randomChild(),
            AggregateFunction.NO_WINDOW,
            randomChild(),
            randomChild()
        );
    }

    @Override
    protected WindowFunnelLevel mutateInstance(WindowFunnelLevel instance) throws IOException {
        Source source = instance.source();
        Expression timestamp = instance.timestamp();
        Expression mask = instance.mask();
        Expression filter = instance.filter();
        Expression maxspan = instance.maxspan();
        Expression steps = instance.steps();
        switch (between(0, 4)) {
            case 0 -> timestamp = randomValueOtherThan(timestamp, AbstractExpressionSerializationTests::randomChild);
            case 1 -> mask = randomValueOtherThan(mask, AbstractExpressionSerializationTests::randomChild);
            case 2 -> filter = randomValueOtherThan(filter, AbstractExpressionSerializationTests::randomChild);
            case 3 -> maxspan = randomValueOtherThan(maxspan, AbstractExpressionSerializationTests::randomChild);
            case 4 -> steps = randomValueOtherThan(steps, AbstractExpressionSerializationTests::randomChild);
        }
        return new WindowFunnelLevel(source, timestamp, mask, filter, instance.window(), maxspan, steps);
    }
}

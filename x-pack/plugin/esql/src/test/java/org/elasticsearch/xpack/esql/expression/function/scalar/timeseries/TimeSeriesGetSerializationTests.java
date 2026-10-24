/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.util.ArrayList;

public class TimeSeriesGetSerializationTests extends AbstractExpressionSerializationTests<TimeSeriesGet> {
    @Override
    protected TimeSeriesGet createTestInstance() {
        return new TimeSeriesGet(randomSource(), randomChild(), randomChild());
    }

    @Override
    protected TimeSeriesGet mutateInstance(TimeSeriesGet instance) {
        var children = new ArrayList<>(instance.children());
        int index = randomInt(children.size() - 1);
        children.set(index, mutateExpression(children.get(index)));
        return instance.replaceChildren(children);
    }
}

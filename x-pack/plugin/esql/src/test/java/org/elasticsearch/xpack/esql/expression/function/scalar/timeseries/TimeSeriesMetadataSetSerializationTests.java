/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.util.ArrayList;
import java.util.List;

public class TimeSeriesMetadataSetSerializationTests extends AbstractExpressionSerializationTests<TimeSeriesMetadataSet> {
    @Override
    protected TimeSeriesMetadataSet createTestInstance() {
        return new TimeSeriesMetadataSet(randomSource(), randomChild(), randomChild(), randomChild(), randomPath());
    }

    @Override
    protected TimeSeriesMetadataSet mutateInstance(TimeSeriesMetadataSet instance) {
        var children = new ArrayList<>(instance.children());
        int index = randomInt(children.size());
        if (index == children.size()) {
            List<String> path = randomValueOtherThan(instance.path(), TimeSeriesMetadataSetSerializationTests::randomPath);
            return new TimeSeriesMetadataSet(instance.source(), children.get(0), children.get(1), children.get(2), path);
        }
        children.set(index, mutateExpression(children.get(index)));
        return instance.replaceChildren(children);
    }

    private static List<String> randomPath() {
        return randomList(1, 3, () -> randomAlphaOfLength(5));
    }
}

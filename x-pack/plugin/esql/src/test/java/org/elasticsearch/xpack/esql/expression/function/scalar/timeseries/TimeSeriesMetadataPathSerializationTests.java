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

public class TimeSeriesMetadataPathSerializationTests extends AbstractExpressionSerializationTests<TimeSeriesMetadataPath> {
    @Override
    protected TimeSeriesMetadataPath createTestInstance() {
        return new TimeSeriesMetadataPath(randomSource(), randomChild(), randomChild(), randomPaths());
    }

    @Override
    protected TimeSeriesMetadataPath mutateInstance(TimeSeriesMetadataPath instance) {
        var children = new ArrayList<>(instance.children());
        int index = randomInt(children.size());
        if (index == children.size()) {
            List<String> paths = randomValueOtherThan(instance.paths(), TimeSeriesMetadataPathSerializationTests::randomPaths);
            return new TimeSeriesMetadataPath(instance.source(), children.get(0), children.get(1), paths);
        }
        children.set(index, mutateExpression(children.get(index)));
        return instance.replaceChildren(children);
    }

    static List<String> randomPaths() {
        return randomList(0, 3, () -> randomAlphaOfLength(5));
    }
}

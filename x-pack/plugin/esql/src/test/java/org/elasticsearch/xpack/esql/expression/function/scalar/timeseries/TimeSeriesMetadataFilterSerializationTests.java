/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;
import java.util.List;

public class TimeSeriesMetadataFilterSerializationTests extends AbstractExpressionSerializationTests<TimeSeriesMetadataFilter> {
    @Override
    protected TimeSeriesMetadataFilter createTestInstance() {
        return new TimeSeriesMetadataFilter(
            randomSource(),
            randomChild(),
            randomList(0, 10, AbstractExpressionSerializationTests::randomChild),
            randomBoolean()
        );
    }

    @Override
    protected TimeSeriesMetadataFilter mutateInstance(TimeSeriesMetadataFilter instance) throws IOException {
        Source source = instance.source();
        Expression labelContainer = instance.labelContainer();
        List<Expression> labels = instance.labels();
        boolean exclude = instance.exclude();
        switch (between(0, 2)) {
            case 0 -> labelContainer = randomValueOtherThan(labelContainer, AbstractExpressionSerializationTests::randomChild);
            case 1 -> labels = randomValueOtherThan(labels, () -> randomList(0, 10, AbstractExpressionSerializationTests::randomChild));
            case 2 -> exclude = exclude == false;
            default -> throw new AssertionError();
        }
        return new TimeSeriesMetadataFilter(source, labelContainer, labels, exclude);
    }
}

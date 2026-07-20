/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.compute.aggregation.QuantileStates;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class PercentileSerializationTests extends AbstractExpressionSerializationTests<Percentile> {
    @Override
    protected Percentile createTestInstance() {
        Source source = randomSource();
        Expression field = randomChild();
        Expression percentile = randomChild();
        double compression = randomCompression();
        return new Percentile(source, field, Literal.TRUE, AggregateFunction.NO_WINDOW, percentile, compression);
    }

    @Override
    protected Percentile mutateInstance(Percentile instance) throws IOException {
        Source source = instance.source();
        Expression field = instance.field();
        Expression percentile = instance.percentile();
        double compression = instance.compression();
        switch (between(0, 2)) {
            case 0 -> field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
            case 1 -> percentile = randomValueOtherThan(percentile, AbstractExpressionSerializationTests::randomChild);
            case 2 -> compression = randomValueOtherThan(compression, PercentileSerializationTests::randomCompression);
        }
        return new Percentile(source, field, Literal.TRUE, AggregateFunction.NO_WINDOW, percentile, compression);
    }

    private static double randomCompression() {
        // Small discrete set so randomValueOtherThan converges quickly,
        // spanning the meaningful range including DEFAULT_COMPRESSION.
        return randomFrom(50.0, 100.0, 200.0, QuantileStates.DEFAULT_COMPRESSION);
    }
}

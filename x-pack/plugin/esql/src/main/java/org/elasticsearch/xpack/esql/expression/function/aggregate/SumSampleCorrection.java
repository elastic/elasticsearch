/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;

public class SumSampleCorrection extends Sum {

    private final Expression sampleProbability;

    public SumSampleCorrection(Source source, Expression field, Expression filter, Expression sampleProbability) {
        super(source, field, filter);
        this.sampleProbability = sampleProbability;
    }

    @Override
    public Expression surrogate() {
        return switch (dataType()) {
            case DOUBLE -> new Div(source(), new Sum(source(), field(), filter()), sampleProbability);
            case LONG -> new ToLong(source(), new Div(source(), new Sum(source(), field(), filter()), sampleProbability));
            default -> throw new IllegalStateException("unexpected data type [" + dataType() + "]");
        };
    }
}

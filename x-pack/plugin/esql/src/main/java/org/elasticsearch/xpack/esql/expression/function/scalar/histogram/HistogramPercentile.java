/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.histogram;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramQuantile;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cast;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

public class HistogramPercentile extends EsqlScalarFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "HistogramPercentile",
        HistogramPercentile::new
    );

    private final Expression histogram;
    private final Expression percentile;

    @FunctionInfo(returnType = { "double" })
    public HistogramPercentile(
        Source source,
        @Param(name = "histogram", type = { "exponential_histogram" }) Expression histogram,
        @Param(name = "percentile", type = { "double", "integer", "long", "unsigned_long" }) Expression percentile
    ) {
        super(source, List.of(histogram, percentile));
        this.histogram = histogram;
        this.percentile = percentile;
    }

    private HistogramPercentile(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    protected TypeResolution resolveType() {
        return isType(histogram, dt -> dt == DataType.EXPONENTIAL_HISTOGRAM, sourceText(), DEFAULT, "exponential_histogram").and(
            isType(percentile, DataType::isNumeric, sourceText(), DEFAULT, "numeric types")
        );
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new HistogramPercentile(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, HistogramPercentile::new, histogram, percentile);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(histogram);
        out.writeNamedWriteable(percentile);
    }

    @Evaluator
    static double process(ExponentialHistogram value, double percentile) {
        // TODO handle nans and out of bounds percentiles
        return ExponentialHistogramQuantile.getQuantile(value, percentile / 100.0);
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var fieldEvaluator = toEvaluator.apply(histogram);
        var percentileEvaluator = Cast.cast(source(), percentile.dataType(), DataType.DOUBLE, toEvaluator.apply(percentile));
        return new HistogramPercentileEvaluator.Factory(source(), fieldEvaluator, percentileEvaluator);
    }

}

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
import org.elasticsearch.compute.data.DoubleBlock;
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

/**
 * Extracts a percentile value from a single histogram value.
 * Note that this function is currently only intended for usage in surrogates and not available as a user-facing function.
 * Therefore, it is intentionally not registered in {@link org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry}.
 */
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

    Expression histogram() {
        return histogram;
    }

    Expression percentile() {
        return percentile;
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
    public boolean foldable() {
        return histogram.foldable() && percentile.foldable();
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

    @Evaluator(warnExceptions = ArithmeticException.class)
    static void process(DoubleBlock.Builder resultBuilder, ExponentialHistogram value, double percentile) {
        if (percentile < 0.0 || percentile > 100.0) {
            throw new ArithmeticException("Percentile value must be in the range [0, 100], got: " + percentile);
        }
        double result = ExponentialHistogramQuantile.getQuantile(value, percentile / 100.0);
        if (Double.isNaN(result)) { // can happen if the histogram is empty
            resultBuilder.appendNull();
        } else {
            resultBuilder.appendDouble(result);
        }
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var fieldEvaluator = toEvaluator.apply(histogram);
        var percentileEvaluator = Cast.cast(source(), percentile.dataType(), DataType.DOUBLE, toEvaluator.apply(percentile));
        return new HistogramPercentileEvaluator.Factory(source(), fieldEvaluator, percentileEvaluator);
    }
}

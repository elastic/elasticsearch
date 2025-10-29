/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.histogram;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ExponentialHistogramBlock;
import org.elasticsearch.compute.data.ExponentialHistogramBlockAccessor;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramQuantile;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
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

public class HistogramPercentile extends EsqlScalarFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "HistogramPercentile",
        HistogramPercentile::new
    );

    private final Expression histogram;
    private final Expression percentile;

    @FunctionInfo(returnType = { "double" })
    public HistogramPercentile(Source source,
                               @Param(
                                   name = "histogram",
                                   type = { "exponential_histogram" }
                               )
                               Expression histogram,
                               @Param(
                                   name = "percentile",
                                   type = { "double", "integer", "long", "unsigned_long" }
                               )
                               Expression percentile) {
        super(source, List.of(histogram, percentile));
        this.histogram = histogram;
        this.percentile = percentile;
    }

    private HistogramPercentile(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
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

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        DataType fieldDataType = histogram.dataType();
        if (fieldDataType != DataType.EXPONENTIAL_HISTOGRAM) {
            throw EsqlIllegalArgumentException.illegalDataType(fieldDataType);
        }
        var fieldEvaluator = toEvaluator.apply(histogram);
        var percentileEvaluator = Cast.cast(source(), percentile.dataType(), DataType.DOUBLE, toEvaluator.apply(percentile));

        return new EvalOperator.ExpressionEvaluator.Factory() {

            @Override
            public String toString() {
                return "HistogramPercentileEvaluator[" + "field=" + fieldEvaluator + ",percentile=" + percentileEvaluator + "]";
            }

            @Override
            public EvalOperator.ExpressionEvaluator get(DriverContext context) {
                var histogram = fieldEvaluator.get(context);
                var percentile = percentileEvaluator.get(context);
                return new Evaluator(source(), context, histogram, percentile);
            }
        };
    }

    private static class Evaluator implements EvalOperator.ExpressionEvaluator {

        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Evaluator.class);

        private final DriverContext driverContext;
        private final EvalOperator.ExpressionEvaluator histogram;
        private final EvalOperator.ExpressionEvaluator percentile;
        private final Source source;

        private Warnings warnings;

        private Evaluator(
            Source source,
            DriverContext driverContext,
            EvalOperator.ExpressionEvaluator histogram,
            EvalOperator.ExpressionEvaluator percentile
        ) {
            this.source = source;
            this.driverContext = driverContext;
            this.histogram = histogram;
            this.percentile = percentile;
        }

        private Warnings warnings() {
            if (warnings == null) {
                this.warnings = Warnings.createWarnings(
                    driverContext.warningsMode(),
                    source.source().getLineNumber(),
                    source.source().getColumnNumber(),
                    source.text()
                );
            }
            return warnings;
        }

        @Override
        public Block eval(Page page) {
            // TODO this is a ton of boilerplate, which will we repeated for every function
            // we should adapt EvaluatorImplementer instead to deal with exponential histograms
            ExponentialHistogramBlock histogramBlock = null;
            DoubleBlock percentileBlock = null;
            int positionCount = page.getPositionCount();
            try (DoubleBlock.Builder result = driverContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
                histogramBlock = (ExponentialHistogramBlock) histogram.eval(page);
                percentileBlock = (DoubleBlock) percentile.eval(page);
                var histogramAccessor = new ExponentialHistogramBlockAccessor(histogramBlock);
                position: for (int p = 0; p < positionCount; p++) {
                    switch (percentileBlock.getValueCount(p)) {
                        case 0:
                            result.appendNull();
                            continue position;
                        case 1:
                            break;
                        default:
                            warnings().registerException(IllegalArgumentException.class, "single-value function encountered multi-value");
                            result.appendNull();
                            continue position;
                    }
                    switch (histogramBlock.getValueCount(p)) {
                        case 0:
                            result.appendNull();
                            continue position;
                        case 1:
                            break;
                        default:
                            warnings().registerException(IllegalArgumentException.class, "single-value function encountered multi-value");
                            result.appendNull();
                            continue position;
                    }

                    ExponentialHistogram histogram = histogramAccessor.get(histogramBlock.getFirstValueIndex(p));
                    double percentile = percentileBlock.getDouble(percentileBlock.getFirstValueIndex(p));
                    double resultValue = ExponentialHistogramQuantile.getQuantile(histogram, percentile / 100.0);
                    if (Double.isNaN(resultValue)) {
                        result.appendNull();
                    } else {
                        result.appendDouble(resultValue);
                    }
                }
                return result.build();
            } finally {
                Releasables.close(histogramBlock, percentileBlock);
            }
        }

        @Override
        public long baseRamBytesUsed() {
            return BASE_RAM_BYTES_USED + histogram.baseRamBytesUsed() + percentile.baseRamBytesUsed();
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(histogram, percentile);
        }

        @Override
        public String toString() {
            return "HistogramPercentileEvaluator[field=" + histogram + ",subfieldIndex=" + percentile + "]";
        }
    }
}

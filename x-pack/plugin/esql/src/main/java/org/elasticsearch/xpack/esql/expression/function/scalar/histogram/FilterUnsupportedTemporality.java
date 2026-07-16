/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.histogram;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.aggregation.InvalidTemporalityException;
import org.elasticsearch.compute.aggregation.Temporality;
import org.elasticsearch.compute.aggregation.TemporalityAccessor;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ExponentialHistogramBlock;
import org.elasticsearch.compute.data.TDigestBlock;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * Filters histograms by their temporality. Returns the histogram unchanged if temporality is delta or null.
 * Throws an exception if temporality is cumulative.
 * <p>
 * This function is injected during local planning to handle backwards compatibility in CCS scenarios
 * where the coordinating node is older and produces plans that apply {@link ExtractHistogramComponent}
 * directly on histogram fields without going through {@code merge_over_time}.
 * <p>
 * This function is not registered in the function registry and is not available as a user-facing function.
 */
public class FilterUnsupportedTemporality extends EsqlScalarFunction {

    private final Expression histogram;
    private final Expression temporality;

    @FunctionInfo(returnType = { "exponential_histogram", "tdigest" }, briefSummary = "Filters histograms with unsupported temporality.")
    public FilterUnsupportedTemporality(
        Source source,
        @Param(name = "histogram", type = { "exponential_histogram", "tdigest" }) Expression histogram,
        @Param(name = "temporality", type = { "keyword" }) Expression temporality
    ) {
        super(source, List.of(histogram, temporality));
        this.histogram = histogram;
        this.temporality = temporality;
    }

    public Expression histogram() {
        return histogram;
    }

    public Expression temporality() {
        return temporality;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("FilterUnsupportedTemporality is not meant to be serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("FilterUnsupportedTemporality is not meant to be serialized");
    }

    @Override
    public DataType dataType() {
        return histogram.dataType();
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new FilterUnsupportedTemporality(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, FilterUnsupportedTemporality::new, histogram, temporality);
    }

    @Override
    protected TypeResolution resolveType() {
        TypeResolution histoTypeCheck = isType(
            histogram,
            dt -> dt == DataType.EXPONENTIAL_HISTOGRAM || dt == DataType.TDIGEST,
            sourceText(),
            FIRST,
            "exponential_histogram",
            "tdigest"
        );
        TypeResolution temporalityTypeCheck = isType(temporality, dt -> dt == DataType.KEYWORD, sourceText(), SECOND, "keyword");
        return histoTypeCheck.and(temporalityTypeCheck);
    }

    @Override
    public boolean foldable() {
        return false;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var histogramEvaluator = toEvaluator.apply(histogram);
        var temporalityEvaluator = toEvaluator.apply(temporality);
        if (histogram.dataType() == DataType.EXPONENTIAL_HISTOGRAM) {
            return new FilterUnsupportedTemporalityExpHistEvaluator.Factory(
                source(),
                histogramEvaluator,
                temporalityEvaluator,
                context -> new TemporalityAccessor[1]
            );
        }
        return new FilterUnsupportedTemporalityTDigestEvaluator.Factory(
            source(),
            histogramEvaluator,
            temporalityEvaluator,
            context -> new TemporalityAccessor[1]
        );
    }

    @Evaluator(extraName = "ExpHist", warnExceptions = { InvalidTemporalityException.class }, allNullsIsNull = false)
    static void processExpHist(
        ExponentialHistogramBlock.Builder result,
        @Position int position,
        ExponentialHistogramBlock histogram,
        BytesRefBlock temporality,
        @Fixed(includeInToString = false, scope = Fixed.Scope.THREAD_LOCAL) TemporalityAccessor[] accessor
    ) {
        if (histogram.isNull(position)) {
            result.appendNull();
            return;
        }
        if (accessor[0] == null || accessor[0].block() != temporality) {
            accessor[0] = TemporalityAccessor.create(temporality, Temporality.DELTA);
        }
        if (accessor[0].get(position) == Temporality.CUMULATIVE) {
            throw new IllegalArgumentException("Cumulative temporality is not supported for the exponential_histogram type on all nodes");
        }
        result.copyFrom(histogram, position, position + 1);
    }

    @Evaluator(extraName = "TDigest", warnExceptions = { InvalidTemporalityException.class }, allNullsIsNull = false)
    static void processTDigest(
        TDigestBlock.Builder result,
        @Position int position,
        TDigestBlock histogram,
        BytesRefBlock temporality,
        @Fixed(includeInToString = false, scope = Fixed.Scope.THREAD_LOCAL) TemporalityAccessor[] accessor
    ) {
        if (histogram.isNull(position)) {
            result.appendNull();
            return;
        }
        if (accessor[0] == null || accessor[0].block() != temporality) {
            accessor[0] = TemporalityAccessor.create(temporality, Temporality.DELTA);
        }
        if (accessor[0].get(position) == Temporality.CUMULATIVE) {
            throw new IllegalArgumentException("Cumulative temporality is not supported for the tdigest type.");
        }
        result.copyFrom(histogram, position, position + 1);
    }
}

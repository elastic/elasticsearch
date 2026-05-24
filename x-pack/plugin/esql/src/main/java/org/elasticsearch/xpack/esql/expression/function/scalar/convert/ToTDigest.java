/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.data.BreakingTDigestHolder;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.xpack.core.analytics.mapper.EncodedTDigest;
import org.elasticsearch.xpack.core.analytics.mapper.ExponentialHistogramToTDigestConverter;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.compute.ann.Fixed.Scope.THREAD_LOCAL;

public class ToTDigest extends AbstractConvertFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "ToTDigest",
        ToTDigest::new
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(ToTDigest.class).unary(ToTDigest::new).name("to_tdigest");

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(DataType.TDIGEST, (source, field) -> field),
        Map.entry(
            DataType.HISTOGRAM,
            (source, in) -> new ToTDigestFromHistogramEvaluator.Factory(source, in, dc -> new EncodedTDigest(), dc -> new TDigestHolder())
        ),
        Map.entry(
            DataType.EXPONENTIAL_HISTOGRAM,
            (source, in) -> new ToTDigestFromExponentialHistogramEvaluator.Factory(
                source,
                in,
                dc -> BreakingTDigestHolder.create(dc.breaker())
            )
        )
    );

    @FunctionInfo(
        returnType = "tdigest",
        description = "Converts an untyped histogram to a TDigest, assuming the values are centroids.",
        examples = { @Example(file = "histogram", tag = "to_tdigest") },
        appliesTo = {
            @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.3.0"),
            @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.4.0") }
    )
    public ToTDigest(
        Source source,
        @Param(
            name = "field",
            type = { "histogram", "tdigest", "exponential_histogram" },
            description = "The histogram value to be converted"
        ) Expression field
    ) {
        super(source, field);
    }

    protected ToTDigest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public DataType dataType() {
        return DataType.TDIGEST;
    }

    @Override
    protected Map<DataType, BuildFactory> factories() {
        return EVALUATORS;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToTDigest(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToTDigest::new, field());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @ConvertEvaluator(extraName = "FromHistogram", warnExceptions = { IllegalArgumentException.class })
    static TDigestHolder fromHistogram(
        BytesRef in,
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) EncodedTDigest decoder,
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) TDigestHolder scratch
    ) {
        if (in.length > ByteSizeUnit.MB.toBytes(2)) {
            throw new IllegalArgumentException("Histogram length is greater than 2MB");
        }
        // even though the encoded format is the same, we need to decode here to compute the summary data
        decoder.reset(in);
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        double sum = 0;
        long totalCount = 0;
        EncodedTDigest.CentroidIterator it = decoder.centroidIterator();
        while (it.next()) {
            long count = it.currentCount();
            double value = it.currentMean();
            min = Math.min(min, value);
            max = Math.max(max, value);
            sum += value * count;
            totalCount += count;
        }
        if (totalCount == 0) {
            min = Double.NaN;
            max = Double.NaN;
            sum = Double.NaN;
        }
        scratch.reset(in, min, max, sum, totalCount);
        return scratch;
    }

    @ConvertEvaluator(extraName = "FromExponentialHistogram")
    static TDigestHolder fromExponentialHistogram(
        ExponentialHistogram in,
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) BreakingTDigestHolder scratch
    ) {
        EncodedTDigest.CentroidIterator convertedCentroids = ExponentialHistogramToTDigestConverter.convert(
            in.negativeBuckets(),
            in.zeroBucket(),
            in.positiveBuckets()
        );
        double sum = in.isEmpty() == false ? in.sum() : Double.NaN;
        scratch.set(convertedCentroids, sum, in.min(), in.max());
        return scratch.accessor();
    }

}

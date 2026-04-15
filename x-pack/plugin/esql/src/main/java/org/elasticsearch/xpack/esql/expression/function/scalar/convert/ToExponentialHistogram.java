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
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.data.ExponentialHistogramBlock;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.exponentialhistogram.TDigestToExponentialHistogramConverter;
import org.elasticsearch.tdigest.Centroid;
import org.elasticsearch.xpack.core.analytics.mapper.EncodedTDigest;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.compute.ann.Fixed.Scope.THREAD_LOCAL;

public class ToExponentialHistogram extends AbstractConvertFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "ToExponentialHistogram",
        ToExponentialHistogram::new
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(ToExponentialHistogram.class)
        .unary(ToExponentialHistogram::new)
        .name("to_exponential_histogram");

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(DataType.EXPONENTIAL_HISTOGRAM, (source, field) -> field),
        Map.entry(
            DataType.HISTOGRAM,
            (source, in) -> new ToExponentialHistogramFromHistogramEvaluator.Factory(
                source,
                in,
                dc -> new EncodedTDigest(),
                dc -> new TDigestHolder()
            )
        ),
        Map.entry(DataType.TDIGEST, ToExponentialHistogramFromTDigestEvaluator.Factory::new)
    );

    @FunctionInfo(
        returnType = "exponential_histogram",
        description = "Converts histogram-like values to an exponential histogram.",
        examples = { @Example(file = "exponential_histogram", tag = "to_exponential_histogram") },
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.4.0") }
    )
    public ToExponentialHistogram(
        Source source,
        @Param(
            name = "field",
            type = { "histogram", "exponential_histogram", "tdigest" },
            description = "The histogram value to be converted"
        ) Expression field
    ) {
        super(source, field);
    }

    protected ToExponentialHistogram(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public DataType dataType() {
        return DataType.EXPONENTIAL_HISTOGRAM;
    }

    @Override
    protected Map<DataType, BuildFactory> factories() {
        return EVALUATORS;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToExponentialHistogram(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToExponentialHistogram::new, field());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Evaluator(extraName = "FromHistogram")
    static void fromHistogram(
        ExponentialHistogramBlock.Builder resultBuilder,
        BytesRef in,
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) EncodedTDigest decoder,
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) TDigestHolder scratch
    ) {
        fromTDigest(resultBuilder, ToTDigest.fromHistogram(in, decoder, scratch));
    }

    @Evaluator(extraName = "FromTDigest")
    static void fromTDigest(ExponentialHistogramBlock.Builder resultBuilder, TDigestHolder in) {
        // Todo: this could be made completly allocation free in case the T-Digest does not have negative values,
        // but we leave that for later
        List<Double> centroids = new ArrayList<>(in.centroidCount());
        List<Long> counts = new ArrayList<>(in.centroidCount());
        for (Centroid centroid : in.centroids()) {
            centroids.add(centroid.mean());
            counts.add(centroid.count());
        }
        TDigestToExponentialHistogramConverter.LazyConversion conversion = TDigestToExponentialHistogramConverter.convertLazy(
            new TDigestToExponentialHistogramConverter.ArrayBasedCentroidIterator(centroids, counts)
        );
        resultBuilder.append(
            conversion.getScale(),
            conversion.negativeBuckets(),
            conversion.positiveBuckets(),
            conversion.getZeroThreshold(),
            conversion.getZeroCount(),
            in.size(),
            in.getSum(),
            in.getMin(),
            in.getMax()
        );
    }
}

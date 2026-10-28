/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.exponentialhistogram.ReleasableExponentialHistogram;
import org.elasticsearch.exponentialhistogram.TDigestToExponentialHistogramConverter;
import org.elasticsearch.exponentialhistogram.ZeroBucket;
import org.elasticsearch.tdigest.Centroid;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class ToExponentialHistogramTests extends AbstractScalarFunctionTestCase {

    public ToExponentialHistogramTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        final List<TestCaseSupplier> suppliers = new ArrayList<>();

        TestCaseSupplier.forUnaryHistogram(
            suppliers,
            "ToExponentialHistogramFromHistogramEvaluator[in=Attribute[channel=0]]",
            DataType.EXPONENTIAL_HISTOGRAM,
            ToExponentialHistogramTests::fromHistogram,
            List.of()
        );

        TestCaseSupplier.forUnaryTDigest(
            suppliers,
            "ToExponentialHistogramFromTDigestEvaluator[in=Attribute[channel=0]]",
            DataType.EXPONENTIAL_HISTOGRAM,
            ToExponentialHistogramTests::fromTDigest,
            List.of()
        );

        TestCaseSupplier.forUnaryExponentialHistogram(
            suppliers,
            // This gets optimized to a no-op
            "Attribute[channel=0]",
            DataType.EXPONENTIAL_HISTOGRAM,
            h -> h,
            List.of()
        );
        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToExponentialHistogram(source, args.getFirst());
    }

    static ExponentialHistogram fromHistogram(BytesRef in) {
        return fromTDigest(ToTDigestTests.fromHistogram(in));
    }

    static ExponentialHistogram fromTDigest(TDigestHolder in) {
        List<Double> centroids = new ArrayList<>(in.centroidCount());
        List<Long> counts = new ArrayList<>(in.centroidCount());
        for (Centroid centroid : in.centroids()) {
            centroids.add(centroid.mean());
            counts.add(centroid.count());
        }
        try (
            ReleasableExponentialHistogram converted = TDigestToExponentialHistogramConverter.convert(
                new TDigestToExponentialHistogramConverter.ArrayBasedCentroidIterator(centroids, counts),
                ExponentialHistogramCircuitBreaker.noop()
            )
        ) {
            long convertedCount = converted.negativeBuckets().valueCount() + converted.positiveBuckets().valueCount();
            long targetCount = (long) (double) in.size();
            long zeroCount = targetCount - convertedCount;
            return ExponentialHistogram.builder(converted, ExponentialHistogramCircuitBreaker.noop())
                .zeroBucket(ZeroBucket.create(converted.zeroBucket().zeroThreshold(), zeroCount))
                .min(in.getMin())
                .max(in.getMax())
                .sum(in.size() == 0 ? 0.0 : in.getSum())
                .build();
        }
    }
}

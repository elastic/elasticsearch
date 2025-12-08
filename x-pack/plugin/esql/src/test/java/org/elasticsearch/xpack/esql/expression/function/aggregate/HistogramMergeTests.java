/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramMerger;
import org.elasticsearch.exponentialhistogram.ZeroBucket;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractAggregationTestCase;
import org.elasticsearch.xpack.esql.expression.function.MultiRowTestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.compute.aggregation.ExponentialHistogramStates.MAX_BUCKET_COUNT;
import static org.hamcrest.Matchers.equalTo;

public class HistogramMergeTests extends AbstractAggregationTestCase {
    public HistogramMergeTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var suppliers = new ArrayList<TestCaseSupplier>();

        Stream.of(MultiRowTestCaseSupplier.exponentialHistogramCases(1, 100))
            .flatMap(List::stream)
            .map(HistogramMergeTests::makeSupplier)
            .collect(Collectors.toCollection(() -> suppliers));

        return parameterSuppliersFromTypedDataWithDefaultChecks(suppliers, true);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new HistogramMerge(source, args.get(0));
    }

    private static TestCaseSupplier makeSupplier(TestCaseSupplier.TypedDataSupplier fieldSupplier) {
        return new TestCaseSupplier(List.of(fieldSupplier.type()), () -> {
            var fieldTypedData = fieldSupplier.get();
            var fieldValues = fieldTypedData.multiRowData();

            ExponentialHistogramMerger merger = ExponentialHistogramMerger.create(
                MAX_BUCKET_COUNT,
                ExponentialHistogramCircuitBreaker.noop()
            );

            boolean anyValuesNonNull = false;

            for (var fieldValue : fieldValues) {
                ExponentialHistogram histogram = (ExponentialHistogram) fieldValue;
                if (histogram != null) {
                    anyValuesNonNull = true;
                    merger.add(histogram);
                }
            }

            var expected = anyValuesNonNull ? merger.get() : null;
            return new TestCaseSupplier.TestCase(
                List.of(fieldTypedData),
                standardAggregatorName("HistogramMerge", fieldSupplier.type()),
                DataType.EXPONENTIAL_HISTOGRAM,
                equalToWithLenientZeroBucket(expected)
            );
        });
    }

    private static Matcher<?> equalToWithLenientZeroBucket(ExponentialHistogram expected) {
        // if there is no zero-threshold involved, merging is deterministic
        if (expected.zeroBucket().zeroThreshold() == 0) {
            return equalTo(expected);
        }

        // if there is a zero-threshold involed, things get a little more hairy
        // the exact merge result depends on the order in which downscales happen vs when the highest zero threshold is seen
        // this means the zero-bucket can be different to the expected result and the scale can slightly differ
        // we fix this by adjusting both histograms to the same scale

        return new BaseMatcher<ExponentialHistogram>() {
            @Override
            public boolean matches(Object actualObj) {
                if (actualObj instanceof ExponentialHistogram == false) {
                    return false;
                }
                ExponentialHistogram actual = (ExponentialHistogram) actualObj;

                // step one: bring both histogram to the same scale
                int targetScale = Math.min(actual.scale(), expected.scale());
                ExponentialHistogram a = downscaleTo(actual, targetScale);
                ExponentialHistogram b = downscaleTo(expected, targetScale);

                // step two: bring the zero-threshold of both histograms to the same value (the higher one)
                ZeroBucket targetZeroBucket;
                if (a.zeroBucket().compareZeroThreshold(b.zeroBucket()) >= 0) {
                    targetZeroBucket = a.zeroBucket();
                } else {
                    targetZeroBucket = b.zeroBucket();
                }
                a = increaseZeroThreshold(a, targetZeroBucket);
                b = increaseZeroThreshold(b, targetZeroBucket);
                // now they should actually be equal!
                return a.equals(b);
            }

            @Override
            public void describeTo(Description description) {
                description.appendValue(expected);
            }
        };
    }

    private static ExponentialHistogram downscaleTo(ExponentialHistogram histogram, int targetScale) {
        assert histogram.scale() >= targetScale;
        ExponentialHistogramMerger merger = ExponentialHistogramMerger.createWithMaxScale(
            MAX_BUCKET_COUNT,
            targetScale,
            ExponentialHistogramCircuitBreaker.noop()
        );
        merger.addWithoutUpscaling(histogram);
        return merger.get();
    }

    private static ExponentialHistogram increaseZeroThreshold(ExponentialHistogram histo, ZeroBucket targetZeroBucket) {
        ExponentialHistogramMerger merger = ExponentialHistogramMerger.create(MAX_BUCKET_COUNT, ExponentialHistogramCircuitBreaker.noop());
        merger.addWithoutUpscaling(histo);
        // now add a histogram with only the zero-threshold with a count of 1 to trigger merging of overlapping buckets
        merger.add(
            ExponentialHistogram.builder(ExponentialHistogram.MAX_SCALE, ExponentialHistogramCircuitBreaker.noop())
                .zeroBucket(copyWithNewCount(targetZeroBucket, 1))
                .build()
        );
        // the merger now has the desired zero-threshold, but we need to subtract the fake zero count again
        ExponentialHistogram mergeResult = merger.get();
        return ExponentialHistogram.builder(mergeResult, ExponentialHistogramCircuitBreaker.noop())
            .zeroBucket(copyWithNewCount(mergeResult.zeroBucket(), mergeResult.zeroBucket().count() - 1))
            .build();
    }

    private static ZeroBucket copyWithNewCount(ZeroBucket zb, long newCount) {
        if (zb.isIndexBased()) {
            return ZeroBucket.create(zb.index(), zb.scale(), newCount);
        } else {
            return ZeroBucket.create(zb.zeroThreshold(), newCount);
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.compute.aggregation.TDigestStates;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramMerger;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramUtils;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.tdigest.Centroid;
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

        Stream.of(MultiRowTestCaseSupplier.exponentialHistogramCases(1, 100), MultiRowTestCaseSupplier.tdigestCases(1, 100))
            .flatMap(List::stream)
            .map(HistogramMergeTests::makeSupplier)
            .collect(Collectors.toCollection(() -> suppliers));

        return parameterSuppliersFromTypedDataWithDefaultChecks(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new HistogramMerge(source, args.get(0));
    }

    private static TestCaseSupplier makeSupplier(TestCaseSupplier.TypedDataSupplier fieldSupplier) {
        return new TestCaseSupplier(List.of(fieldSupplier.type()), () -> {
            var fieldTypedData = fieldSupplier.get();
            var fieldValues = fieldTypedData.multiRowData();

            Matcher<?> resultMatcher;

            if (fieldTypedData.type() == DataType.EXPONENTIAL_HISTOGRAM) {
                resultMatcher = createExpectedExponentialHistogramMatcher(fieldValues);
            } else if (fieldTypedData.type() == DataType.TDIGEST) {
                resultMatcher = createExpectedTDigestMatcher(fieldValues);
            } else {
                throw new IllegalArgumentException("Unsupported data type [" + fieldTypedData.type() + "]");
            }

            return new TestCaseSupplier.TestCase(
                List.of(fieldTypedData),
                standardAggregatorName("HistogramMerge", fieldSupplier.type()),
                fieldTypedData.type(),
                resultMatcher
            );

        });
    }

    private static Matcher<?> createExpectedTDigestMatcher(List<Object> fieldValues) {
        // TDigest is non-deterministic, we just do a sanity check here:
        // the total count should match exactly and the result should have at least as many centroids as the largest input
        // in addition we check the p1 and p99 with a rather large tolerance

        long totalCount = 0;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        double sum = 0.0;
        boolean anyValuesNonNull = false;

        long maxCentroidCount = 0;
        TDigestState reference = TDigestState.createWithoutCircuitBreaking(TDigestStates.COMPRESSION);

        for (var fieldValue : fieldValues) {
            TDigestHolder tdigest = (TDigestHolder) fieldValue;
            if (tdigest != null) {
                anyValuesNonNull = true;
                totalCount += tdigest.getValueCount();
                min = Double.isNaN(tdigest.getMin()) ? min : Math.min(min, tdigest.getMin());
                max = Double.isNaN(tdigest.getMax()) ? max : Math.max(max, tdigest.getMax());
                sum += Double.isNaN(tdigest.getSum()) ? 0.0 : tdigest.getSum();

                TDigestState decoded = TDigestState.createWithoutCircuitBreaking(TDigestStates.COMPRESSION);
                tdigest.addTo(decoded);
                tdigest.addTo(reference);
                maxCentroidCount = Math.max(maxCentroidCount, decoded.centroidCount());
            }
        }

        if (anyValuesNonNull == false) {
            return equalTo(null);
        }

        double finalMin = min;
        double finalMax = max;
        double finalSum = sum;
        long finalTotalCount = totalCount;
        long finalMaxCentroidCount = maxCentroidCount;

        return new BaseMatcher<TDigestHolder>() {
            @Override
            public boolean matches(Object actualObj) {
                if (actualObj instanceof TDigestHolder == false) {
                    return false;
                }
                TDigestHolder actual = (TDigestHolder) actualObj;

                if (finalTotalCount > 0) {
                    if (finalMin != actual.getMin() || finalMax != actual.getMax() || finalSum != actual.getSum()) {
                        return false;
                    }
                } else {
                    if (Double.isNaN(actual.getMin()) == false
                        || Double.isNaN(actual.getMax()) == false
                        || Double.isNaN(actual.getSum()) == false) {
                        return false;
                    }
                }
                if (finalTotalCount != actual.getValueCount()) {
                    return false;
                }

                TDigestState decoded = TDigestState.createWithoutCircuitBreaking(TDigestStates.COMPRESSION);
                actual.addTo(decoded);
                if (decoded.centroidCount() < finalMaxCentroidCount) {
                    return false;
                }
                long tDigestTotalCount = 0;
                for (Centroid centroid : decoded.centroids()) {
                    tDigestTotalCount += centroid.count();
                }
                if (tDigestTotalCount != finalTotalCount) {
                    return false;
                }
                if (tDigestTotalCount > 0) {
                    if (Math.abs(decoded.quantile(0.01) - reference.quantile(0.01)) > 0.1) {
                        return false;
                    }
                    if (Math.abs(decoded.quantile(0.99) - reference.quantile(0.99)) > 0.1) {
                        return false;
                    }
                }

                return true;
            }

            @Override
            public void describeTo(Description description) {}
        };
    }

    private static Matcher<?> createExpectedExponentialHistogramMatcher(List<Object> fieldValues) {
        ExponentialHistogramMerger merger = ExponentialHistogramMerger.create(MAX_BUCKET_COUNT, ExponentialHistogramCircuitBreaker.noop());

        boolean anyValuesNonNull = false;

        for (var fieldValue : fieldValues) {
            ExponentialHistogram histogram = (ExponentialHistogram) fieldValue;
            if (histogram != null) {
                anyValuesNonNull = true;
                merger.add(histogram);
            }
        }

        var expected = anyValuesNonNull ? merger.get() : null;
        return equalToWithLenientZeroBucket(expected);
    }

    private static Matcher<?> equalToWithLenientZeroBucket(ExponentialHistogram expected) {
        return new BaseMatcher<ExponentialHistogram>() {
            @Override
            public boolean matches(Object actualObj) {
                if (actualObj instanceof ExponentialHistogram == false) {
                    return false;
                }
                ExponentialHistogram actual = (ExponentialHistogram) actualObj;

                ExponentialHistogramUtils.HistogramPair result = ExponentialHistogramUtils.removeMergeNoise(actual, expected);
                return result.first().equals(result.second());
            }

            @Override
            public void describeTo(Description description) {
                description.appendValue(expected);
            }
        };
    }

}

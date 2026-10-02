/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.histogram;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.compute.aggregation.TDigestStates;
import org.elasticsearch.compute.data.DoubleRangeBlockBuilder;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramQuantile;
import org.elasticsearch.search.aggregations.metrics.MemoryTrackingTDigestArrays;
import org.elasticsearch.tdigest.TDigest;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.math.Maths;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.startsWith;

public class HistogramFractionTests extends AbstractScalarFunctionTestCase {

    public HistogramFractionTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        List<TestCaseSupplier.TypedDataSupplier> histogramSuppliers = new ArrayList<>();
        histogramSuppliers.addAll(TestCaseSupplier.exponentialHistogramCases());
        histogramSuppliers.addAll(TestCaseSupplier.tdigestCases());

        for (TestCaseSupplier.TypedDataSupplier histogramSupplier : histogramSuppliers) {
            addCase(suppliers, histogramSupplier, null);
            addCase(suppliers, histogramSupplier, -3);
            addCase(suppliers, histogramSupplier, 0);
            addCase(suppliers, histogramSupplier, 3);
        }
        List<TestCaseSupplier> cases = anyNullIsNull(true, suppliers).stream()
            .filter(supplier -> supplier.types().size() < 3 || supplier.types().get(2) != DataType.NULL)
            .toList();
        return parameterSuppliersFromTypedData(cases);
    }

    private static void addCase(List<TestCaseSupplier> suppliers, TestCaseSupplier.TypedDataSupplier histogramSupplier, Integer decimals) {
        List<DataType> types = decimals != null
            ? List.of(histogramSupplier.type(), DataType.DOUBLE_RANGE, DataType.INTEGER)
            : List.of(histogramSupplier.type(), DataType.DOUBLE_RANGE);
        suppliers.add(new TestCaseSupplier(histogramSupplier.name() + ", decimals=" + decimals, types, () -> {
            TestCaseSupplier.TypedData histogram = histogramSupplier.get();
            DoubleRangeBlockBuilder.DoubleRange bucket = TestCaseSupplier.randomDoubleRange();
            List<TestCaseSupplier.TypedData> data = new ArrayList<>();
            data.add(histogram);
            data.add(new TestCaseSupplier.TypedData(bucket, DataType.DOUBLE_RANGE, "bucket"));
            if (decimals != null) {
                data.add(new TestCaseSupplier.TypedData(decimals, DataType.INTEGER, "decimals").forceLiteral());
            }
            return new TestCaseSupplier.TestCase(
                data,
                evaluatorMatcher(histogramSupplier.type(), decimals),
                DataType.DOUBLE,
                expectedMatcher(histogram.data(), bucket, decimals)
            );
        }));
    }

    private static Matcher<String> evaluatorMatcher(DataType histogramType, Integer decimals) {
        String prefix = "HistogramFraction"
            + (histogramType == DataType.TDIGEST ? "TDigest" : "ExponentialHistogram")
            + "Evaluator[histogram=Attribute[channel=0], bucket=Attribute[channel=1], decimals="
            + decimals;
        return histogramType == DataType.TDIGEST ? startsWith(prefix) : equalTo(prefix + "]");
    }

    private static Matcher<Double> expectedMatcher(Object histogram, DoubleRangeBlockBuilder.DoubleRange bucket, Integer decimals) {
        RankRange lowerRank;
        RankRange upperRank;
        if (histogram instanceof ExponentialHistogram exponentialHistogram) {
            if (exponentialHistogram.valueCount() == 0) {
                return equalTo(0.0);
            }
            long count = exponentialHistogram.valueCount();
            DoubleUnaryOperator quantile = q -> ExponentialHistogramQuantile.getQuantile(exponentialHistogram, q);
            lowerRank = rankRange(count, bucket.from(), quantile);
            upperRank = rankRange(count, bucket.to(), quantile);
        } else if (histogram instanceof TDigestHolder tdigest) {
            try (
                TDigest scratch = TDigest.createMergingDigest(
                    new MemoryTrackingTDigestArrays(new NoopCircuitBreaker("histogram-fraction-tests")),
                    TDigestStates.COMPRESSION
                )
            ) {
                scratch.add(tdigest);
                long count = scratch.size();
                if (count == 0) {
                    return equalTo(0.0);
                }
                lowerRank = rankRange(count, bucket.from(), scratch::quantile);
                upperRank = rankRange(count, bucket.to(), scratch::quantile);
            }
        } else {
            throw new AssertionError("unexpected histogram [" + histogram + "]");
        }

        double lowerResult;
        double upperResult;
        if (decimals == null) {
            lowerResult = upperRank.lower() - lowerRank.upper();
            upperResult = upperRank.upper() - lowerRank.lower();
        } else {
            lowerResult = Maths.round(upperRank.lower(), decimals).doubleValue() - Maths.round(lowerRank.upper(), decimals).doubleValue();
            upperResult = Maths.round(upperRank.upper(), decimals).doubleValue() - Maths.round(lowerRank.lower(), decimals).doubleValue();
        }
        lowerResult = Math.max(0.0, lowerResult);
        upperResult = Math.max(lowerResult, upperResult);
        return allOf(greaterThanOrEqualTo(lowerResult), lessThanOrEqualTo(upperResult));
    }

    /**
     * Finds adjacent quantiles that bracket the rank of {@code value}.
     */
    private static RankRange rankRange(long count, double value, DoubleUnaryOperator quantile) {
        if (value < quantile.applyAsDouble(0.0)) {
            return new RankRange(0.0, 0.0);
        }
        if (value > quantile.applyAsDouble(1.0)) {
            return new RankRange(count, count);
        }

        long low = 1;
        long high = count;
        long firstGreater = -1;
        while (low <= high) {
            long mid = low + (high - low) / 2;
            if (quantile.applyAsDouble(mid / (double) count) > value) {
                firstGreater = mid;
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        if (firstGreater == -1) {
            return new RankRange(count, count);
        }
        return new RankRange(firstGreater - 1.0, firstGreater);
    }

    private record RankRange(double lower, double upper) {}

    public void testDecimalsMustBeFoldable() {
        HistogramFraction function = new HistogramFraction(
            Source.EMPTY,
            field("histogram", DataType.TDIGEST),
            field("bucket", DataType.DOUBLE_RANGE),
            field("decimals", DataType.INTEGER)
        );
        assertTrue(function.typeResolved().unresolved());
        assertThat(function.typeResolved().message(), equalTo("third argument of [] must be a constant, received [decimals]"));
    }

    public void testEmptyExponentialHistogramReturnsZero() {
        DoubleRangeBlockBuilder.DoubleRange bucket = new DoubleRangeBlockBuilder.DoubleRange(-1.0, 1.0);
        assertEquals(0.0, HistogramFraction.process(ExponentialHistogram.empty(), bucket, null), 0.0);
        assertEquals(0.0, HistogramFraction.process(ExponentialHistogram.empty(), bucket, 2), 0.0);
    }

    public void testWholeExponentialHistogramReturnsCount() {
        ExponentialHistogram exponentialHistogram = randomValueOtherThanMany(
            ExponentialHistogram::isEmpty,
            EsqlTestUtils::randomExponentialHistogram
        );
        DoubleRangeBlockBuilder.DoubleRange exponentialHistogramRange = new DoubleRangeBlockBuilder.DoubleRange(
            Math.nextDown(exponentialHistogram.min()),
            Math.nextUp(exponentialHistogram.max())
        );
        assertEquals(
            exponentialHistogram.valueCount(),
            HistogramFraction.process(exponentialHistogram, exponentialHistogramRange, null),
            0.0
        );
    }

    public void testWholeTDigestReturnsCount() {
        TDigestHolder tdigest = EsqlTestUtils.randomTDigest();
        DoubleRangeBlockBuilder.DoubleRange tdigestRange = new DoubleRangeBlockBuilder.DoubleRange(
            Math.nextDown(tdigest.getMin()),
            Math.nextUp(tdigest.getMax())
        );
        assertEquals(tdigest.size(), HistogramFraction.process(tdigest, tdigestRange, null, newTDigestArrays()), 0.0);
    }

    public void testExponentialHistogramFractionsAreAdditive() {
        int decimals = randomIntBetween(-5, 50);
        ExponentialHistogram exponentialHistogram = randomValueOtherThanMany(
            ExponentialHistogram::isEmpty,
            EsqlTestUtils::randomExponentialHistogram
        );
        assertFractionsAreAdditive(
            exponentialHistogram.min(),
            exponentialHistogram.max(),
            decimals,
            bucket -> HistogramFraction.process(exponentialHistogram, bucket, decimals)
        );
    }

    public void testTDigestFractionsAreAdditive() {
        int decimals = randomIntBetween(-5, 50);
        TDigestHolder tdigest = EsqlTestUtils.randomTDigest();
        MemoryTrackingTDigestArrays tdigestArrays = newTDigestArrays();
        assertFractionsAreAdditive(
            tdigest.getMin(),
            tdigest.getMax(),
            decimals,
            bucket -> HistogramFraction.process(tdigest, bucket, decimals, tdigestArrays)
        );
    }

    private void assertFractionsAreAdditive(
        double min,
        double max,
        int decimals,
        Function<DoubleRangeBlockBuilder.DoubleRange, Double> fraction
    ) {
        double rangeMin = min - Math.abs(min) * 0.1;
        double rangeMax = max + Math.abs(max) * 0.1;
        double from = randomDoubleBetween(rangeMin, rangeMax, true);
        double to = randomDoubleBetween(from, rangeMax, true);
        double x = randomDoubleBetween(from, to, true);

        double whole = fraction.apply(new DoubleRangeBlockBuilder.DoubleRange(from, to));
        double parts = fraction.apply(new DoubleRangeBlockBuilder.DoubleRange(from, x)) + fraction.apply(
            new DoubleRangeBlockBuilder.DoubleRange(x, to)
        );
        double tolerance = Math.ulp(whole) + Math.ulp(parts);
        assertEquals("decimals=" + decimals + ", range=[" + from + ", " + to + "], x=" + x, whole, parts, tolerance);
    }

    private static MemoryTrackingTDigestArrays newTDigestArrays() {
        return new MemoryTrackingTDigestArrays(new NoopCircuitBreaker("histogram-fraction-tests"));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new HistogramFraction(source, args.get(0), args.get(1), args.size() == 2 ? null : args.get(2));
    }
}

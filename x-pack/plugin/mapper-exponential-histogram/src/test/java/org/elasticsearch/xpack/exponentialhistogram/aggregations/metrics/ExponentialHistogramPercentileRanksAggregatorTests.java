/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.exponentialhistogram.aggregations.metrics;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramMerger;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramQuantile;
import org.elasticsearch.exponentialhistogram.ExponentialScaleUtils;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalTDigestPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.PercentileRanksAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.PercentilesConfig;
import org.elasticsearch.search.aggregations.metrics.TDigestExecutionHint;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.exponentialhistogram.ExponentialHistogramFieldMapper;
import org.elasticsearch.xpack.exponentialhistogram.aggregations.ExponentialHistogramAggregatorTestCase;
import org.elasticsearch.xpack.exponentialhistogram.aggregations.support.ExponentialHistogramValuesSourceType;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ExponentialHistogramPercentileRanksAggregatorTests extends ExponentialHistogramAggregatorTestCase {

    private static final String FIELD_NAME = "my_histogram";

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        var tdigestConfig = new PercentilesConfig.TDigest();
        if (randomBoolean()) {
            tdigestConfig.setCompression(randomDoubleBetween(50, 200, true));
        }
        if (randomBoolean()) {
            tdigestConfig.parseExecutionHint(randomFrom(TDigestExecutionHint.values()).toString());
        }
        return new PercentileRanksAggregationBuilder("my_percentile_ranks", new double[] { 10.0, 100.0, 1000.0 }).field(fieldName)
            .percentilesConfig(tdigestConfig);
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return List.of(
            CoreValuesSourceType.NUMERIC,
            CoreValuesSourceType.DATE,
            CoreValuesSourceType.BOOLEAN,
            ExponentialHistogramValuesSourceType.EXPONENTIAL_HISTOGRAM
        );
    }

    public void testNoDocs() throws IOException {
        testCase(Queries.ALL_DOCS_INSTANCE, iw -> {
            // Intentionally not writing any docs
        }, percentileRanks -> assertFalse(AggregationInspectionHelper.hasValue(percentileRanks)));
    }

    public void testNoMatchingField() throws IOException {
        List<ExponentialHistogram> histograms = createRandomHistograms(10);
        testCase(
            Queries.ALL_DOCS_INSTANCE,
            iw -> histograms.forEach(histo -> addHistogramDoc(iw, "wrong_field", histo)),
            percentileRanks -> assertFalse(AggregationInspectionHelper.hasValue(percentileRanks))
        );
    }

    public void testRandomHistograms() throws IOException {
        List<ExponentialHistogram> histograms = createRandomHistograms(randomIntBetween(1, 100));
        boolean anyNonEmpty = histograms.stream().anyMatch(histo -> histo.valueCount() > 0);

        testCase(Queries.ALL_DOCS_INSTANCE, iw -> histograms.forEach(histo -> addHistogramDoc(iw, FIELD_NAME, histo)), percentileRanks -> {
            assertThat(AggregationInspectionHelper.hasValue(percentileRanks), equalTo(anyNonEmpty));
            verifyPercentileRanks(percentileRanks, histograms);
        });
    }

    public void testQueryFiltering() throws IOException {
        List<Map.Entry<ExponentialHistogram, Boolean>> histogramsWithFilter = createRandomHistograms(10).stream()
            .map(histo -> Map.entry(histo, randomBoolean()))
            .toList();

        List<ExponentialHistogram> filteredHistograms = histogramsWithFilter.stream()
            .filter(Map.Entry::getValue)
            .map(Map.Entry::getKey)
            .toList();

        boolean anyMatchingNonEmpty = filteredHistograms.stream().anyMatch(histo -> histo.valueCount() > 0);

        testCase(
            new TermQuery(new Term("match", "yes")),
            iw -> histogramsWithFilter.forEach(
                entry -> addHistogramDoc(
                    iw,
                    FIELD_NAME,
                    entry.getKey(),
                    new StringField("match", entry.getValue() ? "yes" : "no", Field.Store.NO)
                )
            ),
            percentileRanks -> {
                assertThat(AggregationInspectionHelper.hasValue(percentileRanks), equalTo(anyMatchingNonEmpty));
                verifyPercentileRanks(percentileRanks, filteredHistograms);
            }
        );
    }

    public void testCustomValues() throws IOException {
        List<ExponentialHistogram> histograms = createRandomHistograms(randomIntBetween(10, 50));
        boolean anyNonEmpty = histograms.stream().anyMatch(histo -> histo.valueCount() > 0);

        double[] customValues = new double[] { 0.1, 1.0, 5.0, 10.0, 50.0, 100.0, 500.0 };
        testCase(Queries.ALL_DOCS_INSTANCE, iw -> histograms.forEach(histo -> addHistogramDoc(iw, FIELD_NAME, histo)), percentileRanks -> {
            assertThat(AggregationInspectionHelper.hasValue(percentileRanks), equalTo(anyNonEmpty));
            if (anyNonEmpty) {
                // Verify all requested percentile ranks are calculable and in valid range
                double lastPercent = -1;
                for (double value : customValues) {
                    double percent = percentileRanks.percent(value);
                    assertTrue("Percentile rank for value " + value + " should be finite", Double.isFinite(percent));
                    assertThat("Percentile ranks should be in [0, 100]", percent, greaterThanOrEqualTo(0.0));
                    assertThat("Percentile ranks should be in [0, 100]", percent, lessThanOrEqualTo(100.0));
                    // Percentile ranks should be non-decreasing as values increase
                    assertTrue(
                        "Percentile ranks should be non-decreasing for increasing values",
                        percent >= lastPercent || Double.isNaN(lastPercent)
                    );
                    lastPercent = percent;
                }
            }
        }, customValues);
    }

    private static void verifyPercentileRanks(
        InternalTDigestPercentileRanks percentileRanks,
        List<ExponentialHistogram> referenceHistograms
    ) {
        ExponentialHistogram reference = ExponentialHistogram.merge(
            ExponentialHistogramMerger.DEFAULT_MAX_HISTOGRAM_BUCKETS,
            ExponentialHistogramCircuitBreaker.noop(),
            referenceHistograms.iterator()
        );

        for (int i = 0; i <= 100; i++) {

            double value = 0;
            if (reference.valueCount() > 0) {
                value = ExponentialHistogramQuantile.getQuantile(reference, i / 100.0);
            }

            double aggregatedPercent = percentileRanks.percent(value);
            double referencePercent = getPercentileRank(reference, value);

            if (Double.isNaN(referencePercent)) {
                assertThat(aggregatedPercent, equalTo(Double.NaN));
            } else {
                double relativeBucketSize = ExponentialScaleUtils.getLowerBucketBoundary(1, reference.scale()) / ExponentialScaleUtils
                    .getLowerBucketBoundary(0, reference.scale());
                double absoluteTolerance = Math.max(Math.abs(value * (relativeBucketSize - 1)), reference.zeroBucket().zeroThreshold());

                double minPercent = getPercentileRank(reference, value - absoluteTolerance) - 0.1;
                double maxPercent = getPercentileRank(reference, value + absoluteTolerance) + 0.1;
                assertThat(
                    "Percentile rank for value " + value + " should be close to reference",
                    aggregatedPercent,
                    greaterThanOrEqualTo(minPercent)
                );
                assertThat(
                    "Percentile rank for value " + value + " should be close to reference",
                    aggregatedPercent,
                    lessThanOrEqualTo(maxPercent)
                );
            }
        }
    }

    private static double getPercentileRank(ExponentialHistogram histogram, double x) {
        long numValuesLess = ExponentialHistogramQuantile.estimateRank(histogram, x, false);
        long numValuesLessOrEqual = ExponentialHistogramQuantile.estimateRank(histogram, x, true);
        long numValuesEqual = numValuesLessOrEqual - numValuesLess;
        // Just like for t-digest, equal values get half credit
        return (numValuesLess + numValuesEqual / 2.0) / histogram.valueCount() * 100;
    }

    private void testCase(
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<InternalTDigestPercentileRanks> verify
    ) throws IOException {
        testCase(query, buildIndex, verify, new double[] { 1.0, 10.0, 100.0 });
    }

    private void testCase(
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<InternalTDigestPercentileRanks> verify,
        double[] values
    ) throws IOException {
        PercentileRanksAggregationBuilder builder = new PercentileRanksAggregationBuilder("test", values).field(FIELD_NAME);

        var fieldType = new ExponentialHistogramFieldMapper.ExponentialHistogramFieldType(FIELD_NAME, Collections.emptyMap(), null);
        testCase(buildIndex, verify, new AggTestConfig(builder, fieldType).withQuery(query));
    }
}

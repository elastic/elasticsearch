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
import org.elasticsearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.elasticsearch.search.aggregations.metrics.PercentilesAggregationBuilder;
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
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ExponentialHistogramPercentilesAggregatorTests extends ExponentialHistogramAggregatorTestCase {

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
        return new PercentilesAggregationBuilder("my_percentiles").field(fieldName).percentilesConfig(tdigestConfig);
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
        }, percentiles -> assertFalse(AggregationInspectionHelper.hasValue(percentiles)));
    }

    public void testNoMatchingField() throws IOException {
        List<ExponentialHistogram> histograms = createRandomHistograms(10);
        testCase(
            Queries.ALL_DOCS_INSTANCE,
            iw -> histograms.forEach(histo -> addHistogramDoc(iw, "wrong_field", histo)),
            percentiles -> assertFalse(AggregationInspectionHelper.hasValue(percentiles))
        );
    }

    public void testRandomHistograms() throws IOException {
        List<ExponentialHistogram> histograms = createRandomHistograms(randomIntBetween(1, 100));
        boolean anyNonEmpty = histograms.stream().anyMatch(histo -> histo.valueCount() > 0);

        testCase(Queries.ALL_DOCS_INSTANCE, iw -> histograms.forEach(histo -> addHistogramDoc(iw, FIELD_NAME, histo)), percentiles -> {
            assertThat(AggregationInspectionHelper.hasValue(percentiles), equalTo(anyNonEmpty));
            verifyPercentiles(percentiles, histograms);
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
            percentiles -> {
                assertThat(AggregationInspectionHelper.hasValue(percentiles), equalTo(anyMatchingNonEmpty));
                verifyPercentiles(percentiles, filteredHistograms);
            }
        );
    }

    public void testCustomPercentiles() throws IOException {
        List<ExponentialHistogram> histograms = createRandomHistograms(randomIntBetween(10, 50));
        boolean anyNonEmpty = histograms.stream().anyMatch(histo -> histo.valueCount() > 0);

        double[] customPercents = new double[] { 1, 10, 25, 50, 75, 90, 99 };
        testCase(Queries.ALL_DOCS_INSTANCE, iw -> histograms.forEach(histo -> addHistogramDoc(iw, FIELD_NAME, histo)), percentiles -> {
            assertThat(AggregationInspectionHelper.hasValue(percentiles), equalTo(anyNonEmpty));
            if (anyNonEmpty) {
                // Verify all requested percentiles are calculable and in order
                double lastValue = Double.NEGATIVE_INFINITY;
                for (double percent : customPercents) {
                    double value = percentiles.percentile(percent);
                    assertTrue("Percentile " + percent + " should be finite", Double.isFinite(value));
                    assertTrue("Percentiles should be non-decreasing", value >= lastValue);
                    lastValue = value;
                }
            }
        }, customPercents);
    }

    private static void verifyPercentiles(InternalTDigestPercentiles percentiles, List<ExponentialHistogram> referenceHistograms) {
        ExponentialHistogram reference = ExponentialHistogram.merge(
            ExponentialHistogramMerger.DEFAULT_MAX_HISTOGRAM_BUCKETS,
            ExponentialHistogramCircuitBreaker.noop(),
            referenceHistograms.iterator()
        );

        double[] percentToTest = new double[] { 25, 50, 75, 99 };
        for (double percent : percentToTest) {
            double aggregatedValue = percentiles.percentile(percent);
            double referenceValue = ExponentialHistogramQuantile.getQuantile(reference, percent / 100.0);
            if (Double.isNaN(referenceValue)) {
                // reference is empty
                assertThat(aggregatedValue, equalTo(Double.NaN));
            } else {
                if (Math.abs(referenceValue) <= 2 * reference.zeroBucket().zeroThreshold()) {
                    // actual should be somewhere in the zero bucket
                    assertThat(Math.abs(aggregatedValue), lessThanOrEqualTo(2 * reference.zeroBucket().zeroThreshold()));
                } else {
                    double relativeError = Math.abs(aggregatedValue - referenceValue) / Math.abs(referenceValue);
                    double relativeBucketSize = ExponentialScaleUtils.getLowerBucketBoundary(1, reference.scale()) / ExponentialScaleUtils
                        .getLowerBucketBoundary(0, reference.scale());
                    double allowedError = Math.max(0.0001, (relativeBucketSize - 1.0) * 1.1);
                    assertThat(relativeError, lessThan(allowedError)); // within 5% relative error
                }
            }

        }
    }

    private void testCase(
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<InternalTDigestPercentiles> verify
    ) throws IOException {
        testCase(query, buildIndex, verify, new double[] { 25, 50, 75, 99 });
    }

    private void testCase(
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<InternalTDigestPercentiles> verify,
        double[] percentiles
    ) throws IOException {
        PercentilesAggregationBuilder builder = new PercentilesAggregationBuilder("test").field(FIELD_NAME).percentiles(percentiles);

        var fieldType = new ExponentialHistogramFieldMapper.ExponentialHistogramFieldType(FIELD_NAME, Collections.emptyMap(), null);
        testCase(buildIndex, verify, new AggTestConfig(builder, fieldType).withQuery(query));
    }
}

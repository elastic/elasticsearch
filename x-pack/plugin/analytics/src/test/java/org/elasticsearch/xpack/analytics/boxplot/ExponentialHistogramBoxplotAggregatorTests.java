/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.analytics.boxplot;

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
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.analytics.aggregations.ExponentialHistogramAggregatorTestCase;
import org.elasticsearch.xpack.analytics.aggregations.metrics.ExponentialHistogramPercentilesAggregatorTests;
import org.elasticsearch.xpack.analytics.aggregations.support.AnalyticsValuesSourceType;
import org.elasticsearch.xpack.analytics.mapper.ExponentialHistogramFieldMapper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;

public class ExponentialHistogramBoxplotAggregatorTests extends ExponentialHistogramAggregatorTestCase {

    private static final String FIELD_NAME = "my_histogram";

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new BoxplotAggregationBuilder("my_boxplot").field(fieldName);
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return List.of(CoreValuesSourceType.NUMERIC, AnalyticsValuesSourceType.HISTOGRAM, AnalyticsValuesSourceType.EXPONENTIAL_HISTOGRAM);
    }

    public void testNoDocs() throws IOException {
        testCase(Queries.ALL_DOCS_INSTANCE, iw -> {
            // Intentionally not writing any docs
        }, boxplot -> assertFalse(hasValue(boxplot)));
    }

    public void testNoMatchingField() throws IOException {
        List<ExponentialHistogram> histograms = createRandomHistograms(10);
        testCase(
            Queries.ALL_DOCS_INSTANCE,
            iw -> histograms.forEach(histo -> addHistogramDoc(iw, "wrong_field", histo)),
            boxplot -> assertFalse(hasValue(boxplot))
        );
    }

    public void testRandomHistograms() throws IOException {
        List<ExponentialHistogram> histograms = createRandomHistograms(randomIntBetween(1, 100));
        boolean anyNonEmpty = histograms.stream().anyMatch(histo -> histo.valueCount() > 0);

        testCase(Queries.ALL_DOCS_INSTANCE, iw -> histograms.forEach(histo -> addHistogramDoc(iw, FIELD_NAME, histo)), boxplot -> {
            assertThat(hasValue(boxplot), equalTo(anyNonEmpty));
            verifyBoxplot(boxplot, histograms);
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
            boxplot -> {
                assertThat(hasValue(boxplot), equalTo(anyMatchingNonEmpty));
                verifyBoxplot(boxplot, filteredHistograms);
            }
        );
    }

    private static void verifyBoxplot(InternalBoxplot boxplot, List<ExponentialHistogram> referenceHistograms) {
        ExponentialHistogram reference = ExponentialHistogram.merge(
            ExponentialHistogramMerger.DEFAULT_MAX_HISTOGRAM_BUCKETS,
            ExponentialHistogramCircuitBreaker.noop(),
            referenceHistograms.iterator()
        );

        if (reference.valueCount() == 0) {
            // Empty histogram should produce NaN for quartiles
            assertEquals(Double.POSITIVE_INFINITY, boxplot.getMin(), 0);
            assertEquals(Double.NEGATIVE_INFINITY, boxplot.getMax(), 0);
            assertEquals(Double.NaN, boxplot.getQ1(), 0);
            assertEquals(Double.NaN, boxplot.getQ2(), 0);
            assertEquals(Double.NaN, boxplot.getQ3(), 0);
            return;
        }

        // Verify quartiles (Q1=25%, Q2=50%, Q3=75%)
        ExponentialHistogramPercentilesAggregatorTests.verifyPercentile(reference, 25, boxplot.getQ1());
        ExponentialHistogramPercentilesAggregatorTests.verifyPercentile(reference, 50, boxplot.getQ2());
        ExponentialHistogramPercentilesAggregatorTests.verifyPercentile(reference, 75, boxplot.getQ3());

        // Verify min/max are within reasonable bounds
        double minReference = reference.min();
        double maxReference = reference.max();
        if (Double.isFinite(minReference)) {
            // Min from boxplot should be close to actual min
            assertTrue("Boxplot min should be <= actual max", boxplot.getMin() <= maxReference);
        }
        if (Double.isFinite(maxReference)) {
            // Max from boxplot should be close to actual max
            assertTrue("Boxplot max should be >= actual min", boxplot.getMax() >= minReference);
        }
    }

    /**
     * Check if a boxplot has meaningful data. An empty boxplot has min=+Inf, max=-Inf, and NaN quartiles.
     */
    private static boolean hasValue(InternalBoxplot boxplot) {
        return boxplot.getMin() != Double.POSITIVE_INFINITY
            && boxplot.getMax() != Double.NEGATIVE_INFINITY
            && Double.isNaN(boxplot.getQ1()) == false;
    }

    private void testCase(Query query, CheckedConsumer<RandomIndexWriter, IOException> buildIndex, Consumer<InternalBoxplot> verify)
        throws IOException {
        BoxplotAggregationBuilder builder = new BoxplotAggregationBuilder("test").field(FIELD_NAME);

        var fieldType = new ExponentialHistogramFieldMapper.ExponentialHistogramFieldType(FIELD_NAME, Collections.emptyMap(), null);
        testCase(buildIndex, verify, new AggTestConfig(builder, fieldType).withQuery(query));
    }
}

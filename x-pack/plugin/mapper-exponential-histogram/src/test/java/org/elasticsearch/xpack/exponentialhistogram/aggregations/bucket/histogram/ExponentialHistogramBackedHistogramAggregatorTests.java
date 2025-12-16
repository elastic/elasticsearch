/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.exponentialhistogram.aggregations.bucket.histogram;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.exponentialhistogram.BucketIterator;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.exponentialhistogram.ExponentialScaleUtils;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.xpack.exponentialhistogram.ExponentialHistogramFieldMapper;
import org.elasticsearch.xpack.exponentialhistogram.aggregations.ExponentialHistogramAggregatorTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;

public class ExponentialHistogramBackedHistogramAggregatorTests extends ExponentialHistogramAggregatorTestCase {

    private static final String FIELD_NAME = "histo_field";

    public void testRandomHistogramsAndRandomIntervalAndOffset() throws Exception {
        List<ExponentialHistogram> histograms = new ArrayList<>();
        double min = 0;
        double max = 0;
        while (min == max) {
            histograms.clear();
            histograms.addAll(createRandomHistograms(randomIntBetween(1, 1000)));
            min = histograms.stream()
                .filter(histogram -> histogram.valueCount() > 0)
                .mapToDouble(ExponentialHistogram::min)
                .min()
                .orElse(0.0);
            max = histograms.stream()
                .filter(histogram -> histogram.valueCount() > 0)
                .mapToDouble(ExponentialHistogram::max)
                .max()
                .orElse(0.0);
        }

        double interval = Math.max(1.0, (max - min) / randomIntBetween(1, 100));
        double offset = randomBoolean() ? 0 : randomDouble() * interval;

        Map<Double, Long> expectedHistogram = computeExpectedHistogram(histograms, interval, offset);

        testCase(
            new MatchAllDocsQuery(),
            histoAgg -> histoAgg.interval(interval).offset(offset),
            iw -> histograms.forEach(histo -> addHistogramDoc(iw, FIELD_NAME, histo)),
            histogram -> {
                assertThat(histogram.getBuckets().size(), equalTo(expectedHistogram.size()));
                assertTrue(AggregationInspectionHelper.hasValue(histogram));

                Iterator<Map.Entry<Double, Long>> expectedBucketsIterator = expectedHistogram.entrySet().iterator();
                for (int i = 0; i < histogram.getBuckets().size(); i++) {
                    double key = (Double) histogram.getBuckets().get(i).getKey();
                    long count = histogram.getBuckets().get(i).getDocCount();
                    Map.Entry<Double, Long> expected = expectedBucketsIterator.next();
                    assertThat(key, equalTo(expected.getKey()));
                    assertThat(count, equalTo(expected.getValue()));
                }
            }
        );
    }

    public void testMinDocCount() throws Exception {
        ExponentialHistogramCircuitBreaker noopBreaker = ExponentialHistogramCircuitBreaker.noop();
        List<ExponentialHistogram> histograms = List.of(
            ExponentialHistogram.create(100, noopBreaker, 0, 1.2, 10, 12, 24),
            ExponentialHistogram.create(100, noopBreaker, 5.3, 6, 6, 20),
            ExponentialHistogram.create(100, noopBreaker, -10, 0.01, 10, 10, 30, 90)
        );

        testCase(
            new MatchAllDocsQuery(),
            histoAgg -> histoAgg.interval(5).minDocCount(2),
            iw -> histograms.forEach(histo -> addHistogramDoc(iw, FIELD_NAME, histo)),
            histogram -> {
                assertEquals(4, histogram.getBuckets().size());
                assertEquals(0d, histogram.getBuckets().get(0).getKey());
                assertEquals(3, histogram.getBuckets().get(0).getDocCount());
                assertEquals(5d, histogram.getBuckets().get(1).getKey());
                assertEquals(3, histogram.getBuckets().get(1).getDocCount());
                assertEquals(10d, histogram.getBuckets().get(2).getKey());
                assertEquals(4, histogram.getBuckets().get(2).getDocCount());
                assertEquals(20d, histogram.getBuckets().get(3).getKey());
                assertEquals(2, histogram.getBuckets().get(3).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        );
    }

    public void testNoDocs() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            // Intentionally not writing any docs
        }, histogram -> {
            assertThat(histogram.getBuckets().isEmpty(), equalTo(true));
            assertThat(AggregationInspectionHelper.hasValue(histogram), equalTo(false));
        });
    }

    public void testNoMatchingField() throws IOException {
        List<ExponentialHistogram> histograms = createRandomHistograms(10);
        testCase(new MatchAllDocsQuery(), iw -> histograms.forEach(histo -> addHistogramDoc(iw, "wrong_field", histo)), histogram -> {
            assertThat(histogram.getBuckets().isEmpty(), equalTo(true));
            assertThat(AggregationInspectionHelper.hasValue(histogram), equalTo(false));
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

        double min = filteredHistograms.stream().filter(h -> h.valueCount() > 0).mapToDouble(ExponentialHistogram::min).min().orElse(0.0);
        double max = filteredHistograms.stream().filter(h -> h.valueCount() > 0).mapToDouble(ExponentialHistogram::max).max().orElse(0.0);
        double interval = Math.max(1.0, (max - min) / 20);
        Map<Double, Long> expectedHistogram = computeExpectedHistogram(filteredHistograms, interval, 0.0);

        testCase(
            new TermQuery(new Term("match", "yes")),
            histoAgg -> histoAgg.interval(interval),
            iw -> histogramsWithFilter.forEach(
                entry -> addHistogramDoc(
                    iw,
                    FIELD_NAME,
                    entry.getKey(),
                    new StringField("match", entry.getValue() ? "yes" : "no", Field.Store.NO)
                )
            ),
            histogram -> {
                assertThat(histogram.getBuckets().size(), equalTo(expectedHistogram.size()));
                if (filteredHistograms.stream().anyMatch(h -> h.valueCount() > 0)) {
                    assertThat(AggregationInspectionHelper.hasValue(histogram), equalTo(true));
                } else {
                    assertThat(AggregationInspectionHelper.hasValue(histogram), equalTo(false));
                }

                Iterator<Map.Entry<Double, Long>> expectedBucketsIterator = expectedHistogram.entrySet().iterator();
                for (int i = 0; i < histogram.getBuckets().size(); i++) {
                    double key = (Double) histogram.getBuckets().get(i).getKey();
                    long count = histogram.getBuckets().get(i).getDocCount();
                    Map.Entry<Double, Long> expected = expectedBucketsIterator.next();
                    assertThat(key, equalTo(expected.getKey()));
                    assertThat(count, equalTo(expected.getValue()));
                }
            }
        );
    }

    private void testCase(Query query, CheckedConsumer<RandomIndexWriter, IOException> buildIndex, Consumer<InternalHistogram> verify)
        throws IOException {
        testCase(query, aggBuilder -> {}, buildIndex, verify);
    }

    private void testCase(
        Query query,
        Consumer<HistogramAggregationBuilder> aggCustomizer,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<InternalHistogram> verify
    ) throws IOException {
        var fieldType = defaultFieldType(FIELD_NAME);
        HistogramAggregationBuilder aggregationBuilder = createAggBuilderForTypeTest(fieldType, FIELD_NAME);
        aggCustomizer.accept(aggregationBuilder);
        testCase(buildIndex, verify, new AggTestConfig(aggregationBuilder, fieldType).withQuery(query));
    }

    private MappedFieldType defaultFieldType(String fieldName) {
        return new ExponentialHistogramFieldMapper.ExponentialHistogramFieldType(fieldName, Collections.emptyMap(), null);
    }

    private Map<Double, Long> computeExpectedHistogram(List<ExponentialHistogram> histograms, double interval, double offset) {
        SortedMap<Double, Long> expectedHistogram = new TreeMap<>();
        for (ExponentialHistogram histogram : histograms) {
            BucketIterator negIt = histogram.negativeBuckets().iterator();
            while (negIt.hasNext()) {
                double center = -ExponentialScaleUtils.getPointOfLeastRelativeError(negIt.peekIndex(), negIt.scale());
                center = Math.clamp(center, histogram.min(), histogram.max());
                addToHistogram(expectedHistogram, interval, offset, center, negIt.peekCount());
                negIt.advance();
            }
            if (histogram.zeroBucket().count() > 0) {
                double center = Math.clamp(0.0, histogram.min(), histogram.max());
                addToHistogram(expectedHistogram, interval, offset, center, histogram.zeroBucket().count());
            }
            BucketIterator posIt = histogram.positiveBuckets().iterator();
            while (posIt.hasNext()) {
                double center = ExponentialScaleUtils.getPointOfLeastRelativeError(posIt.peekIndex(), posIt.scale());
                center = Math.clamp(center, histogram.min(), histogram.max());
                addToHistogram(expectedHistogram, interval, offset, center, posIt.peekCount());
                posIt.advance();
            }
        }
        return expectedHistogram;
    }

    private void addToHistogram(SortedMap<Double, Long> histogram, double interval, double offset, double value, long count) {
        double key = Math.floor((value - offset) / interval) * interval + offset;
        histogram.putIfAbsent(key, 0L);
        histogram.computeIfPresent(key, (k, v) -> v + count);
    }

    @Override
    protected HistogramAggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new HistogramAggregationBuilder("my_agg").field(FIELD_NAME).interval(1.0).minDocCount(1);
    }
}

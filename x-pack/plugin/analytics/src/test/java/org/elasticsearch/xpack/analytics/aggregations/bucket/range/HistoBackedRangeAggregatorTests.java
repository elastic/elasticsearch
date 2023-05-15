/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.aggregations.bucket.range;

import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.index.mapper.CustomTermFreqField;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.range.InternalRange;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;
import org.elasticsearch.search.aggregations.metrics.Percentiles;
import org.elasticsearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.PercentilesConfig;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.elasticsearch.xpack.analytics.mapper.HistogramFieldMapper;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static java.util.Collections.singleton;
import static org.elasticsearch.xpack.analytics.AnalyticsTestsUtils.hdrHistogramFieldDocValues;
import static org.elasticsearch.xpack.analytics.AnalyticsTestsUtils.histogramFieldDocValues;
import static org.hamcrest.Matchers.lessThan;

public class HistoBackedRangeAggregatorTests extends AggregatorTestCase {

    private static final String HISTO_FIELD_NAME = "histo_field";
    private static final String RAW_FIELD_NAME = "raw_field";

    @SuppressWarnings("rawtypes")
    public void testPercentilesAccuracy() throws Exception {
        long absError = 0L;
        long docCount = 0L;
        for (int k = 0; k < 10; k++) {
            try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
                docCount += generateDocs(w);
                double[] steps = IntStream.range(2, 99).filter(i -> i % 2 == 0).mapToDouble(Double::valueOf).toArray();

                PercentilesAggregationBuilder rawPercentiles = new PercentilesAggregationBuilder("my_agg").field(RAW_FIELD_NAME)
                    .percentilesConfig(new PercentilesConfig.Hdr())
                    .percentiles(steps);

                PercentilesAggregationBuilder aggregatedPercentiles = new PercentilesAggregationBuilder("my_agg").field(HISTO_FIELD_NAME)
                    .percentilesConfig(new PercentilesConfig.Hdr())
                    .percentiles(steps);

                try (IndexReader reader = w.getReader()) {
                    IndexSearcher searcher = new IndexSearcher(reader);
                    RangeAggregationBuilder aggBuilder = new RangeAggregationBuilder("my_agg").field(HISTO_FIELD_NAME);

                    RangeAggregationBuilder rawFieldAgg = new RangeAggregationBuilder("my_agg").field(RAW_FIELD_NAME);
                    Percentiles rawPercentileResults = searchAndReduce(
                        searcher,
                        new AggTestConfig(rawPercentiles, defaultFieldType(RAW_FIELD_NAME))
                    );
                    Percentiles aggregatedPercentileResults = searchAndReduce(
                        searcher,
                        new AggTestConfig(aggregatedPercentiles, defaultFieldType(HISTO_FIELD_NAME))
                    );
                    aggBuilder.addUnboundedTo(aggregatedPercentileResults.percentile(steps[0]));
                    rawFieldAgg.addUnboundedTo(rawPercentileResults.percentile(steps[0]));

                    for (int i = 1; i < steps.length; i++) {
                        aggBuilder.addRange(
                            aggregatedPercentileResults.percentile(steps[i - 1]),
                            aggregatedPercentileResults.percentile(steps[i])
                        );
                        rawFieldAgg.addRange(rawPercentileResults.percentile(steps[i - 1]), rawPercentileResults.percentile(steps[i]));
                    }
                    aggBuilder.addUnboundedFrom(aggregatedPercentileResults.percentile(steps[steps.length - 1]));
                    rawFieldAgg.addUnboundedFrom(rawPercentileResults.percentile(steps[steps.length - 1]));

                    InternalRange<? extends InternalRange.Bucket, ? extends InternalRange> range = searchAndReduce(
                        searcher,
                        new AggTestConfig(aggBuilder, defaultFieldType(HISTO_FIELD_NAME))
                    );
                    InternalRange<? extends InternalRange.Bucket, ? extends InternalRange> rawRange = searchAndReduce(
                        searcher,
                        new AggTestConfig(rawFieldAgg, defaultFieldType(RAW_FIELD_NAME))
                    );
                    for (int j = 0; j < rawRange.getBuckets().size(); j++) {
                        absError += Math.abs(range.getBuckets().get(j).getDocCount() - rawRange.getBuckets().get(j).getDocCount());
                    }
                }
            }
        }
        assertThat((double) absError / docCount, lessThan(0.1));
    }

    @SuppressWarnings("rawtypes")
    public void testMediumRangesAccuracy() throws Exception {
        List<RangeAggregator.Range> ranges = Arrays.asList(
            new RangeAggregator.Range(null, null, 2.0),
            new RangeAggregator.Range(null, 2.0, 4.0),
            new RangeAggregator.Range(null, 4.0, 6.0),
            new RangeAggregator.Range(null, 6.0, 8.0),
            new RangeAggregator.Range(null, 8.0, 9.0),
            new RangeAggregator.Range(null, 8.0, 11.0),
            new RangeAggregator.Range(null, 11.0, 12.0),
            new RangeAggregator.Range(null, 12.0, null)
        );
        testRanges(ranges, "manual_medium_ranges");
    }

    public void testLargerRangesAccuracy() throws Exception {
        List<RangeAggregator.Range> ranges = Arrays.asList(
            new RangeAggregator.Range(null, null, 8.0),
            new RangeAggregator.Range(null, 8.0, 12.0),
            new RangeAggregator.Range(null, 12.0, null)
        );
        testRanges(ranges, "manual_big_ranges");
    }

    public void testSmallerRangesAccuracy() throws Exception {
        List<RangeAggregator.Range> ranges = Arrays.asList(
            new RangeAggregator.Range(null, null, 1.0),
            new RangeAggregator.Range(null, 1.0, 1.5),
            new RangeAggregator.Range(null, 1.5, 2.0),
            new RangeAggregator.Range(null, 2.0, 2.5),
            new RangeAggregator.Range(null, 2.5, 3.0),
            new RangeAggregator.Range(null, 3.0, 3.5),
            new RangeAggregator.Range(null, 3.5, 4.0),
            new RangeAggregator.Range(null, 4.0, 4.5),
            new RangeAggregator.Range(null, 4.5, 5.0),
            new RangeAggregator.Range(null, 5.0, 5.5),
            new RangeAggregator.Range(null, 5.5, 6.0),
            new RangeAggregator.Range(null, 6.0, 6.5),
            new RangeAggregator.Range(null, 6.5, 7.0),
            new RangeAggregator.Range(null, 7.0, 7.5),
            new RangeAggregator.Range(null, 7.5, 8.0),
            new RangeAggregator.Range(null, 8.0, 8.5),
            new RangeAggregator.Range(null, 8.5, 9.0),
            new RangeAggregator.Range(null, 9.0, 9.5),
            new RangeAggregator.Range(null, 9.5, 10.0),
            new RangeAggregator.Range(null, 10.0, 10.5),
            new RangeAggregator.Range(null, 10.5, 11.0),
            new RangeAggregator.Range(null, 11.0, 11.5),
            new RangeAggregator.Range(null, 11.5, 12.0),
            new RangeAggregator.Range(null, 12.0, null)
        );
        testRanges(ranges, "manual_small_ranges");
    }

    @SuppressWarnings("rawtypes")
    private void testRanges(List<RangeAggregator.Range> ranges, String name) throws Exception {
        long absError = 0L;
        long docCount = 0L;
        for (int k = 0; k < 10; k++) {
            try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
                docCount += generateDocs(w);

                try (IndexReader reader = w.getReader()) {
                    IndexSearcher searcher = new IndexSearcher(reader);
                    RangeAggregationBuilder aggBuilder = new RangeAggregationBuilder("my_agg").field(HISTO_FIELD_NAME);
                    RangeAggregationBuilder rawFieldAgg = new RangeAggregationBuilder("my_agg").field(RAW_FIELD_NAME);
                    ranges.forEach(r -> {
                        aggBuilder.addRange(r);
                        rawFieldAgg.addRange(r);
                    });

                    InternalRange<? extends InternalRange.Bucket, ? extends InternalRange> range = searchAndReduce(
                        searcher,
                        new AggTestConfig(aggBuilder, defaultFieldType(HISTO_FIELD_NAME))
                    );
                    InternalRange<? extends InternalRange.Bucket, ? extends InternalRange> rawRange = searchAndReduce(
                        searcher,
                        new AggTestConfig(rawFieldAgg, defaultFieldType(RAW_FIELD_NAME))
                    );
                    for (int j = 0; j < rawRange.getBuckets().size(); j++) {
                        absError += Math.abs(range.getBuckets().get(j).getDocCount() - rawRange.getBuckets().get(j).getDocCount());
                    }
                }
            }
        }
        assertThat("test " + name, (double) absError / docCount, lessThan(0.1));
    }

    @SuppressWarnings("rawtypes")
    public void testOverlapping() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            w.addDocument(
                Arrays.asList(
                    histogramFieldDocValues(HISTO_FIELD_NAME, new double[] { 0, 1.2, 10, 12, 24 }, new int[] { 3, 1, 2, 4, 6 }),
                    new CustomTermFreqField("_doc_count", "_doc_count", 16)
                )
            );
            w.addDocument(
                Arrays.asList(
                    histogramFieldDocValues(HISTO_FIELD_NAME, new double[] { 5.3, 6, 6, 20 }, new int[] { 1, 3, 4, 5 }),
                    new CustomTermFreqField("_doc_count", "_doc_count", 13)
                )
            );
            w.addDocument(
                Arrays.asList(
                    histogramFieldDocValues(HISTO_FIELD_NAME, new double[] { -10, 0.01, 10, 10, 30 }, new int[] { 10, 2, 4, 14, 11 }),
                    new CustomTermFreqField("_doc_count", "_doc_count", 41)
                )
            );

            RangeAggregationBuilder aggBuilder = new RangeAggregationBuilder("my_agg").field(HISTO_FIELD_NAME)
                .addUnboundedTo(0)
                .addRange(5, 10)
                .addRange(7, 10)
                .addRange(0, 20)
                .addRange(0, 10)
                .addRange(10, 20)
                .addUnboundedFrom(20);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalRange<? extends InternalRange.Bucket, ? extends InternalRange> range = searchAndReduce(
                    searcher,
                    new AggTestConfig(aggBuilder, defaultFieldType(HISTO_FIELD_NAME))
                );
                assertTrue(AggregationInspectionHelper.hasValue(range));
                assertEquals(7, range.getBuckets().size());

                assertEquals(10, range.getBuckets().get(0).getDocCount());
                assertEquals("*-0.0", range.getBuckets().get(0).getKey());

                assertEquals(14, range.getBuckets().get(1).getDocCount());
                assertEquals("0.0-10.0", range.getBuckets().get(1).getKey());

                assertEquals(38, range.getBuckets().get(2).getDocCount());
                assertEquals("0.0-20.0", range.getBuckets().get(2).getKey());

                assertEquals(8, range.getBuckets().get(3).getDocCount());
                assertEquals("5.0-10.0", range.getBuckets().get(3).getKey());

                assertEquals(0, range.getBuckets().get(4).getDocCount());
                assertEquals("7.0-10.0", range.getBuckets().get(4).getKey());

                assertEquals(24, range.getBuckets().get(5).getDocCount());
                assertEquals("10.0-20.0", range.getBuckets().get(5).getKey());

                assertEquals(22, range.getBuckets().get(6).getDocCount());
                assertEquals("20.0-*", range.getBuckets().get(6).getKey());
            }
        }
    }

    @SuppressWarnings("rawtypes")
    public void testNonOverlapping() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            w.addDocument(
                Arrays.asList(
                    histogramFieldDocValues(HISTO_FIELD_NAME, new double[] { 0, 1.2, 10, 12, 24 }, new int[] { 3, 1, 2, 4, 6 }),
                    new CustomTermFreqField("_doc_count", "_doc_count", 16)
                )
            );
            w.addDocument(
                Arrays.asList(
                    histogramFieldDocValues(HISTO_FIELD_NAME, new double[] { 5.3, 6, 6, 20 }, new int[] { 1, 3, 4, 5 }),
                    new CustomTermFreqField("_doc_count", "_doc_count", 13)
                )
            );
            w.addDocument(
                Arrays.asList(
                    histogramFieldDocValues(HISTO_FIELD_NAME, new double[] { -10, 0.01, 10, 10, 30 }, new int[] { 10, 2, 4, 14, 11 }),
                    new CustomTermFreqField("_doc_count", "_doc_count", 41)
                )
            );

            RangeAggregationBuilder aggBuilder = new RangeAggregationBuilder("my_agg").field(HISTO_FIELD_NAME)
                .addUnboundedTo(0)
                .addRange(0, 10)
                .addRange(10, 20)
                .addUnboundedFrom(20);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalRange<? extends InternalRange.Bucket, ? extends InternalRange> range = searchAndReduce(
                    searcher,
                    new AggTestConfig(aggBuilder, defaultFieldType(HISTO_FIELD_NAME))
                );
                assertTrue(AggregationInspectionHelper.hasValue(range));
                assertEquals(4, range.getBuckets().size());

                assertEquals(10, range.getBuckets().get(0).getDocCount());
                assertEquals("*-0.0", range.getBuckets().get(0).getKey());

                assertEquals(14, range.getBuckets().get(1).getDocCount());
                assertEquals("0.0-10.0", range.getBuckets().get(1).getKey());

                assertEquals(24, range.getBuckets().get(2).getDocCount());
                assertEquals("10.0-20.0", range.getBuckets().get(2).getKey());

                assertEquals(22, range.getBuckets().get(3).getDocCount());
                assertEquals("20.0-*", range.getBuckets().get(3).getKey());
            }
        }
    }

    public void testSubAggs() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {

            w.addDocument(singleton(histogramFieldDocValues(HISTO_FIELD_NAME, new double[] { -4.5, 4.3 })));
            w.addDocument(singleton(histogramFieldDocValues(HISTO_FIELD_NAME, new double[] { -5, 3.2 })));

            RangeAggregationBuilder aggBuilder = new RangeAggregationBuilder("my_agg").field(HISTO_FIELD_NAME)
                .addRange(-1.0, 3.0)
                .subAggregation(new TopHitsAggregationBuilder("top_hits"));
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
                    searchAndReduce(searcher, new AggTestConfig(aggBuilder, defaultFieldType(HISTO_FIELD_NAME)));
                });
                assertEquals("Range aggregation on histogram fields does not support sub-aggregations", e.getMessage());
            }
        }
    }

    private long generateDocs(RandomIndexWriter w) throws Exception {
        double[] lows = new double[50];
        double[] mids = new double[50];
        double[] highs = new double[50];
        for (int j = 0; j < 50; j++) {
            lows[j] = randomDoubleBetween(0.0, 5.0, true);
            mids[j] = randomDoubleBetween(7.0, 9.0, false);
            highs[j] = randomDoubleBetween(10.0, 13.0, false);
            w.addDocument(singleton(new DoubleDocValuesField(RAW_FIELD_NAME, lows[j])));
            w.addDocument(singleton(new DoubleDocValuesField(RAW_FIELD_NAME, mids[j])));
            w.addDocument(singleton(new DoubleDocValuesField(RAW_FIELD_NAME, highs[j])));
        }
        w.addDocument(singleton(hdrHistogramFieldDocValues(HISTO_FIELD_NAME, lows)));
        w.addDocument(singleton(hdrHistogramFieldDocValues(HISTO_FIELD_NAME, mids)));
        w.addDocument(singleton(hdrHistogramFieldDocValues(HISTO_FIELD_NAME, highs)));
        return 150;
    }

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new AnalyticsPlugin());
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new RangeAggregationBuilder("_name").field(fieldName);
    }

    private MappedFieldType defaultFieldType(String fieldName) {
        if (fieldName.equals(HISTO_FIELD_NAME)) {
            return new HistogramFieldMapper.HistogramFieldType(fieldName, Collections.emptyMap());
        } else {
            return new NumberFieldMapper.NumberFieldType(fieldName, NumberFieldMapper.NumberType.DOUBLE);
        }
    }
}

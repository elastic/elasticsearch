/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.aggregations.bucket.histogram;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.index.mapper.CustomTermFreqField;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.histogram.DoubleBounds;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.elasticsearch.xpack.analytics.mapper.HistogramFieldMapper;

import java.util.Collections;
import java.util.List;

import static java.util.Collections.singleton;
import static org.elasticsearch.xpack.analytics.AnalyticsTestsUtils.histogramFieldDocValues;

public class HistoBackedHistogramAggregatorTests extends AggregatorTestCase {

    private static final String FIELD_NAME = "field";

    public void testHistograms() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            w.addDocument(singleton(histogramFieldDocValues(FIELD_NAME, new double[] { 0, 1.2, 10, 12, 24 })));
            w.addDocument(singleton(histogramFieldDocValues(FIELD_NAME, new double[] { 5.3, 6, 6, 20 })));
            w.addDocument(singleton(histogramFieldDocValues(FIELD_NAME, new double[] { -10, 0.01, 10, 10, 30 })));

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field(FIELD_NAME).interval(5);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, defaultFieldType(FIELD_NAME));
                assertEquals(9, histogram.getBuckets().size());
                assertEquals(-10d, histogram.getBuckets().get(0).getKey());
                assertEquals(1, histogram.getBuckets().get(0).getDocCount());
                assertEquals(-5d, histogram.getBuckets().get(1).getKey());
                assertEquals(0, histogram.getBuckets().get(1).getDocCount());
                assertEquals(0d, histogram.getBuckets().get(2).getKey());
                assertEquals(3, histogram.getBuckets().get(2).getDocCount());
                assertEquals(5d, histogram.getBuckets().get(3).getKey());
                assertEquals(3, histogram.getBuckets().get(3).getDocCount());
                assertEquals(10d, histogram.getBuckets().get(4).getKey());
                assertEquals(4, histogram.getBuckets().get(4).getDocCount());
                assertEquals(15d, histogram.getBuckets().get(5).getKey());
                assertEquals(0, histogram.getBuckets().get(5).getDocCount());
                assertEquals(20d, histogram.getBuckets().get(6).getKey());
                assertEquals(2, histogram.getBuckets().get(6).getDocCount());
                assertEquals(25d, histogram.getBuckets().get(7).getKey());
                assertEquals(0, histogram.getBuckets().get(7).getDocCount());
                assertEquals(30d, histogram.getBuckets().get(8).getKey());
                assertEquals(1, histogram.getBuckets().get(8).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        }
    }

    public void testMinDocCount() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            w.addDocument(singleton(histogramFieldDocValues(FIELD_NAME, new double[] { 0, 1.2, 10, 12, 24 })));
            w.addDocument(singleton(histogramFieldDocValues(FIELD_NAME, new double[] { 5.3, 6, 6, 20 })));
            w.addDocument(singleton(histogramFieldDocValues(FIELD_NAME, new double[] { -10, 0.01, 10, 10, 30, 90 })));

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field(FIELD_NAME).interval(5).minDocCount(2);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, defaultFieldType(FIELD_NAME));
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
        }
    }

    public void testHistogramWithDocCountField() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            w.addDocument(
                List.of(
                    // Add the _doc_dcount field
                    new CustomTermFreqField("_doc_count", "_doc_count", 8),
                    histogramFieldDocValues(FIELD_NAME, new double[] { 0, 1.2, 10, 10, 12, 24, 24, 24 })
                )
            );

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field(FIELD_NAME).interval(100);

            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, defaultFieldType(FIELD_NAME));
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
                assertEquals(8, histogram.getBuckets().get(0).getDocCount());
            }
        }
    }

    public void testRandomOffset() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            // Note, these values are carefully chosen to ensure that no matter what offset we pick, no two can end up in the same bucket
            w.addDocument(singleton(histogramFieldDocValues(FIELD_NAME, new double[] { 3.2, 9.3 })));
            w.addDocument(singleton(histogramFieldDocValues(FIELD_NAME, new double[] { -5, 3.2 })));

            final double offset = randomDouble();
            final double interval = 5;
            final double expectedOffset = offset % interval;
            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field(FIELD_NAME)
                .interval(interval)
                .offset(offset)
                .minDocCount(1);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, defaultFieldType(FIELD_NAME));

                assertEquals(3, histogram.getBuckets().size());
                assertEquals(-10 + expectedOffset, histogram.getBuckets().get(0).getKey());
                assertEquals(1, histogram.getBuckets().get(0).getDocCount());

                assertEquals(expectedOffset, histogram.getBuckets().get(1).getKey());
                assertEquals(2, histogram.getBuckets().get(1).getDocCount());

                assertEquals(5 + expectedOffset, histogram.getBuckets().get(2).getKey());
                assertEquals(1, histogram.getBuckets().get(2).getDocCount());

                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        }
    }

    public void testExtendedBounds() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {

            w.addDocument(singleton(histogramFieldDocValues(FIELD_NAME, new double[] { -4.5, 4.3 })));
            w.addDocument(singleton(histogramFieldDocValues(FIELD_NAME, new double[] { -5, 3.2 })));

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field(FIELD_NAME)
                .interval(5)
                .extendedBounds(-12, 13);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, defaultFieldType(FIELD_NAME));
                assertEquals(6, histogram.getBuckets().size());
                assertEquals(-15d, histogram.getBuckets().get(0).getKey());
                assertEquals(0, histogram.getBuckets().get(0).getDocCount());
                assertEquals(-10d, histogram.getBuckets().get(1).getKey());
                assertEquals(0, histogram.getBuckets().get(1).getDocCount());
                assertEquals(-5d, histogram.getBuckets().get(2).getKey());
                assertEquals(2, histogram.getBuckets().get(2).getDocCount());
                assertEquals(0d, histogram.getBuckets().get(3).getKey());
                assertEquals(2, histogram.getBuckets().get(3).getDocCount());
                assertEquals(5d, histogram.getBuckets().get(4).getKey());
                assertEquals(0, histogram.getBuckets().get(4).getDocCount());
                assertEquals(10d, histogram.getBuckets().get(5).getKey());
                assertEquals(0, histogram.getBuckets().get(5).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        }
    }

    public void testHardBounds() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {

            w.addDocument(singleton(histogramFieldDocValues(FIELD_NAME, new double[] { -4.5, 4.3 })));
            w.addDocument(singleton(histogramFieldDocValues(FIELD_NAME, new double[] { -5, 3.2 })));
            w.addDocument(singleton(histogramFieldDocValues(FIELD_NAME, new double[] { 1.0, 2.2 })));
            w.addDocument(singleton(histogramFieldDocValues(FIELD_NAME, new double[] { -6.0, 12.2 })));

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field(FIELD_NAME)
                .interval(5)
                .hardBounds(new DoubleBounds(0.0, 5.0));
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, defaultFieldType(FIELD_NAME));
                assertEquals(1, histogram.getBuckets().size());
                assertEquals(0d, histogram.getBuckets().get(0).getKey());
                assertEquals(4, histogram.getBuckets().get(0).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        }
    }

    /**
     * Test that sub-aggregations are not supported
     */
    public void testSubAggs() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {

            w.addDocument(singleton(histogramFieldDocValues(FIELD_NAME, new double[] { -4.5, 4.3 })));
            w.addDocument(singleton(histogramFieldDocValues(FIELD_NAME, new double[] { -5, 3.2 })));

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field(FIELD_NAME)
                .interval(5)
                .extendedBounds(-12, 13)
                .subAggregation(new TopHitsAggregationBuilder("top_hits"));
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);

                IllegalArgumentException e = expectThrows(
                    IllegalArgumentException.class,
                    () -> searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, defaultFieldType(FIELD_NAME))
                );

                assertEquals("Histogram aggregation on histogram fields does not support sub-aggregations", e.getMessage());
            }
        }
    }

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new AnalyticsPlugin());
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new HistogramAggregationBuilder("_name").field(fieldName);
    }

    private MappedFieldType defaultFieldType(String fieldName) {
        return new HistogramFieldMapper.HistogramFieldType(fieldName, Collections.emptyMap());
    }

}

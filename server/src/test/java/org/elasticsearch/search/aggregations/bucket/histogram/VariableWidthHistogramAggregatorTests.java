/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.metrics.InternalStats;
import org.elasticsearch.search.aggregations.metrics.StatsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.aggregations.support.ValueType;

import java.io.IOException;

public class VariableWidthHistogramAggregatorTests extends AggregatorTestCase {

    public void testNoDocs() throws Exception{
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            VariableWidthHistogramAggregationBuilder aggBuilder = new VariableWidthHistogramAggregationBuilder("my_agg")
                .field("field")
                .setNumBuckets(5)
                .setShardSize(5);


            MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
            fieldType.setName("field");
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalVariableWidthHistogram histogram = search(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);

                assertEquals(0, histogram.getBuckets().size());
                assertFalse(AggregationInspectionHelper.hasValue(histogram));
            }
        }
    }

    public void testMoreClustersThanDocs() throws Exception {
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (long value : new long[] {3, 10, -200}) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("field", value));
                w.addDocument(doc);
            }

            VariableWidthHistogramAggregationBuilder aggBuilder = new VariableWidthHistogramAggregationBuilder("my_agg")
                .field("field")
                .setNumBuckets(4)
                .setShardSize(4);

            // Each document should have its own cluster, since shard_size > # docs
            MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
            fieldType.setName("field");
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalVariableWidthHistogram histogram = search(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);

                double double_error = 1d / 10000d;
                assertEquals(3, histogram.getBuckets().size());
                assertEquals(-200, histogram.getBuckets().get(0).centroid(), double_error);
                assertEquals(1, histogram.getBuckets().get(0).getDocCount());
                assertEquals(3, histogram.getBuckets().get(1).centroid(), double_error);
                assertEquals(1, histogram.getBuckets().get(1).getDocCount());
                assertEquals(10, histogram.getBuckets().get(2).centroid(), double_error);
                assertEquals(1, histogram.getBuckets().get(2).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        }
    }

    public void testLongs() throws Exception {
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (long value : new long[] {-3, 1, 5, -2, 4}) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("field", value));
                w.addDocument(doc);
            }

            VariableWidthHistogramAggregationBuilder aggBuilder = new VariableWidthHistogramAggregationBuilder("my_agg")
                .field("field")
                .setNumBuckets(3)
                .setShardSize(6)
                .setCacheLimit(3);

            // Expected clusters: [ (-3, -2), (1), (4, 5) ]
            // Corresponding keys (centroids): [ -2.5, 1, 4.5 ]
            MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
            fieldType.setName("field");
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalVariableWidthHistogram histogram = search(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);

                double double_error = 1d / 10000d;
                assertEquals(3, histogram.getBuckets().size());
                assertEquals(-2.5, histogram.getBuckets().get(0).centroid(), double_error);
                assertEquals(2, histogram.getBuckets().get(0).getDocCount());
                assertEquals(1, histogram.getBuckets().get(1).centroid(), double_error);
                assertEquals(1, histogram.getBuckets().get(1).getDocCount());
                assertEquals(4.5, histogram.getBuckets().get(2).centroid(), double_error);
                assertEquals(2, histogram.getBuckets().get(2).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        }
    }

    public void testDoubles() throws Exception {
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (double value : new double[] {1.2, 3.3, 5.3, 8.8, 2.26, -0.4, 5.9}) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("field", NumericUtils.doubleToSortableLong(value)));
                w.addDocument(doc);
            }

            VariableWidthHistogramAggregationBuilder aggBuilder = new VariableWidthHistogramAggregationBuilder("my_agg")
                .field("field")
                .setNumBuckets(2) // This value is only used at reduce time
                .setShardSize(6)
                .setCacheLimit(4);

            // (Cache limit < shard size) is illogical, but it simplifies testing
            // Once the cache is full, the algorithm creates (3/4 * shard_size = 4) initial buckets.
            // So, there will initially be a bucket for each of the first 4 values

            // Expected clusters: [ (-0.4, 1.2), (2.26, 3.3), (5.3, 5.9) (8.8)]
            // Corresponding keys (centroids): [ 0.4, 2.78, 5.6, 8.8]
            MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.DOUBLE);
            fieldType.setName("field");
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalVariableWidthHistogram histogram = search(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);

                double double_error = 1d / 10000d;
                assertEquals(4, histogram.getBuckets().size());
                assertEquals(0.4, histogram.getBuckets().get(0).centroid(), double_error);
                assertEquals(2, histogram.getBuckets().get(0).getDocCount());
                assertEquals(2.78, histogram.getBuckets().get(1).centroid(), double_error);
                assertEquals(2, histogram.getBuckets().get(1).getDocCount());
                assertEquals(5.6, histogram.getBuckets().get(2).centroid(), double_error);
                assertEquals(2, histogram.getBuckets().get(2).getDocCount());
                assertEquals(8.8, histogram.getBuckets().get(3).centroid(), double_error);
                assertEquals(1, histogram.getBuckets().get(3).getDocCount(), double_error);
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        }
    }

    // Once the cache limit is reached, cached documents are collected into (3/4 * shard_size) buckets
    // A new bucket should be added when there is a document that is distant from all existing buckets
    public void testNewBucketCreation() throws Exception {
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (double value : new double[] {-1, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 40, 30, 25, 32, 38, 80, 50, 75}) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("field", NumericUtils.doubleToSortableLong(value)));
                w.addDocument(doc);
            }

            VariableWidthHistogramAggregationBuilder aggBuilder = new VariableWidthHistogramAggregationBuilder("my_agg")
                .field("field")
                .setNumBuckets(2) // This value is only used at reduce time
                .setShardSize(16) // 3/4 * 16 = 12, so 12 buckets will initially be created from the cached documents
                .setCacheLimit(12);

            // Expected clusters: [ (-1), (1), (3), (5), (7), (9), (11), (13), (15), (17),
            //                      (19), (25, 30, 32), (38, 40), (50), (75, 80) ]
            // Corresponding keys (centroids): [ -1, 1, 3, ..., 17, 19, 29, 39, 50, 77.5]
            // Note: New buckets are created for 30, 50, and 80 because they are distant from the other buckets
            MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.DOUBLE);
            fieldType.setName("field");
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalVariableWidthHistogram histogram = search(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);

                double double_error = 1d / 10000d;
                assertEquals(15, histogram.getBuckets().size());
                for(int i = 0; i <= 10; i++){
                    double expectedKey = (2 * i) - 1;
                    assertEquals(expectedKey, histogram.getBuckets().get(i).centroid(), double_error);
                    assertEquals(1, histogram.getBuckets().get(i).getDocCount());
                }
                assertEquals(29d, histogram.getBuckets().get(11).centroid(), double_error);
                assertEquals(3, histogram.getBuckets().get(11).getDocCount());
                assertEquals(39d, histogram.getBuckets().get(12).centroid(), double_error);
                assertEquals(2, histogram.getBuckets().get(12).getDocCount());
                assertEquals(50d, histogram.getBuckets().get(13).centroid(), double_error);
                assertEquals(1, histogram.getBuckets().get(13).getDocCount());
                assertEquals(77.5d, histogram.getBuckets().get(14).centroid(), double_error);
                assertEquals(2, histogram.getBuckets().get(14).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        }
    }

    // There should not be more than `shard_size` documents on a node, even when very distant documents appear
    public void testNewBucketLimit() throws Exception{
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (double value : new double[] {1,2,3,4,5, 10, 20, 50, 100, 5400, -900}) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("field", NumericUtils.doubleToSortableLong(value)));
                w.addDocument(doc);
            }

            VariableWidthHistogramAggregationBuilder aggBuilder = new VariableWidthHistogramAggregationBuilder("my_agg")
                .field("field")
                .setNumBuckets(2) // This value is only used at reduce time
                .setShardSize(4)
                .setCacheLimit(5);

            // Expected clusters: [ (-900, 1, 2), (3, 4), (5), (10, 20, 50, 100, 5400)]
            // Corresponding keys (centroids): [ -299, 3.5, 5, 1116]
            MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.DOUBLE);
            fieldType.setName("field");
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalVariableWidthHistogram histogram = search(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);

                double double_error = 1d / 10000d;
                assertEquals(4, histogram.getBuckets().size());
                assertEquals(-299, histogram.getBuckets().get(0).centroid(), double_error);
                assertEquals(3, histogram.getBuckets().get(0).getDocCount());
                assertEquals(3.5, histogram.getBuckets().get(1).centroid(), double_error);
                assertEquals(2, histogram.getBuckets().get(1).getDocCount());
                assertEquals(5, histogram.getBuckets().get(2).centroid(), double_error);
                assertEquals(1, histogram.getBuckets().get(2).getDocCount());
                assertEquals(1116, histogram.getBuckets().get(3).centroid(), double_error);
                assertEquals(5, histogram.getBuckets().get(3).getDocCount(), double_error);
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        }
    }

    public void testSimpleSubAggregations() throws IOException{
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (long value : new long[] {1, 5, 9, 2, 8}) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("field", value));
                w.addDocument(doc);
            }

            VariableWidthHistogramAggregationBuilder aggBuilder = new VariableWidthHistogramAggregationBuilder("my_agg")
                .field("field")
                .setNumBuckets(2)
                .setCacheLimit(3)
                .setShardSize(4)
                .subAggregation(AggregationBuilders.stats("stats").field("field"));

            // Expected clusters: [ (1, 2), (5), (8,9) ]
            MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
            fieldType.setName("field");
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalVariableWidthHistogram histogram = search(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
                double delta_error = 1d/10000d;

                InternalStats stats = histogram.getBuckets().get(0).getAggregations().get("stats");
                assertEquals(1d, stats.getMin(), delta_error);
                assertEquals(2d, stats.getMax(), delta_error);
                assertEquals(2L, stats.getCount());
                assertTrue(AggregationInspectionHelper.hasValue(stats));

                stats = histogram.getBuckets().get(1).getAggregations().get("stats");
                assertEquals(5d, stats.getMin(), delta_error);
                assertEquals(5d, stats.getMax(), delta_error);
                assertEquals(1, stats.getCount());
                assertTrue(AggregationInspectionHelper.hasValue(stats));

                stats = histogram.getBuckets().get(2).getAggregations().get("stats");
                assertEquals(8d, stats.getMin(), delta_error);
                assertEquals(9d, stats.getMax(), delta_error);
                assertEquals(2L, stats.getCount());
                assertTrue(AggregationInspectionHelper.hasValue(stats));
            }
        }
    }

    public void testComplexSubAggregations() throws IOException{
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (long value : new long[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("field", value));
                w.addDocument(doc);
            }

            VariableWidthHistogramAggregationBuilder aggBuilder = new VariableWidthHistogramAggregationBuilder("my_agg")
                .field("field")
                .setNumBuckets(3)
                .setCacheLimit(12)
                .setShardSize(4)
                .subAggregation(new StatsAggregationBuilder("stats").field("field"));

            // Expected clusters: [ (0, 1, 2, 3), (4, 5, 6, 7), (8, 9, 10, 11) ]
            MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
            fieldType.setName("field");
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalVariableWidthHistogram histogram = search(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
                double delta_error = 1d/10000d;

                InternalStats stats = histogram.getBuckets().get(0).getAggregations().get("stats");
                assertEquals(0d, stats.getMin(), delta_error);
                assertEquals(3L, stats.getMax(), delta_error);
                assertEquals(4L, stats.getCount());
                assertTrue(AggregationInspectionHelper.hasValue(stats));

                stats = histogram.getBuckets().get(1).getAggregations().get("stats");
                assertEquals(4d, stats.getMin(), delta_error);
                assertEquals(7d, stats.getMax(), delta_error);
                assertEquals(4L, stats.getCount());
                assertTrue(AggregationInspectionHelper.hasValue(stats));

                stats = histogram.getBuckets().get(2).getAggregations().get("stats");
                assertEquals(8d, stats.getMin(), delta_error);
                assertEquals(11d, stats.getMax(), delta_error);
                assertEquals(4L, stats.getCount());
                assertTrue(AggregationInspectionHelper.hasValue(stats));
            }
        }
    }

    // NOCOMMIT - This is a WIP!
    /*
    public void testDepthFirstModeFail() throws IOException{
        // VariableWidthHistogram should throw an `IllegalStateException` when it is forced to execute in depth_first mode
        // WIP
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (long value : new long[] {0, 1, 2, 3, 4, 5}) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("field", value));
                w.addDocument(doc);
            }

            TermsAggregationBuilder aggBuilder = new TermsAggregationBuilder("my_agg_2", ValueType.LONG)
                .field("field")
                .collectMode(Aggregator.SubAggCollectionMode.DEPTH_FIRST)
                .subAggregation(new VariableWidthHistogramAggregationBuilder("agg_3").field("field_2").setNumBuckets(5));

            MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
            fieldType.setName("field");
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                //TermsAggregator histogram = search(searcher, new MatchAllDocsQuery(), aggBuilder);

                TermsAggregator aggregator = super.createAggregator(aggBuilder, searcher, fieldType);
                aggregator.preCollection();
                searcher.search(new MatchAllDocsQuery(), aggregator);
                aggregator.postCollection();
                Terms result = (Terms) aggregator.buildAggregation(0L);

                assert(1 == 2); // We should never get here. An exception should have been thrown
            } catch (IllegalStateException e){ }
        }
    }
    */
}

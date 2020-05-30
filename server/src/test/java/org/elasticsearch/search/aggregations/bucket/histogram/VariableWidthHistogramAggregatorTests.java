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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.metrics.InternalStats;
import org.elasticsearch.search.aggregations.metrics.StatsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class VariableWidthHistogramAggregatorTests extends AggregatorTestCase {

    private static final String INSTANT_FIELD = "instant";
    private static final String NUMERIC_FIELD = "numeric";

    private static final Query DEFAULT_QUERY = new MatchAllDocsQuery();

    public void testNoDocs() throws Exception{
        final List<Number> dataset = Arrays.asList();
        testBothCases(DEFAULT_QUERY, dataset,
            aggregation -> aggregation.field(NUMERIC_FIELD).setNumBuckets(2).setShardSize(6).setCacheLimit(4),
            histogram -> {
                final List<InternalVariableWidthHistogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(0, buckets.size());
            });
    }

    public void testMoreClustersThanDocs() throws Exception {
        final List<Number> dataset = Arrays.asList(-3L, 10L, -200L);
        double doubleError = 1d / 10000d;

        // Each document should have its own cluster, since shard_size > # docs
        final Map<Double, Integer> expectedDocCount = new HashMap<>();
        expectedDocCount.put(-200d, 1);
        expectedDocCount.put(-3d, 1);
        expectedDocCount.put(10d, 1);
        final Map<Double, Double> expectedCentroids = new HashMap<>();
        expectedCentroids.put(-200d, -200d);
        expectedCentroids.put(-3d, -3d);
        expectedCentroids.put(10d, 10d);

        testBothCases(DEFAULT_QUERY, dataset,
            aggregation -> aggregation.field(NUMERIC_FIELD).setNumBuckets(4).setShardSize(4),
            histogram -> {
                final List<InternalVariableWidthHistogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(expectedDocCount.size(), buckets.size());
                buckets.forEach(bucket -> {
                    assertEquals(expectedDocCount.getOrDefault(bucket.getKey(), 0).longValue(), bucket.getDocCount());
                    assertEquals(expectedCentroids.getOrDefault(bucket.getKey(), 0d).doubleValue(), bucket.centroid(), doubleError);
                });
            });
    }

    public void testLongs() throws Exception {
        final List<Number> dataset = Arrays.asList(-3L, 1L, 5L, -2L, 4L);
        double doubleError = 1d / 10000d;

        // Expected clusters: [ (-3, -2), (1), (4, 5) ]
        // Corresponding keys (centroids): [ -2.5, 1, 4.5 ]

        final Map<Double, Integer> expectedDocCount = new HashMap<>();
        expectedDocCount.put(-3d, 2);
        expectedDocCount.put(1d, 1);
        expectedDocCount.put(4d, 2);
        final Map<Double, Double> expectedCentroids = new HashMap<>();
        expectedCentroids.put(-3d, -2.5);
        expectedCentroids.put(1d, 1d);
        expectedCentroids.put(4d, 4.5);

        testBothCases(DEFAULT_QUERY, dataset,
            aggregation -> aggregation.field(NUMERIC_FIELD).setNumBuckets(3).setShardSize(6).setCacheLimit(3),
            histogram -> {
                final List<InternalVariableWidthHistogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(expectedDocCount.size(), buckets.size());
                buckets.forEach(bucket -> {
                    assertEquals(expectedDocCount.getOrDefault(bucket.getKey(), 0).longValue(), bucket.getDocCount());
                    assertEquals(expectedCentroids.getOrDefault(bucket.getKey(), 0d).doubleValue(), bucket.centroid(), doubleError);
                });
            });
    }

    public void testDoubles() throws Exception {
        final List<Number> dataset = Arrays.asList(1.2, 3.3, 5.3, 8.8, 2.26, -0.4, 5.9);
        double doubleError = 1d / 10000d;

        // Search (no reduce)

        // (Cache limit < shard size) is illogical, but it simplifies testing
        // Once the cache is full, the algorithm creates (3/4 * shard_size = 4) initial buckets.
        // So, there will initially be a bucket for each of the first 4 values
        // Expected clusters from search: [ (-0.4, 1.2), (2.26, 3.3), (5.3, 5.9) (8.8)]
        // Corresponding keys (centroids): [ 0.4, 2.78, 5.6, 8.8]
        final Map<Double, Integer> expectedDocCountOnlySearch = new HashMap<>();
        expectedDocCountOnlySearch.put(-0.4, 2);
        expectedDocCountOnlySearch.put(2.26, 2);
        expectedDocCountOnlySearch.put(5.3, 2);
        expectedDocCountOnlySearch.put(8.8, 1);
        final Map<Double, Double> expectedCentroidsOnlySearch = new HashMap<>();
        expectedCentroidsOnlySearch.put(-0.4, 0.4);
        expectedCentroidsOnlySearch.put(2.26, 2.78);
        expectedCentroidsOnlySearch.put(5.3, 5.6);
        expectedCentroidsOnlySearch.put(8.8, 8.8);

        testSearchCase(DEFAULT_QUERY, dataset,
            aggregation -> aggregation.field(NUMERIC_FIELD).setNumBuckets(2).setShardSize(6).setCacheLimit(4),
            histogram -> {
                final List<InternalVariableWidthHistogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(expectedDocCountOnlySearch.size(), buckets.size());
                buckets.forEach(bucket -> {
                    assertEquals(expectedDocCountOnlySearch.getOrDefault(bucket.getKey(), 0).longValue(), bucket.getDocCount(), doubleError);
                    assertEquals(expectedCentroidsOnlySearch.getOrDefault(bucket.getKey(), 0d).doubleValue(), bucket.centroid(), doubleError);
                });
            });

        // Search + Reduce

        // Before reducing we have one bucket per doc
        // Expected clusters after reduce: [ (-0.4) , (1.2, 2.26, 3.3), (5.3, 5.9) (8.8)]
        // Corresponding keys (centroids): [ -0.4, (1.2+2.26+3.3)/3, 5.6, 8.8]
        final Map<Double, Integer> expectedDocCountSearchReduce = new HashMap<>();
        expectedDocCountSearchReduce.put(-0.4, 1);
        expectedDocCountSearchReduce.put(1.2, 3);
        expectedDocCountSearchReduce.put(5.3, 2);
        expectedDocCountSearchReduce.put(8.8, 1);
        final Map<Double, Double> expectedCentroidsSearchReduce = new HashMap<>();
        expectedCentroidsSearchReduce.put(-0.4, -0.4);
        expectedCentroidsSearchReduce.put(1.2, (1.2 + 2.26 + 3.3) / 3);
        expectedCentroidsSearchReduce.put(5.3, 5.6);
        expectedCentroidsSearchReduce.put(8.8, 8.8);

        testSearchAndReduceCase(DEFAULT_QUERY, dataset,
            aggregation -> aggregation.field(NUMERIC_FIELD).setNumBuckets(4).setShardSize(6).setCacheLimit(4),
            histogram -> {
                final List<InternalVariableWidthHistogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(expectedDocCountSearchReduce.size(), buckets.size());
                buckets.forEach(bucket -> {
                    assertEquals(expectedDocCountSearchReduce.getOrDefault(bucket.getKey(), 0).longValue(), bucket.getDocCount());
                    assertEquals(expectedCentroidsSearchReduce.getOrDefault(bucket.getKey(), 0d).doubleValue(), bucket.centroid(), doubleError);
                });
            });
    }

    // Once the cache limit is reached, cached documents are collected into (3/4 * shard_size) buckets
    // A new bucket should be added when there is a document that is distant from all existing buckets
    public void testNewBucketCreation() throws Exception {
        final List<Number> dataset = Arrays.asList(-1, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 40, 30, 25, 32, 38, 80, 50, 75);
        double doubleError = 1d / 10000d;

        // Search (no reduce)

        // Expected clusters: [ (-1), (1), (3), (5), (7), (9), (11), (13), (15), (17),
        //                      (19), (25, 30, 32), (38, 40), (50), (75, 80) ]
        // Corresponding keys (centroids): [ -1, 1, 3, ..., 17, 19, 29, 39, 50, 77.5]
        // Note: New buckets are created for 30, 50, and 80 because they are distant from the other buckets
        final List<Double> keys = Arrays.asList(-1d, 1d, 3d, 5d, 7d, 9d, 11d, 13d, 15d, 17d, 19d, 25d, 38d, 50d, 75d);
        final List<Integer> docCounts = Arrays.asList(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3, 2, 1, 2);
        final List<Double> centroids = Arrays.asList(-1d, 1d, 3d, 5d, 7d, 9d, 11d, 13d, 15d, 17d, 19d, 29d, 39d, 50d, 77.5d);
        assert keys.size() == docCounts.size() && keys.size() == centroids.size();

        final Map<Double, Integer> expectedDocCountOnlySearch = new HashMap<>();
        final Map<Double, Double> expectedCentroidsOnlySearch = new HashMap<>();
        for(int i=0; i<keys.size(); i++){
            expectedDocCountOnlySearch.put(keys.get(i), docCounts.get(i));
            expectedCentroidsOnlySearch.put(keys.get(i), centroids.get(i));
        }

        // Search + Reduce

        // Expected clusters: [ (-1, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19), (25, 30, 32, 38, 40, 50), (75, 80) ]
        // Corresponding centroids: [ 9, (25 + 30 + 32 + 38 + 40 + 50)/6, 77.5]
        final Map<Double, Integer> expectedDocCountSearchReduce = new HashMap<>();
        expectedDocCountSearchReduce.put(-1d, 11);
        expectedDocCountSearchReduce.put(25d, 6);
        expectedDocCountSearchReduce.put(75d, 2);
        final Map<Double, Double> expectedCentroidsSearchReduce = new HashMap<>();
        expectedCentroidsSearchReduce.put(-1d, 9d);
        expectedCentroidsSearchReduce.put(25d, (25 + 30 + 32 + 38 + 40 + 50)/6d);
        expectedCentroidsSearchReduce.put(75d, 77.5);


        testSearchCase(DEFAULT_QUERY, dataset,
            aggregation -> aggregation.field(NUMERIC_FIELD).setNumBuckets(2).setShardSize(16).setCacheLimit(12),
            histogram -> {
                final List<InternalVariableWidthHistogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(expectedDocCountOnlySearch.size(), buckets.size());
                buckets.forEach(bucket -> {
                    assertEquals(expectedDocCountOnlySearch.getOrDefault(bucket.getKey(), 0).longValue(), bucket.getDocCount());
                    assertEquals(expectedCentroidsOnlySearch.getOrDefault(bucket.getKey(), 0d).doubleValue(), bucket.centroid(), doubleError);
                });
            });

        testSearchAndReduceCase(DEFAULT_QUERY, dataset,
            aggregation -> aggregation.field(NUMERIC_FIELD).setNumBuckets(3).setShardSize(16).setCacheLimit(12),
            histogram -> {
                final List<InternalVariableWidthHistogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(expectedDocCountSearchReduce.size(), buckets.size());
                buckets.forEach(bucket -> {
                    assertEquals(expectedDocCountSearchReduce.getOrDefault(bucket.getKey(), 0).longValue(), bucket.getDocCount());
                    assertEquals(expectedCentroidsSearchReduce.getOrDefault(bucket.getKey(), 0d).doubleValue(), bucket.centroid(), doubleError);
                });
            });
    }

    // There should not be more than `shard_size` documents on a node, even when very distant documents appear
    public void testNewBucketLimit() throws Exception{
        final List<Number> dataset =  Arrays.asList(1,2,3,4,5, 10, 20, 50, 100, 5400, -900);
        double doubleError = 1d / 10000d;

        // Expected clusters: [ (-900, 1, 2), (3, 4), (5), (10, 20, 50, 100, 5400)]
        // Corresponding keys (centroids): [ -299, 3.5, 5, 1116]
        final Map<Double, Integer> expectedDocCount = new HashMap<>();
        expectedDocCount.put(-900d, 3);
        expectedDocCount.put(3d, 2);
        expectedDocCount.put(5d, 1);
        expectedDocCount.put(10d, 5);

        final Map<Double, Double> expectedCentroids = new HashMap<>();
        expectedCentroids.put(-900d, -299d);
        expectedCentroids.put(3d, 3.5d);
        expectedCentroids.put(5d, 5d);
        expectedCentroids.put(10d, 1116d);

        testSearchCase(DEFAULT_QUERY, dataset,
            aggregation -> aggregation.field(NUMERIC_FIELD) .setNumBuckets(2).setShardSize(4).setCacheLimit(5),
            histogram -> {
                final List<InternalVariableWidthHistogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(expectedDocCount.size(), buckets.size());
                buckets.forEach(bucket -> {
                    assertEquals(expectedDocCount.getOrDefault(bucket.getKey(), 0).longValue(), bucket.getDocCount());
                    assertEquals(expectedCentroids.getOrDefault(bucket.getKey(), 0d).doubleValue(), bucket.centroid(), doubleError);
                });
            });

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

    private void testSearchCase(final Query query, final List<Number> dataset,
                                final Consumer<VariableWidthHistogramAggregationBuilder> configure,
                                final Consumer<InternalVariableWidthHistogram> verify) throws IOException {
        executeTestCase(false, query, dataset, configure, verify);
    }


    private void testSearchAndReduceCase(final Query query, final List<Number> dataset,
                                         final Consumer<VariableWidthHistogramAggregationBuilder> configure,
                                         final Consumer<InternalVariableWidthHistogram> verify) throws IOException {
        executeTestCase(true, query, dataset, configure, verify);
    }

    private void testBothCases(final Query query, final List<Number> dataset,
                                         final Consumer<VariableWidthHistogramAggregationBuilder> configure,
                                         final Consumer<InternalVariableWidthHistogram> verify) throws IOException {
        executeTestCase(true, query, dataset, configure, verify);
        executeTestCase(false, query, dataset, configure, verify);
    }

    @Override
    protected IndexSettings createIndexSettings() {
        final Settings nodeSettings = Settings.builder()
            .put("search.max_buckets", 25000).build();
        return new IndexSettings(
            IndexMetadata.builder("_index").settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .creationDate(System.currentTimeMillis())
                .build(),
            nodeSettings
        );
    }

    private void executeTestCase(final boolean reduced, final Query query, final List<Number> dataset,
                                 final Consumer<VariableWidthHistogramAggregationBuilder> configure,
                                 final Consumer<InternalVariableWidthHistogram> verify) throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                indexSampleData(dataset, indexWriter);
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                final IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                final VariableWidthHistogramAggregationBuilder aggregationBuilder = new VariableWidthHistogramAggregationBuilder("_name");
                if (configure != null) {
                    configure.accept(aggregationBuilder);
                }

                final NumberFieldMapper.Builder builder;
                if(dataset.size() == 0 || dataset.get(0) instanceof Double){
                    builder = new NumberFieldMapper.Builder("_name", NumberFieldMapper.NumberType.DOUBLE);
                } else if(dataset.get(0) instanceof Long){
                    builder = new NumberFieldMapper.Builder("_name", NumberFieldMapper.NumberType.LONG);
                } else if (dataset.get(0) instanceof Integer){
                    builder = new NumberFieldMapper.Builder("_name", NumberFieldMapper.NumberType.INTEGER);
                } else {
                    throw new IOException("Test data has an invalid type");
                }

                final MappedFieldType fieldType = builder.fieldType();
                fieldType.setHasDocValues(true);
                fieldType.setName(aggregationBuilder.field());

                final InternalVariableWidthHistogram histogram;
                if (reduced) {
                    histogram = searchAndReduce(indexSearcher, query, aggregationBuilder, fieldType);
                } else {
                    histogram = search(indexSearcher, query, aggregationBuilder, fieldType);
                }
                verify.accept(histogram);
            }
        }
    }

    private void indexSampleData(List<Number> dataset, RandomIndexWriter indexWriter) throws IOException {
        final Document document = new Document();
        int i = 0;
        for (final Number doc : dataset) {
            if (frequently()) {
                indexWriter.commit();
            }

            long fieldVal;
            if(doc instanceof Double){
                fieldVal =  NumericUtils.doubleToSortableLong(doc.doubleValue());
            } else if(doc instanceof Integer) {
                fieldVal =  doc.intValue();
            } else if(doc instanceof Long){
                fieldVal = doc.longValue();
            } else {
                throw new IOException("Test data has an invalid type");
            }

            document.add(new SortedNumericDocValuesField(NUMERIC_FIELD, fieldVal));
            indexWriter.addDocument(document);
            document.clear();
            i += 1;
        }
    }


}

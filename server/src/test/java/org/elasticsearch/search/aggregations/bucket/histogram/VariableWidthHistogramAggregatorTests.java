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
import org.elasticsearch.search.aggregations.bucket.terms.InternalTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalStats;
import org.elasticsearch.search.aggregations.metrics.StatsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class VariableWidthHistogramAggregatorTests extends AggregatorTestCase {

    private static final String NUMERIC_FIELD = "numeric";

    private static final Query DEFAULT_QUERY = new MatchAllDocsQuery();
    private VariableWidthHistogramAggregationBuilder aggregationBuilder;

    public void testNoDocs() throws Exception{
        final List<Number> dataset = Arrays.asList();
        testBothCases(DEFAULT_QUERY, dataset, true,
            aggregation -> aggregation.field(NUMERIC_FIELD).setNumBuckets(2).setShardSize(6).setInitialBuffer(4),
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
        final Map<Double, Double> expectedMins = new HashMap<>();
        expectedMins.put(-200d, -200d);
        expectedMins.put(-3d, -3d);
        expectedMins.put(10d, 10d);

        testBothCases(DEFAULT_QUERY, dataset, true,
            aggregation -> aggregation.field(NUMERIC_FIELD).setNumBuckets(4).setShardSize(4),
            histogram -> {
                final List<InternalVariableWidthHistogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(expectedDocCount.size(), buckets.size());
                buckets.forEach(bucket -> {
                    assertEquals(expectedDocCount.getOrDefault(bucket.getKey(), 0).longValue(), bucket.getDocCount());
                    assertEquals(expectedMins.getOrDefault(bucket.getKey(), 0d).doubleValue(), bucket.min(), doubleError);
                });
            });
    }

    public void testLongs() throws Exception {
        final List<Number> dataset = Arrays.asList(5L, -3L, 1L, -2L, 4L);
        double doubleError = 1d / 10000d;

        // Expected clusters: [ (-3, -2), (1), (4, 5) ]
        // Corresponding keys (centroids): [ -2.5, 1, 4.5 ]

        final Map<Double, Integer> expectedDocCount = new HashMap<>();
        expectedDocCount.put(-2.5, 2);
        expectedDocCount.put(1d, 1);
        expectedDocCount.put(4.5, 2);

        final Map<Double, Double> expectedMins = new HashMap<>();
        expectedMins.put(-2.5, -3d);
        expectedMins.put(1d, 1d);
        expectedMins.put(4.5, 4d);

        final Map<Double, Double> expectedMaxes = new HashMap<>();
        expectedMaxes.put(-2.5, -2d);
        expectedMaxes.put(1d, 1d);
        expectedMaxes.put(4.5, 5d);

        testSearchCase(DEFAULT_QUERY, dataset, false,
            aggregation -> aggregation.field(NUMERIC_FIELD).setNumBuckets(3).setShardSize(6).setInitialBuffer(3),
            histogram -> {
                final List<InternalVariableWidthHistogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(expectedDocCount.size(), buckets.size());
                buckets.forEach(bucket -> {
                    assertEquals(expectedDocCount.getOrDefault(bucket.getKey(), 0).longValue(), bucket.getDocCount());
                    assertEquals(expectedMins.getOrDefault(bucket.getKey(), 0d).doubleValue(), bucket.min(), doubleError);
                    assertEquals(expectedMaxes.getOrDefault(bucket.getKey(), 0d).doubleValue(), bucket.max(), doubleError);
                });
            });
    }

    public void testDoubles() throws Exception {
        final List<Number> dataset = Arrays.asList(3.3, 8.8, 1.2, 5.3, 2.26, -0.4, 5.9);
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

        // Index these by min, rather than centroid, to avoid slight precision errors (Ex. 3.999999 vs 4.0) that arise from double division
        final Map<Double, Double> expectedCentroidsOnlySearch = new HashMap<>();
        expectedCentroidsOnlySearch.put(-0.4, 0.4);
        expectedCentroidsOnlySearch.put(2.26, 2.78);
        expectedCentroidsOnlySearch.put(5.3, 5.6);
        expectedCentroidsOnlySearch.put(8.8, 8.8);
        final Map<Double, Double> expectedMaxesOnlySearch = new HashMap<>();
        expectedMaxesOnlySearch.put(-0.4, 1.2);
        expectedMaxesOnlySearch.put(2.26, 3.3);
        expectedMaxesOnlySearch.put(5.3, 5.9);
        expectedMaxesOnlySearch.put(8.8, 8.8);

        testSearchCase(DEFAULT_QUERY, dataset, false,
            aggregation -> aggregation.field(NUMERIC_FIELD).setNumBuckets(2).setShardSize(6).setInitialBuffer(4),
            histogram -> {
                final List<InternalVariableWidthHistogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(expectedCentroidsOnlySearch.size(), buckets.size());
                buckets.forEach(bucket -> {
                    assertEquals(expectedDocCountOnlySearch.getOrDefault(bucket.min(), 0).longValue(), bucket.getDocCount(), doubleError);
                    assertEquals(expectedCentroidsOnlySearch.getOrDefault(bucket.min(), 0d).doubleValue(), bucket.centroid(), doubleError);
                    assertEquals(expectedMaxesOnlySearch.getOrDefault(bucket.min(), 0d).doubleValue(), bucket.max(), doubleError);
                });
            });

        // Search + Reduce

        // Before reducing we have one bucket per doc
        // Expected clusters from search: [ (-0.4, 1.2), (2.26, 3.3), (5.3, 5.9) (8.8)]
        // Corresponding keys (centroids): [ 0.4, 2.78, 5.6, 8.8]
        final Map<Double, Integer> expectedDocCountSearchReduce = new HashMap<>();
        expectedDocCountSearchReduce.put(-0.4, 2);
        expectedDocCountSearchReduce.put(2.26, 2);
        expectedDocCountSearchReduce.put(5.3, 2);
        expectedDocCountSearchReduce.put(8.8, 1);

        // Indexed by min
        final Map<Double, Double> expectedCentroidsSearchReduce = new HashMap<>();
        expectedCentroidsSearchReduce.put(-0.4, 0.4);
        expectedCentroidsSearchReduce.put(2.26,2.78);
        expectedCentroidsSearchReduce.put(5.3, 5.6);
        expectedCentroidsSearchReduce.put(8.8, 8.8);
        final Map<Double, Double> expectedMaxesSearchReduce = new HashMap<>();
        expectedMaxesSearchReduce.put(-0.4, 1.2);
        expectedMaxesSearchReduce.put(2.26,3.3);
        expectedMaxesSearchReduce.put(5.3, 5.9);
        expectedMaxesSearchReduce.put(8.8, 8.8);

        testSearchAndReduceCase(DEFAULT_QUERY, dataset, false,
            aggregation -> aggregation.field(NUMERIC_FIELD).setNumBuckets(4).setShardSize(6).setInitialBuffer(4),
            histogram -> {
            final List<InternalVariableWidthHistogram.Bucket> buckets = histogram.getBuckets();
            assertEquals(expectedDocCountSearchReduce.size(), buckets.size());
                buckets.forEach(bucket -> {
                    long expectedDocCount = expectedDocCountSearchReduce.getOrDefault(bucket.min(), 0).longValue();
                    double expectedCentroid = expectedCentroidsSearchReduce.getOrDefault(bucket.min(), 0d).doubleValue();
                    double expectedMax = expectedMaxesSearchReduce.getOrDefault(bucket.min(), 0d).doubleValue();
                    assertEquals(expectedDocCount, bucket.getDocCount(), doubleError);
                    assertEquals(expectedCentroid, bucket.centroid(), doubleError);
                    assertEquals(expectedMax, bucket.max(), doubleError);
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
        final List<Double> keys = Arrays.asList(-1d, 1d, 3d, 5d, 7d, 9d, 11d, 13d, 15d, 17d, 19d, 29d, 39d, 50d, 77.5d);
        final List<Double> mins = Arrays.asList(-1d, 1d, 3d, 5d, 7d, 9d, 11d, 13d, 15d, 17d, 19d, 25d, 38d, 50d, 75d);
        final List<Double> maxes = Arrays.asList(-1d, 1d, 3d, 5d, 7d, 9d, 11d, 13d, 15d, 17d, 19d, 32d, 40d, 50d, 80d);
        final List<Integer> docCounts = Arrays.asList(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3, 2, 1, 2);
        assert keys.size() == docCounts.size() && keys.size() == keys.size();

        final Map<Double, Integer> expectedDocCountOnlySearch = new HashMap<>();
        final Map<Double, Double> expectedMinsOnlySearch = new HashMap<>();
        final Map<Double, Double> expectedMaxesOnlySearch = new HashMap<>();
        for(int i=0; i<keys.size(); i++){
            expectedDocCountOnlySearch.put(keys.get(i), docCounts.get(i));
            expectedMinsOnlySearch.put(keys.get(i), mins.get(i));
            expectedMaxesOnlySearch.put(keys.get(i), maxes.get(i));
        }

        testSearchCase(DEFAULT_QUERY, dataset, false,
            aggregation -> aggregation.field(NUMERIC_FIELD).setNumBuckets(2).setShardSize(16).setInitialBuffer(12),
            histogram -> {
                final List<InternalVariableWidthHistogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(expectedDocCountOnlySearch.size(), buckets.size());
                buckets.forEach(bucket -> {
                    long expectedDocCount = expectedDocCountOnlySearch.getOrDefault(bucket.getKey(), 0).longValue();
                    double expectedCentroid = expectedMinsOnlySearch.getOrDefault(bucket.getKey(), 0d).doubleValue();
                    double expectedMax = expectedMaxesOnlySearch.getOrDefault(bucket.getKey(), 0d).doubleValue();
                    assertEquals(expectedDocCount, bucket.getDocCount());
                    assertEquals(expectedCentroid, bucket.min(), doubleError);
                    assertEquals(expectedMax, bucket.max(), doubleError);
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
        expectedDocCount.put(-299d, 3);
        expectedDocCount.put(3.5d, 2);
        expectedDocCount.put(5d, 1);
        expectedDocCount.put(1116d, 5);

        final Map<Double, Double> expectedMins = new HashMap<>();
        expectedMins.put(-299d, -900d);
        expectedMins.put(3.5d, 3d);
        expectedMins.put(5d, 5d);
        expectedMins.put(1116d, 10d);

        final Map<Double, Double> expectedMaxes = new HashMap<>();
        expectedMaxes.put(-299d, 2d);
        expectedMaxes.put(3.5d, 4d);
        expectedMaxes.put(5d, 5d);
        expectedMaxes.put(1116d, 5400d);

        testSearchCase(DEFAULT_QUERY, dataset, false,
            aggregation -> aggregation.field(NUMERIC_FIELD) .setNumBuckets(2).setShardSize(4).setInitialBuffer(5),
            histogram -> {
                final List<InternalVariableWidthHistogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(expectedDocCount.size(), buckets.size());
                buckets.forEach(bucket -> {
                    assertEquals(expectedDocCount.getOrDefault(bucket.getKey(), 0).longValue(), bucket.getDocCount());
                    assertEquals(expectedMins.getOrDefault(bucket.getKey(), 0d).doubleValue(), bucket.min(), doubleError);
                    assertEquals(expectedMaxes.getOrDefault(bucket.getKey(), 0d).doubleValue(), bucket.max(), doubleError);
                });
            });

    }


    public void testSimpleSubAggregations() throws IOException{
        final List<Number> dataset =  Arrays.asList(5, 1, 9, 2, 8);

        testSearchAndReduceCase(DEFAULT_QUERY, dataset, false,
            aggregation -> aggregation.field(NUMERIC_FIELD)
                .setNumBuckets(3)
                .setInitialBuffer(3)
                .setShardSize(4)
                .subAggregation(AggregationBuilders.stats("stats").field(NUMERIC_FIELD)),
            histogram -> {
                final List<InternalVariableWidthHistogram.Bucket> buckets = histogram.getBuckets();
                double deltaError = 1d/10000d;

                // Expected clusters: [ (1, 2), (5), (8,9) ]

                InternalStats stats = histogram.getBuckets().get(0).getAggregations().get("stats");
                assertEquals(1, stats.getMin(), deltaError);
                assertEquals(2, stats.getMax(), deltaError);
                assertEquals(2, stats.getCount());
                assertTrue(AggregationInspectionHelper.hasValue(stats));

                stats = histogram.getBuckets().get(1).getAggregations().get("stats");
                assertEquals(5, stats.getMin(), deltaError);
                assertEquals(5, stats.getMax(), deltaError);
                assertEquals(1, stats.getCount());
                assertTrue(AggregationInspectionHelper.hasValue(stats));

                stats = histogram.getBuckets().get(2).getAggregations().get("stats");
                assertEquals(8, stats.getMin(), deltaError);
                assertEquals(9, stats.getMax(), deltaError);
                assertEquals(2, stats.getCount());
                assertTrue(AggregationInspectionHelper.hasValue(stats));
            });
    }

    public void testComplexSubAggregations() throws IOException{
        final List<Number> dataset =  Arrays.asList(5, 4, 3, 2, 1, 0, 6, 7, 8, 9, 10, 11);

        testSearchCase(DEFAULT_QUERY, dataset, false,
            aggregation -> aggregation.field(NUMERIC_FIELD)
                .setNumBuckets(3)
                .setInitialBuffer(12)
                .setShardSize(4)
                .subAggregation(new StatsAggregationBuilder("stats").field(NUMERIC_FIELD)),
            histogram -> {
                final List<InternalVariableWidthHistogram.Bucket> buckets = histogram.getBuckets();
                double deltaError = 1d / 10000d;

                // Expected clusters: [ (0, 1, 2, 3), (4, 5, 6, 7), (8, 9, 10, 11) ]

                InternalStats stats = histogram.getBuckets().get(0).getAggregations().get("stats");
                assertEquals(0d, stats.getMin(), deltaError);
                assertEquals(3L, stats.getMax(), deltaError);
                assertEquals(4L, stats.getCount());
                assertTrue(AggregationInspectionHelper.hasValue(stats));

                stats = histogram.getBuckets().get(1).getAggregations().get("stats");
                assertEquals(4d, stats.getMin(), deltaError);
                assertEquals(7d, stats.getMax(), deltaError);
                assertEquals(4L, stats.getCount());
                assertTrue(AggregationInspectionHelper.hasValue(stats));

                stats = histogram.getBuckets().get(2).getAggregations().get("stats");
                assertEquals(8d, stats.getMin(), deltaError);
                assertEquals(11d, stats.getMax(), deltaError);
                assertEquals(4L, stats.getCount());
                assertTrue(AggregationInspectionHelper.hasValue(stats));
            });
    }

    public void testSubAggregationReduction() throws IOException{
        final List<Number> dataset =  Arrays.asList(1L, 1L, 1L, 2L, 2L);

        testSearchCase(DEFAULT_QUERY, dataset, false,
            aggregation -> aggregation.field(NUMERIC_FIELD)
                .setNumBuckets(3)
                .setInitialBuffer(12)
                .setShardSize(4)
                .subAggregation(new TermsAggregationBuilder("terms")
                                        .field(NUMERIC_FIELD)
                                        .shardSize(2)
                                        .size(1)),
            histogram -> {
                final List<InternalVariableWidthHistogram.Bucket> buckets = histogram.getBuckets();
                double deltaError = 1d / 10000d;

                // This is a test to make sure that the sub aggregations get reduced
                // This terms sub aggregation has shardSize (2) != size (1), so we will get 1 bucket only if
                // InternalVariableWidthHistogram reduces the sub aggregations.

                InternalTerms terms = histogram.getBuckets().get(0).getAggregations().get("terms");
                assertEquals(1L, terms.getBuckets().size(), deltaError);
                assertEquals(1L, ((InternalTerms.Bucket) terms.getBuckets().get(0)).getKey());
            });
    }

    public void testMultipleSegments() throws IOException{
        final List<Number> dataset =  Arrays.asList(1001, 1002, 1, 2, 1003, 3, 1004, 1005, 4, 5);

        // There should be two clusters: (1, 2, 3, 4, 5) and (1001, 1002, 1003, 1004, 1005)
        // We can't enable multiple segments per index for many of the tests above, because the clusters are too close.
        // Slight randomization --> different caches in the aggregator --> different clusters
        // However, these two clusters are so far apart that even if a doc from one ends up in the other,
        // the centroids will not change much.
        // To account for this case of a document switching clusters, we check that each cluster centroid is within
        // a certain range, rather than asserting exact values.

        testSearchAndReduceCase(DEFAULT_QUERY, dataset, true,
            aggregation -> aggregation.field(NUMERIC_FIELD)
                .setNumBuckets(2)
                .setInitialBuffer(4)
                .setShardSize(3)
                .subAggregation(new StatsAggregationBuilder("stats").field(NUMERIC_FIELD)),
            histogram -> {
                final List<InternalVariableWidthHistogram.Bucket> buckets = histogram.getBuckets();
                double deltaError = 1d / 10000d;

                assertEquals(2, buckets.size());

                // The smaller cluster
                assertEquals(4 <= buckets.get(0).getDocCount() && buckets.get(0).getDocCount() <= 6, true);
                assertEquals(0 <= buckets.get(0).centroid() && buckets.get(0).centroid() <= 200d, true);
                assertEquals(1, buckets.get(0).min(), deltaError);

                // The bigger cluster
                assertEquals(4 <= buckets.get(1).getDocCount() && buckets.get(1).getDocCount() <= 6, true);
                assertEquals(800d <= buckets.get(1).centroid() && buckets.get(1).centroid() <= 1005d, true);
                assertEquals(1005, buckets.get(1).max(), deltaError);
            });

    }


    private void testSearchCase(final Query query, final List<Number> dataset, boolean multipleSegments,
                                final Consumer<VariableWidthHistogramAggregationBuilder> configure,
                                final Consumer<InternalVariableWidthHistogram> verify) throws IOException {
        executeTestCase(false, query, dataset, multipleSegments, configure, verify);
    }


    private void testSearchAndReduceCase(final Query query, final List<Number> dataset, boolean multipleSegments,
                                         final Consumer<VariableWidthHistogramAggregationBuilder> configure,
                                         final Consumer<InternalVariableWidthHistogram> verify) throws IOException {
        executeTestCase(true, query, dataset, multipleSegments, configure, verify);
    }

    private void testBothCases(final Query query, final List<Number> dataset, boolean multipleSegments,
                               final Consumer<VariableWidthHistogramAggregationBuilder> configure,
                               final Consumer<InternalVariableWidthHistogram> verify) throws IOException {
        executeTestCase(true, query, dataset, multipleSegments, configure, verify);
        executeTestCase(false, query, dataset, multipleSegments, configure, verify);
    }

    @Override
    protected IndexSettings createIndexSettings() {
        final Settings nodeSettings = Settings.builder()
            .put("search.max_buckets", 25000).build();
        return new IndexSettings(
            IndexMetadata.builder("_index")
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .creationDate(System.currentTimeMillis())
                .build(),
            nodeSettings
        );
    }

    private void executeTestCase(final boolean reduced, final Query query,
                                 final List<Number> dataset, boolean multipleSegments,
                                 final Consumer<VariableWidthHistogramAggregationBuilder> configure,
                                 final Consumer<InternalVariableWidthHistogram> verify) throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                indexSampleData(dataset, indexWriter, multipleSegments);
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                final IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                final VariableWidthHistogramAggregationBuilder aggregationBuilder =
                    new VariableWidthHistogramAggregationBuilder("_name");
                if (configure != null) {
                    configure.accept(aggregationBuilder);
                }

                final MappedFieldType fieldType;
                if(dataset.size() == 0 || dataset.get(0) instanceof Double){
                    fieldType = new NumberFieldMapper.NumberFieldType(aggregationBuilder.field(), NumberFieldMapper.NumberType.DOUBLE);
                } else if(dataset.get(0) instanceof Long){
                    fieldType = new NumberFieldMapper.NumberFieldType(aggregationBuilder.field(), NumberFieldMapper.NumberType.LONG);
                } else if (dataset.get(0) instanceof Integer){
                    fieldType = new NumberFieldMapper.NumberFieldType(aggregationBuilder.field(), NumberFieldMapper.NumberType.INTEGER);
                } else {
                    throw new IOException("Test data has an invalid type");
                }



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

    private void indexSampleData(List<Number> dataset, RandomIndexWriter indexWriter, boolean multipleSegments) throws IOException {
        if(!multipleSegments) {
            // Put all of the documents into one segment
            List<Document> documents = new ArrayList<>();
            for (final Number doc : dataset) {
                final Document document = new Document();
                long fieldVal = convertDocumentToSortableValue(doc);
                document.add(new SortedNumericDocValuesField(NUMERIC_FIELD, fieldVal));
                documents.add(document);
            }
            indexWriter.addDocuments(documents);
        } else {
            // Create multiple segments in the index
            final Document document = new Document();
            for (final Number doc : dataset) {
                if (frequently()) {
                    indexWriter.commit();
                }

                long fieldVal = convertDocumentToSortableValue(doc);
                document.add(new SortedNumericDocValuesField(NUMERIC_FIELD, fieldVal));
                indexWriter.addDocument(document);
                document.clear();
            }
        }
    }

    long convertDocumentToSortableValue(Number doc) throws IOException{
        if (doc instanceof Double) {
            return NumericUtils.doubleToSortableLong(doc.doubleValue());
        } else if (doc instanceof Integer) {
            return doc.intValue();
        } else if (doc instanceof Long) {
            return doc.longValue();
        }
        throw new IOException("Document has an invalid type");
    }


}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
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

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class VariableWidthHistogramAggregatorTests extends AggregatorTestCase {

    private static final String NUMERIC_FIELD = "numeric";

    private static final Query DEFAULT_QUERY = new MatchAllDocsQuery();

    public void testNoDocs() throws Exception {
        final List<Number> dataset = Arrays.asList();
        testSearchCase(
            DEFAULT_QUERY,
            dataset,
            true,
            aggregation -> aggregation.field(NUMERIC_FIELD).setNumBuckets(2).setShardSize(6).setInitialBuffer(4),
            histogram -> {
                final List<InternalVariableWidthHistogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(0, buckets.size());
            }
        );
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

        testSearchCase(DEFAULT_QUERY, dataset, true, aggregation -> aggregation.field(NUMERIC_FIELD).setNumBuckets(4), histogram -> {
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

        testSearchCase(
            DEFAULT_QUERY,
            dataset,
            false,
            aggregation -> aggregation.field(NUMERIC_FIELD).setNumBuckets(3).setShardSize(6).setInitialBuffer(3),
            histogram -> {
                final List<InternalVariableWidthHistogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(expectedDocCount.size(), buckets.size());
                buckets.forEach(bucket -> {
                    assertEquals(expectedDocCount.getOrDefault(bucket.getKey(), 0).longValue(), bucket.getDocCount());
                    assertEquals(expectedMins.getOrDefault(bucket.getKey(), 0d).doubleValue(), bucket.min(), doubleError);
                    assertEquals(expectedMaxes.getOrDefault(bucket.getKey(), 0d).doubleValue(), bucket.max(), doubleError);
                });
            }
        );
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

        testSearchCase(
            DEFAULT_QUERY,
            dataset,
            false,
            aggregation -> aggregation.field(NUMERIC_FIELD).setNumBuckets(4).setShardSize(6).setInitialBuffer(4),
            histogram -> {
                final List<InternalVariableWidthHistogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(expectedCentroidsOnlySearch.size(), buckets.size());
                buckets.forEach(bucket -> {
                    assertEquals(expectedDocCountOnlySearch.getOrDefault(bucket.min(), 0).longValue(), bucket.getDocCount(), doubleError);
                    assertEquals(expectedCentroidsOnlySearch.getOrDefault(bucket.min(), 0d).doubleValue(), bucket.centroid(), doubleError);
                    assertEquals(expectedMaxesOnlySearch.getOrDefault(bucket.min(), 0d).doubleValue(), bucket.max(), doubleError);
                });
            }
        );

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
        expectedCentroidsSearchReduce.put(2.26, 2.78);
        expectedCentroidsSearchReduce.put(5.3, 5.6);
        expectedCentroidsSearchReduce.put(8.8, 8.8);
        final Map<Double, Double> expectedMaxesSearchReduce = new HashMap<>();
        expectedMaxesSearchReduce.put(-0.4, 1.2);
        expectedMaxesSearchReduce.put(2.26, 3.3);
        expectedMaxesSearchReduce.put(5.3, 5.9);
        expectedMaxesSearchReduce.put(8.8, 8.8);

        testSearchCase(
            DEFAULT_QUERY,
            dataset,
            false,
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
            }
        );
    }

    // Once the cache limit is reached, cached documents are collected into (3/4 * shard_size) buckets
    // A new bucket should be added when there is a document that is distant from all existing buckets
    public void testNewBucketCreation() throws Exception {
        final List<Number> dataset = Arrays.asList(-1, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 40, 30, 25, 32, 36, 80, 50, 75, 60);
        double doubleError = 1d / 10000d;

        // Expected clusters: [ (-1, 1), (3, 5), (7, 9), (11, 13), (15, 17),
        // (19), (25), (30), (32), (36), (40), (50), (60), (75), (80) ]
        final List<Double> keys = Arrays.asList(0d, 4d, 8d, 12d, 16d, 19d, 25d, 30d, 32d, 36d, 40d, 50d, 60d, 75d, 80d);
        final List<Double> mins = Arrays.asList(-1d, 3d, 7d, 11d, 15d, 19d, 25d, 30d, 32d, 36d, 40d, 50d, 60d, 75d, 80d);
        final List<Double> maxes = Arrays.asList(1d, 5d, 9d, 13d, 17d, 19d, 25d, 30d, 32d, 36d, 40d, 50d, 60d, 75d, 80d);
        final List<Integer> docCounts = Arrays.asList(2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
        assert keys.size() == docCounts.size() && keys.size() == keys.size();

        final Map<Double, Integer> expectedDocCountOnlySearch = new HashMap<>();
        final Map<Double, Double> expectedMinsOnlySearch = new HashMap<>();
        final Map<Double, Double> expectedMaxesOnlySearch = new HashMap<>();
        for (int i = 0; i < keys.size(); i++) {
            expectedDocCountOnlySearch.put(keys.get(i), docCounts.get(i));
            expectedMinsOnlySearch.put(keys.get(i), mins.get(i));
            expectedMaxesOnlySearch.put(keys.get(i), maxes.get(i));
        }

        testSearchCase(DEFAULT_QUERY, dataset, false, aggregation -> aggregation.field(NUMERIC_FIELD).setNumBuckets(15), histogram -> {
            final List<InternalVariableWidthHistogram.Bucket> buckets = histogram.getBuckets();
            assertEquals(expectedDocCountOnlySearch.size(), buckets.size());
            buckets.forEach(bucket -> {
                long expectedDocCount = expectedDocCountOnlySearch.getOrDefault(bucket.getKey(), 0).longValue();
                double expectedCentroid = expectedMinsOnlySearch.getOrDefault(bucket.getKey(), 0d).doubleValue();
                double expectedMax = expectedMaxesOnlySearch.getOrDefault(bucket.getKey(), 0d).doubleValue();
                assertEquals(bucket.getKeyAsString(), expectedDocCount, bucket.getDocCount());
                assertEquals(bucket.getKeyAsString(), expectedCentroid, bucket.min(), doubleError);
                assertEquals(bucket.getKeyAsString(), expectedMax, bucket.max(), doubleError);
            });
        });

        // Rerun the test with very large keys which can cause an overflow
        final Map<Double, Integer> expectedDocCountBigKeys = new HashMap<>();
        final Map<Double, Double> expectedMinsBigKeys = new HashMap<>();
        final Map<Double, Double> expectedMaxesBigKeys = new HashMap<>();
        for (int i = 0; i < keys.size(); i++) {
            expectedDocCountBigKeys.put(Long.MAX_VALUE * keys.get(i), docCounts.get(i));
            expectedMinsBigKeys.put(Long.MAX_VALUE * keys.get(i), Long.MAX_VALUE * mins.get(i));
            expectedMaxesBigKeys.put(Long.MAX_VALUE * keys.get(i), Long.MAX_VALUE * maxes.get(i));
        }

        testSearchCase(
            DEFAULT_QUERY,
            dataset.stream().map(n -> Double.valueOf(n.doubleValue() * Long.MAX_VALUE)).collect(toList()),
            false,
            aggregation -> aggregation.field(NUMERIC_FIELD).setNumBuckets(15),
            histogram -> {
                final List<InternalVariableWidthHistogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(expectedDocCountOnlySearch.size(), buckets.size());
                buckets.forEach(bucket -> {
                    long expectedDocCount = expectedDocCountBigKeys.getOrDefault(bucket.getKey(), 0).longValue();
                    double expectedCentroid = expectedMinsBigKeys.getOrDefault(bucket.getKey(), 0d).doubleValue();
                    double expectedMax = expectedMaxesBigKeys.getOrDefault(bucket.getKey(), 0d).doubleValue();
                    assertEquals(expectedDocCount, bucket.getDocCount());
                    assertEquals(expectedCentroid, bucket.min(), doubleError);
                    assertEquals(expectedMax, bucket.max(), doubleError);
                });
            }
        );
    }

    // There should not be more than `shard_size` documents on a node, even when very distant documents appear
    public void testNewBucketLimit() throws Exception {
        final List<Number> dataset = Arrays.asList(1, 2, 3, 4, 5, 10, 20, 50, 100, 5400, -900);
        double doubleError = 1d / 10000d;

        // Expected clusters: [ (-900, 1, 2, 3, 4, 5), (10, 20, 50, 100, 5400)]
        // Corresponding keys (centroids): [ -147.5, 1116]
        final Map<Double, Integer> expectedDocCount = new HashMap<>();
        expectedDocCount.put(-147.5d, 6);
        expectedDocCount.put(1116.0d, 5);

        final Map<Double, Double> expectedMins = new HashMap<>();
        expectedMins.put(-147.5d, -900d);
        expectedMins.put(1116.0d, 10d);

        final Map<Double, Double> expectedMaxes = new HashMap<>();
        expectedMaxes.put(-147.5d, 5d);
        expectedMaxes.put(1116.0d, 5400d);

        testSearchCase(
            DEFAULT_QUERY,
            dataset,
            false,
            aggregation -> aggregation.field(NUMERIC_FIELD).setNumBuckets(2).setShardSize(4).setInitialBuffer(5),
            histogram -> {
                final List<InternalVariableWidthHistogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(expectedDocCount.size(), buckets.size());
                buckets.forEach(bucket -> {
                    assertEquals(expectedDocCount.getOrDefault(bucket.getKey(), 0).longValue(), bucket.getDocCount());
                    assertEquals(expectedMins.getOrDefault(bucket.getKey(), 0d).doubleValue(), bucket.min(), doubleError);
                    assertEquals(expectedMaxes.getOrDefault(bucket.getKey(), 0d).doubleValue(), bucket.max(), doubleError);
                });
            }
        );

    }

    public void testSimpleSubAggregations() throws IOException {
        final List<Number> dataset = Arrays.asList(5, 1, 9, 2, 8);

        testSearchCase(
            DEFAULT_QUERY,
            dataset,
            false,
            aggregation -> aggregation.field(NUMERIC_FIELD)
                .setNumBuckets(3)
                .setInitialBuffer(3)
                .setShardSize(4)
                .subAggregation(AggregationBuilders.stats("stats").field(NUMERIC_FIELD)),
            histogram -> {
                double deltaError = 1d / 10000d;

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
            }
        );
    }

    public void testComplexSubAggregations() throws IOException {
        final List<Number> dataset = Arrays.asList(5, 4, 3, 2, 1, 0, 6, 7, 8, 9, 10, 11);

        testSearchCase(
            DEFAULT_QUERY,
            dataset,
            false,
            aggregation -> aggregation.field(NUMERIC_FIELD)
                .setNumBuckets(3)
                .setInitialBuffer(12)
                .setShardSize(4)
                .subAggregation(new StatsAggregationBuilder("stats").field(NUMERIC_FIELD)),
            histogram -> {
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
            }
        );
    }

    public void testSubAggregationReduction() throws IOException {
        final List<Number> dataset = Arrays.asList(1L, 1L, 1L, 2L, 2L);

        testSearchCase(
            DEFAULT_QUERY,
            dataset,
            false,
            aggregation -> aggregation.field(NUMERIC_FIELD)
                .setNumBuckets(3)
                .setInitialBuffer(12)
                .setShardSize(4)
                .subAggregation(new TermsAggregationBuilder("terms").field(NUMERIC_FIELD).shardSize(2).size(1)),
            histogram -> {
                double deltaError = 1d / 10000d;

                // This is a test to make sure that the sub aggregations get reduced
                // This terms sub aggregation has shardSize (2) != size (1), so we will get 1 bucket only if
                // InternalVariableWidthHistogram reduces the sub aggregations.

                LongTerms terms = histogram.getBuckets().get(0).getAggregations().get("terms");
                assertEquals(1L, terms.getBuckets().size(), deltaError);
                assertEquals(1L, terms.getBuckets().get(0).getKey());
            }
        );
    }

    public void testMultipleSegments() throws IOException {
        final List<Number> dataset = Arrays.asList(1001, 1002, 1, 2, 1003, 3, 1004, 1005, 4, 5);

        // There should be two clusters: (1, 2, 3, 4, 5) and (1001, 1002, 1003, 1004, 1005)
        // We can't enable multiple segments per index for many of the tests above, because the clusters are too close.
        // Slight randomization --> different caches in the aggregator --> different clusters
        // However, these two clusters are so far apart that even if a doc from one ends up in the other,
        // the centroids will not change much.
        // To account for this case of a document switching clusters, we check that each cluster centroid is within
        // a certain range, rather than asserting exact values.

        testSearchCase(
            DEFAULT_QUERY,
            dataset,
            true,
            aggregation -> aggregation.field(NUMERIC_FIELD)
                .setNumBuckets(2)
                .setInitialBuffer(4)
                .setShardSize(3)
                .subAggregation(new StatsAggregationBuilder("stats").field(NUMERIC_FIELD)),
            histogram -> {
                final List<InternalVariableWidthHistogram.Bucket> buckets = histogram.getBuckets();
                double deltaError = 1d / 10000d;

                assertEquals(2, buckets.size());

                // The lower cluster
                assertThat(buckets.get(0).getDocCount(), both(greaterThanOrEqualTo(4L)).and(lessThanOrEqualTo(7L)));
                assertThat(buckets.get(0).centroid(), both(greaterThanOrEqualTo(0.0)).and(lessThanOrEqualTo(300.0)));
                assertEquals(1, buckets.get(0).min(), deltaError);

                // The higher cluster
                assertThat(buckets.get(1).getDocCount(), equalTo(dataset.size() - buckets.get(0).getDocCount()));
                assertThat(buckets.get(1).centroid(), both(greaterThanOrEqualTo(800.0)).and(lessThanOrEqualTo(1005.0)));
                assertEquals(1005, buckets.get(1).max(), deltaError);
            }
        );

    }

    public void testAsSubAggregation() throws IOException {
        AggregationBuilder builder = new TermsAggregationBuilder("t").field("t")
            .subAggregation(new VariableWidthHistogramAggregationBuilder("v").field("v").setNumBuckets(2));
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex = iw -> {
            iw.addDocument(List.of(new SortedNumericDocValuesField("t", 1), new SortedNumericDocValuesField("v", 1)));
            iw.addDocument(List.of(new SortedNumericDocValuesField("t", 1), new SortedNumericDocValuesField("v", 10)));
            iw.addDocument(List.of(new SortedNumericDocValuesField("t", 1), new SortedNumericDocValuesField("v", 11)));

            iw.addDocument(List.of(new SortedNumericDocValuesField("t", 2), new SortedNumericDocValuesField("v", 20)));
            iw.addDocument(List.of(new SortedNumericDocValuesField("t", 2), new SortedNumericDocValuesField("v", 30)));
        };
        Consumer<LongTerms> verify = terms -> {
            /*
             * This is what the result should be but it never gets called because of
             * the explicit check. We do expect to remove the check in the future,
             * thus, this stays.
             */
            LongTerms.Bucket t1 = terms.getBucketByKey("1");
            InternalVariableWidthHistogram v1 = t1.getAggregations().get("v");
            assertThat(
                v1.getBuckets().stream().map(InternalVariableWidthHistogram.Bucket::centroid).collect(toList()),
                equalTo(List.of(1.0, 10.5))
            );

            LongTerms.Bucket t2 = terms.getBucketByKey("1");
            InternalVariableWidthHistogram v2 = t2.getAggregations().get("v");
            assertThat(
                v2.getBuckets().stream().map(InternalVariableWidthHistogram.Bucket::centroid).collect(toList()),
                equalTo(List.of(20.0, 30))
            );
        };
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> testCase(builder, DEFAULT_QUERY, buildIndex, verify, longField("t"), longField("v"))
        );
        assertThat(e.getMessage(), containsString("cannot be nested"));
    }

    public void testShardSizeTooSmall() throws Exception {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> new VariableWidthHistogramAggregationBuilder("test").setShardSize(1)
        );
        assertThat(e.getMessage(), equalTo("shard_size must be greater than [1] for [test]"));
    }

    public void testSmallShardSize() throws Exception {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> testSearchCase(
                DEFAULT_QUERY,
                List.of(),
                true,
                aggregation -> aggregation.field(NUMERIC_FIELD).setNumBuckets(2).setShardSize(2),
                histogram -> { fail(); }
            )
        );
        assertThat(e.getMessage(), equalTo("3/4 of shard_size must be at least buckets but was [1<2] for [_name]"));
    }

    public void testHugeShardSize() throws Exception {
        final List<Number> dataset = Arrays.asList(1, 2, 3);
        testSearchCase(
            DEFAULT_QUERY,
            dataset,
            true,
            aggregation -> aggregation.field(NUMERIC_FIELD).setShardSize(1000000000),
            histogram -> assertThat(
                histogram.getBuckets().stream().map(InternalVariableWidthHistogram.Bucket::getKey).collect(toList()),
                equalTo(List.of(1.0, 2.0, 3.0))
            )
        );
    }

    public void testSmallInitialBuffer() throws Exception {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> testSearchCase(
                DEFAULT_QUERY,
                List.of(),
                true,
                aggregation -> aggregation.field(NUMERIC_FIELD).setInitialBuffer(1),
                histogram -> { fail(); }
            )
        );
        assertThat(e.getMessage(), equalTo("initial_buffer must be at least buckets but was [1<10] for [_name]"));
    }

    public void testOutOfOrderInitialBuffer() throws Exception {
        final List<Number> dataset = Arrays.asList(1, 2, 3);
        testSearchCase(
            DEFAULT_QUERY,
            dataset,
            true,
            aggregation -> aggregation.field(NUMERIC_FIELD).setInitialBuffer(3).setNumBuckets(3),
            histogram -> {
                assertThat(
                    histogram.getBuckets().stream().map(InternalVariableWidthHistogram.Bucket::getKey).collect(toList()),
                    equalTo(List.of(1.0, 2.0, 3.0))
                );
            }
        );
    }

    public void testDefaultShardSizeDependsOnNumBuckets() throws Exception {
        assertThat(new VariableWidthHistogramAggregationBuilder("test").setNumBuckets(3).getShardSize(), equalTo(150));
    }

    public void testDefaultInitialBufferDependsOnNumBuckets() throws Exception {
        assertThat(new VariableWidthHistogramAggregationBuilder("test").setShardSize(50).getInitialBuffer(), equalTo(500));
        assertThat(new VariableWidthHistogramAggregationBuilder("test").setShardSize(10000).getInitialBuffer(), equalTo(50000));
        assertThat(new VariableWidthHistogramAggregationBuilder("test").setNumBuckets(3).getInitialBuffer(), equalTo(1500));
    }

    @Override
    protected IndexSettings createIndexSettings() {
        final Settings nodeSettings = Settings.builder().put("search.max_buckets", 25000).build();
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

    private void testSearchCase(
        final Query query,
        final List<Number> dataset,
        boolean multipleSegments,
        final Consumer<VariableWidthHistogramAggregationBuilder> configure,
        final Consumer<InternalVariableWidthHistogram> verify
    ) throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                indexSampleData(dataset, indexWriter, multipleSegments);
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                final IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                final VariableWidthHistogramAggregationBuilder aggregationBuilder = new VariableWidthHistogramAggregationBuilder("_name");
                if (configure != null) {
                    configure.accept(aggregationBuilder);
                }

                final MappedFieldType fieldType;
                if (dataset.size() == 0 || dataset.get(0) instanceof Double) {
                    fieldType = new NumberFieldMapper.NumberFieldType(aggregationBuilder.field(), NumberFieldMapper.NumberType.DOUBLE);
                } else if (dataset.get(0) instanceof Long) {
                    fieldType = new NumberFieldMapper.NumberFieldType(aggregationBuilder.field(), NumberFieldMapper.NumberType.LONG);
                } else if (dataset.get(0) instanceof Integer) {
                    fieldType = new NumberFieldMapper.NumberFieldType(aggregationBuilder.field(), NumberFieldMapper.NumberType.INTEGER);
                } else {
                    throw new IOException("Test data has an invalid type");
                }

                final InternalVariableWidthHistogram histogram = searchAndReduce(indexSearcher, query, aggregationBuilder, fieldType);
                verify.accept(histogram);
            }
        }
    }

    private void indexSampleData(List<Number> dataset, RandomIndexWriter indexWriter, boolean multipleSegments) throws IOException {
        if (multipleSegments == false) {
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
                long fieldVal = convertDocumentToSortableValue(doc);
                document.add(new SortedNumericDocValuesField(NUMERIC_FIELD, fieldVal));
                indexWriter.addDocument(document);
                document.clear();
            }
        }
    }

    long convertDocumentToSortableValue(Number doc) throws IOException {
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

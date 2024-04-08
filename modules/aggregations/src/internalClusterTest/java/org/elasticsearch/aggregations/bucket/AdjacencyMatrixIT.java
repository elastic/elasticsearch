/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.aggregations.bucket;

import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.aggregations.AggregationIntegTestCase;
import org.elasticsearch.aggregations.bucket.adjacency.AdjacencyMatrix;
import org.elasticsearch.aggregations.bucket.adjacency.AdjacencyMatrix.Bucket;
import org.elasticsearch.aggregations.bucket.adjacency.AdjacencyMatrixAggregationBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.avg;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class AdjacencyMatrixIT extends AggregationIntegTestCase {

    static int numDocs, numSingleTag1Docs, numSingleTag2Docs, numTag1Docs, numTag2Docs, numMultiTagDocs;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx2");

        numDocs = randomIntBetween(5, 20);
        numTag1Docs = randomIntBetween(1, numDocs - 1);
        numTag2Docs = randomIntBetween(1, numDocs - numTag1Docs);
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < numTag1Docs; i++) {
            numSingleTag1Docs++;
            XContentBuilder source = jsonBuilder().startObject().field("value", i + 1).field("tag", "tag1").endObject();
            builders.add(prepareIndex("idx").setId("" + i).setSource(source));
            if (randomBoolean()) {
                // randomly index the document twice so that we have deleted
                // docs that match the filter
                builders.add(prepareIndex("idx").setId("" + i).setSource(source));
            }
        }
        for (int i = numTag1Docs; i < (numTag1Docs + numTag2Docs); i++) {
            numSingleTag2Docs++;
            XContentBuilder source = jsonBuilder().startObject().field("value", i + 1).field("tag", "tag2").endObject();
            builders.add(prepareIndex("idx").setId("" + i).setSource(source));
            if (randomBoolean()) {
                builders.add(prepareIndex("idx").setId("" + i).setSource(source));
            }
        }
        for (int i = numTag1Docs + numTag2Docs; i < numDocs; i++) {
            numMultiTagDocs++;
            numTag1Docs++;
            numTag2Docs++;
            XContentBuilder source = jsonBuilder().startObject().field("value", i + 1).array("tag", "tag1", "tag2").endObject();
            builders.add(prepareIndex("idx").setId("" + i).setSource(source));
            if (randomBoolean()) {
                builders.add(prepareIndex("idx").setId("" + i).setSource(source));
            }
        }
        prepareCreate("empty_bucket_idx").setMapping("value", "type=integer").get();
        for (int i = 0; i < 2; i++) {
            builders.add(
                prepareIndex("empty_bucket_idx").setId("" + i).setSource(jsonBuilder().startObject().field("value", i * 2).endObject())
            );
        }
        indexRandom(true, builders);
        ensureSearchable();
    }

    public void testSimple() throws Exception {
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                adjacencyMatrix("tags", newMap("tag1", termQuery("tag", "tag1")).add("tag2", termQuery("tag", "tag2")))
            ),
            response -> {
                AdjacencyMatrix matrix = response.getAggregations().get("tags");
                assertThat(matrix, notNullValue());
                assertThat(matrix.getName(), equalTo("tags"));

                int expected = numMultiTagDocs > 0 ? 3 : 2;
                assertThat(matrix.getBuckets().size(), equalTo(expected));

                AdjacencyMatrix.Bucket bucket = matrix.getBucketByKey("tag1");
                assertThat(bucket, Matchers.notNullValue());
                assertThat(bucket.getDocCount(), equalTo((long) numTag1Docs));

                bucket = matrix.getBucketByKey("tag2");
                assertThat(bucket, Matchers.notNullValue());
                assertThat(bucket.getDocCount(), equalTo((long) numTag2Docs));

                bucket = matrix.getBucketByKey("tag1&tag2");
                if (numMultiTagDocs == 0) {
                    assertThat(bucket, Matchers.nullValue());
                } else {
                    assertThat(bucket, Matchers.notNullValue());
                    assertThat(bucket.getDocCount(), equalTo((long) numMultiTagDocs));
                }
            }
        );
    }

    public void testCustomSeparator() throws Exception {
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                adjacencyMatrix("tags", "\t", newMap("tag1", termQuery("tag", "tag1")).add("tag2", termQuery("tag", "tag2")))
            ),
            response -> {
                AdjacencyMatrix matrix = response.getAggregations().get("tags");
                assertThat(matrix, notNullValue());

                AdjacencyMatrix.Bucket bucket = matrix.getBucketByKey("tag1\ttag2");
                if (numMultiTagDocs == 0) {
                    assertThat(bucket, Matchers.nullValue());
                } else {
                    assertThat(bucket, Matchers.notNullValue());
                    assertThat(bucket.getDocCount(), equalTo((long) numMultiTagDocs));
                }
            }
        );
    }

    // See NullPointer issue when filters are empty:
    // https://github.com/elastic/elasticsearch/issues/8438
    public void testEmptyFilterDeclarations() throws Exception {
        QueryBuilder emptyFilter = new BoolQueryBuilder();
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(adjacencyMatrix("tags", newMap("all", emptyFilter).add("tag1", termQuery("tag", "tag1")))),
            response -> {
                AdjacencyMatrix filters = response.getAggregations().get("tags");
                assertThat(filters, notNullValue());
                AdjacencyMatrix.Bucket allBucket = filters.getBucketByKey("all");
                assertThat(allBucket.getDocCount(), equalTo((long) numDocs));

                AdjacencyMatrix.Bucket bucket = filters.getBucketByKey("tag1");
                assertThat(bucket, Matchers.notNullValue());
                assertThat(bucket.getDocCount(), equalTo((long) numTag1Docs));
            }
        );
    }

    public void testWithSubAggregation() throws Exception {
        BoolQueryBuilder boolQ = new BoolQueryBuilder();
        boolQ.must(termQuery("tag", "tag1"));
        boolQ.must(termQuery("tag", "tag2"));
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                adjacencyMatrix("tags", newMap("tag1", termQuery("tag", "tag1")).add("tag2", termQuery("tag", "tag2")).add("both", boolQ))
                    .subAggregation(avg("avg_value").field("value"))
            ),
            response -> {
                AdjacencyMatrix matrix = response.getAggregations().get("tags");
                assertThat(matrix, notNullValue());
                assertThat(matrix.getName(), equalTo("tags"));

                int expectedBuckets = 0;
                if (numTag1Docs > 0) {
                    expectedBuckets++;
                }
                if (numTag2Docs > 0) {
                    expectedBuckets++;
                }
                if (numMultiTagDocs > 0) {
                    // both, both&tag1, both&tag2, tag1&tag2
                    expectedBuckets += 4;
                }

                assertThat(matrix.getBuckets().size(), equalTo(expectedBuckets));
                assertThat(((InternalAggregation) matrix).getProperty("_bucket_count"), equalTo(expectedBuckets));

                Object[] propertiesKeys = (Object[]) ((InternalAggregation) matrix).getProperty("_key");
                Object[] propertiesDocCounts = (Object[]) ((InternalAggregation) matrix).getProperty("_count");
                Object[] propertiesCounts = (Object[]) ((InternalAggregation) matrix).getProperty("avg_value.value");

                assertEquals(expectedBuckets, propertiesKeys.length);
                assertEquals(propertiesKeys.length, propertiesDocCounts.length);
                assertEquals(propertiesKeys.length, propertiesCounts.length);

                for (int i = 0; i < propertiesCounts.length; i++) {
                    AdjacencyMatrix.Bucket bucket = matrix.getBucketByKey(propertiesKeys[i].toString());
                    assertThat(bucket, Matchers.notNullValue());
                    Avg avgValue = bucket.getAggregations().get("avg_value");
                    assertThat(avgValue, notNullValue());
                    assertThat((long) propertiesDocCounts[i], equalTo(bucket.getDocCount()));
                    assertThat((double) propertiesCounts[i], equalTo(avgValue.getValue()));
                }

                AdjacencyMatrix.Bucket tag1Bucket = matrix.getBucketByKey("tag1");
                assertThat(tag1Bucket, Matchers.notNullValue());
                assertThat(tag1Bucket.getDocCount(), equalTo((long) numTag1Docs));
                long sum = 0;
                for (int i = 0; i < numSingleTag1Docs; i++) {
                    sum += i + 1;
                }
                for (int i = numSingleTag1Docs + numSingleTag2Docs; i < numDocs; i++) {
                    sum += i + 1;
                }
                assertThat(tag1Bucket.getAggregations().asList().isEmpty(), is(false));
                Avg avgBucket1Value = tag1Bucket.getAggregations().get("avg_value");
                assertThat(avgBucket1Value, notNullValue());
                assertThat(avgBucket1Value.getName(), equalTo("avg_value"));
                assertThat(avgBucket1Value.getValue(), equalTo((double) sum / numTag1Docs));

                Bucket tag2Bucket = matrix.getBucketByKey("tag2");
                assertThat(tag2Bucket, Matchers.notNullValue());
                assertThat(tag2Bucket.getDocCount(), equalTo((long) numTag2Docs));
                sum = 0;
                for (int i = numSingleTag1Docs; i < numDocs; i++) {
                    sum += i + 1;
                }
                assertThat(tag2Bucket.getAggregations().asList().isEmpty(), is(false));
                Avg avgBucket2Value = tag2Bucket.getAggregations().get("avg_value");
                assertThat(avgBucket2Value, notNullValue());
                assertThat(avgBucket2Value.getName(), equalTo("avg_value"));
                assertThat(avgBucket2Value.getValue(), equalTo((double) sum / numTag2Docs));

                // Check intersection buckets are computed correctly by comparing with
                // ANDed query bucket results
                Bucket bucketBothQ = matrix.getBucketByKey("both");
                if (numMultiTagDocs == 0) {
                    // Empty intersections are not returned.
                    assertThat(bucketBothQ, Matchers.nullValue());
                    Bucket bucketIntersectQ = matrix.getBucketByKey("tag1&tag2");
                    assertThat(bucketIntersectQ, Matchers.nullValue());
                    Bucket tag1Both = matrix.getBucketByKey("both&tag1");
                    assertThat(tag1Both, Matchers.nullValue());
                } else {
                    assertThat(bucketBothQ, Matchers.notNullValue());
                    assertThat(bucketBothQ.getDocCount(), equalTo((long) numMultiTagDocs));
                    Avg avgValueBothQ = bucketBothQ.getAggregations().get("avg_value");

                    Bucket bucketIntersectQ = matrix.getBucketByKey("tag1&tag2");
                    assertThat(bucketIntersectQ, Matchers.notNullValue());
                    assertThat(bucketIntersectQ.getDocCount(), equalTo((long) numMultiTagDocs));
                    Avg avgValueIntersectQ = bucketBothQ.getAggregations().get("avg_value");
                    assertThat(avgValueIntersectQ.getValue(), equalTo(avgValueBothQ.getValue()));

                    Bucket tag1Both = matrix.getBucketByKey("both&tag1");
                    assertThat(tag1Both, Matchers.notNullValue());
                    assertThat(tag1Both.getDocCount(), equalTo((long) numMultiTagDocs));
                    Avg avgValueTag1BothIntersectQ = tag1Both.getAggregations().get("avg_value");
                    assertThat(avgValueTag1BothIntersectQ.getValue(), equalTo(avgValueBothQ.getValue()));
                }
            }
        );
    }

    public void testTooLargeMatrix() {

        int originalMaxClauses = IndexSearcher.getMaxClauseCount();

        try {
            // Create more filters than is permitted by Lucene Bool clause settings.
            MapBuilder filtersMap = new MapBuilder();
            int maxFilters = randomIntBetween(50, 100);
            IndexSearcher.setMaxClauseCount(maxFilters);
            for (int i = 0; i <= maxFilters; i++) {
                filtersMap.add("tag" + i, termQuery("tag", "tag" + i));
            }

            try {
                prepareSearch("idx").addAggregation(adjacencyMatrix("tags", "\t", filtersMap)).get();
                fail("SearchPhaseExecutionException should have been thrown");
            } catch (SearchPhaseExecutionException ex) {
                assertThat(ex.getCause().getMessage(), containsString("Number of filters is too large"));
            }

        } finally {
            IndexSearcher.setMaxClauseCount(originalMaxClauses);
        }
    }

    public void testAsSubAggregation() {
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                histogram("histo").field("value").interval(2L).subAggregation(adjacencyMatrix("matrix", newMap("all", matchAllQuery())))
            ),
            response -> {
                Histogram histo = response.getAggregations().get("histo");
                assertThat(histo, notNullValue());
                assertThat(histo.getBuckets().size(), greaterThanOrEqualTo(1));

                for (Histogram.Bucket bucket : histo.getBuckets()) {
                    AdjacencyMatrix matrix = bucket.getAggregations().get("matrix");
                    assertThat(matrix, notNullValue());
                    assertThat(matrix.getBuckets().size(), equalTo(1));
                    AdjacencyMatrix.Bucket filterBucket = matrix.getBuckets().get(0);
                    assertEquals(bucket.getDocCount(), filterBucket.getDocCount());
                }
            }
        );
    }

    public void testWithContextBasedSubAggregation() throws Exception {

        try {
            prepareSearch("idx").addAggregation(
                adjacencyMatrix("tags", newMap("tag1", termQuery("tag", "tag1")).add("tag2", termQuery("tag", "tag2"))).subAggregation(
                    avg("avg_value")
                )
            ).get();

            fail(
                "expected execution to fail - an attempt to have a context based numeric sub-aggregation, but there is not value source"
                    + "context which the sub-aggregation can inherit"
            );

        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("all shards failed"));
        }
    }

    public void testEmptyAggregation() throws Exception {
        assertResponse(
            prepareSearch("empty_bucket_idx").setQuery(matchAllQuery())
                .addAggregation(
                    histogram("histo").field("value")
                        .interval(1L)
                        .minDocCount(0)
                        .subAggregation(adjacencyMatrix("matrix", newMap("all", matchAllQuery())))
                ),
            response -> {
                assertHitCount(response, 2L);
                Histogram histo = response.getAggregations().get("histo");
                assertThat(histo, Matchers.notNullValue());
                Histogram.Bucket bucket = histo.getBuckets().get(1);
                assertThat(bucket, Matchers.notNullValue());

                AdjacencyMatrix matrix = bucket.getAggregations().get("matrix");
                assertThat(matrix, notNullValue());
                AdjacencyMatrix.Bucket all = matrix.getBucketByKey("all");
                assertThat(all, Matchers.nullValue());
            }
        );
    }

    // Helper methods for building maps of QueryBuilders
    static MapBuilder newMap(String name, QueryBuilder builder) {
        return new MapBuilder().add(name, builder);
    }

    static class MapBuilder extends HashMap<String, QueryBuilder> {
        public MapBuilder add(String name, QueryBuilder builder) {
            put(name, builder);
            return this;
        }
    }

    static AdjacencyMatrixAggregationBuilder adjacencyMatrix(String name, Map<String, QueryBuilder> filters) {
        return new AdjacencyMatrixAggregationBuilder(name, filters);
    }

    static AdjacencyMatrixAggregationBuilder adjacencyMatrix(String name, String separator, Map<String, QueryBuilder> filters) {
        return new AdjacencyMatrixAggregationBuilder(name, separator, filters);
    }

}

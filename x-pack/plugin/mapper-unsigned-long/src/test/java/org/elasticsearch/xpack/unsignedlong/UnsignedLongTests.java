/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.unsignedlong;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.max;
import static org.elasticsearch.search.aggregations.AggregationBuilders.min;
import static org.elasticsearch.search.aggregations.AggregationBuilders.range;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;

@ESIntegTestCase.SuiteScopeTestCase
public class UnsignedLongTests extends ESIntegTestCase {
    final int numDocs = 10;
    final Number[] values = {
        0L,
        0L,
        100L,
        9223372036854775807L,
        new BigInteger("9223372036854775808"),
        new BigInteger("10446744073709551613"),
        new BigInteger("18446744073709551614"),
        new BigInteger("18446744073709551614"),
        new BigInteger("18446744073709551615"),
        new BigInteger("18446744073709551615") };

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(UnsignedLongMapperPlugin.class);
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        Settings.Builder settings = Settings.builder().put(indexSettings()).put("number_of_shards", 1);
        Settings.Builder sortSettings = Settings.builder()
            .put(indexSettings())
            .putList("sort.field", "ul_field")
            .put("number_of_shards", 1);

        prepareCreate("idx").setMapping("ul_field", "type=unsigned_long").setSettings(settings).get();
        prepareCreate("idx-sort").setMapping("ul_field", "type=unsigned_long").setSettings(sortSettings).get();
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            builders.add(prepareIndex("idx").setSource(jsonBuilder().startObject().field("ul_field", values[i]).endObject()));
            builders.add(prepareIndex("idx-sort").setSource(jsonBuilder().startObject().field("ul_field", values[i]).endObject()));
        }
        indexRandom(true, builders);

        prepareCreate("idx2").setMapping("ul_field", "type=long").setSettings(settings).get();
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < 4; i++) {
            IndexRequest indexRequest = new IndexRequest("idx2").source("ul_field", values[i]);
            bulkRequestBuilder.add(indexRequest);
        }
        bulkRequestBuilder.get();

        ensureSearchable();
    }

    public void testSort() {
        for (String index : new String[] { "idx", "idx-sort" }) {
            // asc sort
            assertNoFailuresAndResponse(
                prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).setSize(numDocs).addSort("ul_field", SortOrder.ASC),
                response -> {
                    SearchHit[] hits = response.getHits().getHits();
                    assertEquals(hits.length, numDocs);
                    int i = 0;
                    for (SearchHit hit : hits) {
                        assertEquals(values[i++], hit.getSortValues()[0]);
                    }
                }
            );
            // desc sort
            assertNoFailuresAndResponse(
                prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).setSize(numDocs).addSort("ul_field", SortOrder.DESC),
                response -> {
                    SearchHit[] hits = response.getHits().getHits();
                    assertEquals(hits.length, numDocs);
                    int i = numDocs - 1;
                    for (SearchHit hit : hits) {
                        assertEquals(values[i--], hit.getSortValues()[0]);
                    }
                }
            );
            // asc sort with search_after as Long
            assertNoFailuresAndResponse(
                prepareSearch(index).setQuery(QueryBuilders.matchAllQuery())
                    .setSize(numDocs)
                    .addSort("ul_field", SortOrder.ASC)
                    .searchAfter(new Long[] { 100L }),
                response -> {
                    SearchHit[] hits = response.getHits().getHits();
                    assertEquals(hits.length, 7);
                    int i = 3;
                    for (SearchHit hit : hits) {
                        assertEquals(values[i++], hit.getSortValues()[0]);
                    }
                }
            );
            // asc sort with search_after as BigInteger
            assertNoFailuresAndResponse(
                prepareSearch(index).setQuery(QueryBuilders.matchAllQuery())
                    .setSize(numDocs)
                    .addSort("ul_field", SortOrder.ASC)
                    .searchAfter(new BigInteger[] { new BigInteger("18446744073709551614") }),
                response -> {
                    SearchHit[] hits = response.getHits().getHits();
                    assertEquals(hits.length, 2);
                    int i = 8;
                    for (SearchHit hit : hits) {
                        assertEquals(values[i++], hit.getSortValues()[0]);
                    }
                }
            );
            // asc sort with search_after as BigInteger in String format
            assertNoFailuresAndResponse(
                prepareSearch(index).setQuery(QueryBuilders.matchAllQuery())
                    .setSize(numDocs)
                    .addSort("ul_field", SortOrder.ASC)
                    .searchAfter(new String[] { "18446744073709551614" }),
                response -> {
                    SearchHit[] hits = response.getHits().getHits();
                    assertEquals(hits.length, 2);
                    int i = 8;
                    for (SearchHit hit : hits) {
                        assertEquals(values[i++], hit.getSortValues()[0]);
                    }
                }
            );
            // asc sort with search_after of negative value should fail
            {
                SearchRequestBuilder srb = prepareSearch(index).setQuery(QueryBuilders.matchAllQuery())
                    .setSize(numDocs)
                    .addSort("ul_field", SortOrder.ASC)
                    .searchAfter(new Long[] { -1L });
                ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> srb.get());
                assertThat(exception.getCause().getMessage(), containsString("Failed to parse search_after value"));
            }
            // asc sort with search_after of value>=2^64 should fail
            {
                SearchRequestBuilder srb = prepareSearch(index).setQuery(QueryBuilders.matchAllQuery())
                    .setSize(numDocs)
                    .addSort("ul_field", SortOrder.ASC)
                    .searchAfter(new BigInteger[] { new BigInteger("18446744073709551616") });
                ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> srb.get());
                assertThat(exception.getCause().getMessage(), containsString("Failed to parse search_after value"));
            }
            // desc sort with search_after as BigInteger
            assertNoFailuresAndResponse(
                prepareSearch(index).setQuery(QueryBuilders.matchAllQuery())
                    .setSize(numDocs)
                    .addSort("ul_field", SortOrder.DESC)
                    .searchAfter(new BigInteger[] { new BigInteger("18446744073709551615") }),
                response -> {
                    SearchHit[] hits = response.getHits().getHits();
                    assertEquals(hits.length, 8);
                    int i = 7;
                    for (SearchHit hit : hits) {
                        assertEquals(values[i--], hit.getSortValues()[0]);
                    }
                }
            );
        }
    }

    public void testAggs() {
        // terms agg
        assertNoFailuresAndResponse(prepareSearch("idx").setSize(0).addAggregation(terms("ul_terms").field("ul_field")), response -> {
            Terms terms = response.getAggregations().get("ul_terms");

            long[] expectedBucketDocCounts = { 2, 2, 2, 1, 1, 1, 1 };
            Object[] expectedBucketKeys = {
                0L,
                new BigInteger("18446744073709551614"),
                new BigInteger("18446744073709551615"),
                100L,
                9223372036854775807L,
                new BigInteger("9223372036854775808"),
                new BigInteger("10446744073709551613") };
            int i = 0;
            for (Terms.Bucket bucket : terms.getBuckets()) {
                assertEquals(expectedBucketDocCounts[i], bucket.getDocCount());
                assertEquals(expectedBucketKeys[i], bucket.getKey());
                i++;
            }
        });

        // histogram agg
        assertNoFailuresAndResponse(
            prepareSearch("idx").setSize(0).addAggregation(histogram("ul_histo").field("ul_field").interval(9E18).minDocCount(0)),
            response -> {
                Histogram histo = response.getAggregations().get("ul_histo");

                long[] expectedBucketDocCounts = { 3, 3, 4 };
                double[] expectedBucketKeys = { 0, 9.0E18, 1.8E19 };
                int i = 0;
                for (Histogram.Bucket bucket : histo.getBuckets()) {
                    assertEquals(expectedBucketDocCounts[i], bucket.getDocCount());
                    assertEquals(expectedBucketKeys[i], bucket.getKey());
                    i++;
                }
            }
        );

        // range agg
        assertNoFailuresAndResponse(
            prepareSearch("idx").setSize(0)
                .addAggregation(
                    range("ul_range").field("ul_field").addUnboundedTo(9.0E18).addRange(9.0E18, 1.8E19).addUnboundedFrom(1.8E19)
                ),
            response -> {
                Range range = response.getAggregations().get("ul_range");

                long[] expectedBucketDocCounts = { 3, 3, 4 };
                String[] expectedBucketKeys = { "*-9.0E18", "9.0E18-1.8E19", "1.8E19-*" };
                int i = 0;
                for (Range.Bucket bucket : range.getBuckets()) {
                    assertEquals(expectedBucketDocCounts[i], bucket.getDocCount());
                    assertEquals(expectedBucketKeys[i], bucket.getKey());
                    i++;
                }
            }
        );

        // sum agg
        assertNoFailuresAndResponse(prepareSearch("idx").setSize(0).addAggregation(sum("ul_sum").field("ul_field")), response -> {
            Sum sum = response.getAggregations().get("ul_sum");
            double expectedSum = Arrays.stream(values).mapToDouble(Number::doubleValue).sum();
            assertEquals(expectedSum, sum.value(), 0.001);
        });
        // max agg
        assertNoFailuresAndResponse(prepareSearch("idx").setSize(0).addAggregation(max("ul_max").field("ul_field")), response -> {
            Max max = response.getAggregations().get("ul_max");
            assertEquals(1.8446744073709551615E19, max.value(), 0.001);
        });

        // min agg
        assertNoFailuresAndResponse(prepareSearch("idx").setSize(0).addAggregation(min("ul_min").field("ul_field")), response -> {
            Min min = response.getAggregations().get("ul_min");
            assertEquals(0, min.value(), 0.001);
        });
    }

    public void testSortDifferentFormatsShouldFail() {
        Exception exception = expectThrows(
            SearchPhaseExecutionException.class,
            () -> prepareSearch().setIndices("idx", "idx2").setQuery(QueryBuilders.matchAllQuery()).addSort("ul_field", SortOrder.ASC).get()
        );
        assertEquals(
            exception.getCause().getMessage(),
            "Can't do sort across indices, as a field has [unsigned_long] type in one index, and different type in another index!"
        );
    }

    public void testRangeQuery() {
        assertHitCount(prepareSearch("idx").setSize(0).setQuery(new RangeQueryBuilder("ul_field").to("9.0E18").includeUpper(false)), 3);
        assertHitCount(
            prepareSearch("idx").setSize(0).setQuery(new RangeQueryBuilder("ul_field").from("9.0E18").to("1.8E19").includeUpper(false)),
            3
        );
        assertHitCount(prepareSearch("idx").setSize(0).setQuery(new RangeQueryBuilder("ul_field").from("1.8E19")), 4);
    }
}

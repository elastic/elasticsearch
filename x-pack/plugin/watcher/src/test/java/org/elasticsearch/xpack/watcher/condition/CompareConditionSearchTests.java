/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.condition;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;

import java.time.Clock;
import java.util.Map;

import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.mockExecutionContext;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.when;

public class CompareConditionSearchTests extends AbstractWatcherIntegrationTestCase {

    public void testExecuteWithAggs() throws Exception {
        client().prepareIndex("my-index").setSource("@timestamp", "2005-01-01T00:00").get();
        client().prepareIndex("my-index").setSource("@timestamp", "2005-01-01T00:10").get();
        client().prepareIndex("my-index").setSource("@timestamp", "2005-01-01T00:20").get();
        client().prepareIndex("my-index").setSource("@timestamp", "2005-01-01T00:30").get();
        refresh();

        SearchResponse response = client().prepareSearch("my-index")
                .addAggregation(AggregationBuilders.dateHistogram("rate").field("@timestamp")
                        .dateHistogramInterval(DateHistogramInterval.HOUR).order(BucketOrder.count(false)))
                .get();

        CompareCondition condition = new CompareCondition("ctx.payload.aggregations.rate.buckets.0.doc_count", CompareCondition.Op.GTE, 5,
                Clock.systemUTC());
        WatchExecutionContext ctx = mockExecutionContext("_name", new Payload.XContent(response, ToXContent.EMPTY_PARAMS));
        CompareCondition.Result result = condition.execute(ctx);
        assertThat(result.met(), is(false));
        Map<String, Object> resolvedValues = result.getResolvedValues();
        assertThat(resolvedValues, notNullValue());
        assertThat(resolvedValues.size(), is(1));
        assertThat(resolvedValues, hasEntry("ctx.payload.aggregations.rate.buckets.0.doc_count", (Object) 4));

        client().prepareIndex("my-index").setSource("@timestamp", "2005-01-01T00:40").get();
        refresh();

        response = client().prepareSearch("my-index")
                .addAggregation(AggregationBuilders.dateHistogram("rate")
                        .field("@timestamp").dateHistogramInterval(DateHistogramInterval.HOUR).order(BucketOrder.count(false)))
                .get();

        ctx = mockExecutionContext("_name", new Payload.XContent(response, ToXContent.EMPTY_PARAMS));
        result = condition.execute(ctx);
        assertThat(result.met(), is(true));
        resolvedValues = result.getResolvedValues();
        assertThat(resolvedValues, notNullValue());
        assertThat(resolvedValues.size(), is(1));
        assertThat(resolvedValues, hasEntry("ctx.payload.aggregations.rate.buckets.0.doc_count", (Object) 5));
    }

    public void testExecuteAccessHits() throws Exception {
        CompareCondition condition = new CompareCondition("ctx.payload.hits.hits.0._score", CompareCondition.Op.EQ, 1,
                Clock.systemUTC());
        SearchHit hit = new SearchHit(0, "1", null);
        hit.score(1f);
        hit.shard(new SearchShardTarget("a", new ShardId("a", "indexUUID", 0), null, OriginalIndices.NONE));

        InternalSearchResponse internalSearchResponse = new InternalSearchResponse(
                new SearchHits(new SearchHit[]{hit}, new TotalHits(1L, TotalHits.Relation.EQUAL_TO), 1f),
            null, null, null, false, false, 1);
        SearchResponse response = new SearchResponse(internalSearchResponse, "", 3, 3, 0,
            500L, ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);

        WatchExecutionContext ctx = mockExecutionContext("_watch_name", new Payload.XContent(response, ToXContent.EMPTY_PARAMS));
        assertThat(condition.execute(ctx).met(), is(true));
        hit.score(2f);
        when(ctx.payload()).thenReturn(new Payload.XContent(response, ToXContent.EMPTY_PARAMS));
        CompareCondition.Result result = condition.execute(ctx);
        assertThat(result.met(), is(false));
        Map<String, Object> resolvedValues = result.getResolvedValues();
        assertThat(resolvedValues, notNullValue());
        assertThat(resolvedValues.size(), is(1));
        assertThat(resolvedValues, hasEntry(is("ctx.payload.hits.hits.0._score"), notNullValue()));
    }
}

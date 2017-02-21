/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.condition;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.Index;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.xpack.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.watch.Payload;

import java.time.Clock;
import java.util.Map;

import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.mockExecutionContext;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.when;

public class CompareConditionSearchTests extends AbstractWatcherIntegrationTestCase {

    @Override
    protected boolean enableSecurity() {
        return true;
    }

    public void testExecuteWithAggs() throws Exception {
        client().prepareIndex("my-index", "my-type").setSource("@timestamp", "2005-01-01T00:00").get();
        client().prepareIndex("my-index", "my-type").setSource("@timestamp", "2005-01-01T00:10").get();
        client().prepareIndex("my-index", "my-type").setSource("@timestamp", "2005-01-01T00:20").get();
        client().prepareIndex("my-index", "my-type").setSource("@timestamp", "2005-01-01T00:30").get();
        refresh();

        SearchResponse response = client().prepareSearch("my-index")
                .addAggregation(AggregationBuilders.dateHistogram("rate").field("@timestamp")
                        .dateHistogramInterval(DateHistogramInterval.HOUR).order(Histogram.Order.COUNT_DESC))
                .get();

        CompareCondition condition = new CompareCondition("ctx.payload.aggregations.rate.buckets.0.doc_count", CompareCondition.Op.GTE, 5,
                Clock.systemUTC());
        WatchExecutionContext ctx = mockExecutionContext("_name", new Payload.XContent(response));
        CompareCondition.Result result = condition.execute(ctx);
        assertThat(result.met(), is(false));
        Map<String, Object> resolvedValues = result.getResolvedValues();
        assertThat(resolvedValues, notNullValue());
        assertThat(resolvedValues.size(), is(1));
        assertThat(resolvedValues, hasEntry("ctx.payload.aggregations.rate.buckets.0.doc_count", (Object) 4));

        client().prepareIndex("my-index", "my-type").setSource("@timestamp", "2005-01-01T00:40").get();
        refresh();

        response = client().prepareSearch("my-index")
                .addAggregation(AggregationBuilders.dateHistogram("rate")
                        .field("@timestamp").dateHistogramInterval(DateHistogramInterval.HOUR).order(Histogram.Order.COUNT_DESC))
                .get();

        ctx = mockExecutionContext("_name", new Payload.XContent(response));
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
        SearchHit hit = new SearchHit(0, "1", new Text("type"), null);
        hit.score(1f);
        hit.shard(new SearchShardTarget("a", new Index("a", "indexUUID"), 0));

        InternalSearchResponse internalSearchResponse = new InternalSearchResponse(
                new SearchHits(new SearchHit[]{hit}, 1L, 1f), null, null, null, false, false, 1);
        SearchResponse response = new SearchResponse(internalSearchResponse, "", 3, 3, 500L, new ShardSearchFailure[0]);

        WatchExecutionContext ctx = mockExecutionContext("_watch_name", new Payload.XContent(response));
        assertThat(condition.execute(ctx).met(), is(true));
        hit.score(2f);
        when(ctx.payload()).thenReturn(new Payload.XContent(response));
        CompareCondition.Result result = condition.execute(ctx);
        assertThat(result.met(), is(false));
        Map<String, Object> resolvedValues = result.getResolvedValues();
        assertThat(resolvedValues, notNullValue());
        assertThat(resolvedValues.size(), is(1));
        assertThat(resolvedValues, hasEntry(is("ctx.payload.hits.hits.0._score"), notNullValue()));
    }
}

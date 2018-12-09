/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.condition;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.Index;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptService;
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
import org.elasticsearch.xpack.watcher.test.WatcherMockScriptPlugin;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.mockExecutionContext;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

public class ScriptConditionSearchTests extends AbstractWatcherIntegrationTestCase {

    @Override
    protected List<Class<? extends Plugin>> pluginTypes() {
        List<Class<? extends Plugin>> types = super.pluginTypes();
        types.add(CustomScriptPlugin.class);
        return types;
    }

    public static class CustomScriptPlugin extends WatcherMockScriptPlugin {

        @Override
        @SuppressWarnings("unchecked")
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

            scripts.put("ctx.payload.aggregations.rate.buckets[0]?.doc_count >= 5", vars -> {
                List<?> buckets = (List<?>) XContentMapValues.extractValue("ctx.payload.aggregations.rate.buckets", vars);
                int docCount = (int) XContentMapValues.extractValue("doc_count", (Map<String, Object>) buckets.get(0));
                return docCount >= 5;
            });

            scripts.put("ctx.payload.hits?.hits[0]?._score == 1.0", vars -> {
                List<Map<String, Object>> searchHits = (List<Map<String, Object>>) XContentMapValues.extractValue("ctx.payload.hits.hits",
                        vars);
                double score = (double) XContentMapValues.extractValue("_score", searchHits.get(0));
                return score == 1.0;
            });

            return scripts;
        }
    }

    public void testExecuteWithAggs() throws Exception {
        client().prepareIndex("my-index", "my-type").setSource("@timestamp", "2005-01-01T00:00").get();
        client().prepareIndex("my-index", "my-type").setSource("@timestamp", "2005-01-01T00:10").get();
        client().prepareIndex("my-index", "my-type").setSource("@timestamp", "2005-01-01T00:20").get();
        client().prepareIndex("my-index", "my-type").setSource("@timestamp", "2005-01-01T00:30").get();
        refresh();

        SearchResponse response = client().prepareSearch("my-index")
                .addAggregation(AggregationBuilders.dateHistogram("rate").field("@timestamp")
                        .dateHistogramInterval(DateHistogramInterval.HOUR).order(BucketOrder.count(false)))
                .get();

        ScriptService scriptService = internalCluster().getInstance(ScriptService.class);
        ScriptCondition condition = new ScriptCondition(
                mockScript("ctx.payload.aggregations.rate.buckets[0]?.doc_count >= 5"),
                scriptService);

        WatchExecutionContext ctx = mockExecutionContext("_name", new Payload.XContent(response));
        assertFalse(condition.execute(ctx).met());

        client().prepareIndex("my-index", "my-type").setSource("@timestamp", "2005-01-01T00:40").get();
        refresh();

        response = client().prepareSearch("my-index").addAggregation(AggregationBuilders.dateHistogram("rate").field("@timestamp")
                .dateHistogramInterval(DateHistogramInterval.HOUR).order(BucketOrder.count(false)))
                .get();

        ctx = mockExecutionContext("_name", new Payload.XContent(response));
        assertThat(condition.execute(ctx).met(), is(true));
    }

    public void testExecuteAccessHits() throws Exception {
        ScriptService scriptService = internalCluster().getInstance(ScriptService.class);
        ScriptCondition condition = new ScriptCondition(
                mockScript("ctx.payload.hits?.hits[0]?._score == 1.0"), scriptService);
        SearchHit hit = new SearchHit(0, "1", new Text("type"), null);
        hit.score(1f);
        hit.shard(new SearchShardTarget("a", new Index("a", "testUUID"), 0, null));

        InternalSearchResponse internalSearchResponse = new InternalSearchResponse(new SearchHits(
                new SearchHit[]{hit}, 1L, 1f), null, null, null, false, false, 1);
        SearchResponse response = new SearchResponse(internalSearchResponse, "", 3, 3, 0, 500L, ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY);

        WatchExecutionContext ctx = mockExecutionContext("_watch_name", new Payload.XContent(response));
        assertThat(condition.execute(ctx).met(), is(true));
        hit.score(2f);
        when(ctx.payload()).thenReturn(new Payload.XContent(response));
        assertThat(condition.execute(ctx).met(), is(false));
    }
}

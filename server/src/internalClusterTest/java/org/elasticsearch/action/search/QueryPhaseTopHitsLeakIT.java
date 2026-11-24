/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;

/** Verifies cleanup after an ordinary highlighting error aborts fetch following a top_hits reduction. */
public class QueryPhaseTopHitsLeakIT extends ESSingleNodeTestCase {
    public void testTopHitsReleasedAfterHighlightFailure() {
        assertTopHitsReleasedAfterSearch(true);
    }

    public void testTopHitsReleasedAfterSuccessfulHighlight() {
        assertTopHitsReleasedAfterSearch(false);
    }

    private void assertTopHitsReleasedAfterSearch(boolean failHighlight) {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("test")
                .setSettings(Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0))
                .setMapping("title", "type=text,term_vector=no")
        );
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("title", "a test document").get();
        }
        client().admin().indices().prepareRefresh("test").get();

        // Observe the normal reduction callbacks without taking or transferring any references.
        List<SearchHits> reducedHits = new ArrayList<>();
        AtomicInteger fetchFailures = new AtomicInteger();
        SearchProgressListener progressListener = new SearchProgressListener() {
            @Override
            protected void onPartialReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
                InternalAggregations.addTopHitsToReleaseList(aggs, reducedHits, false);
            }

            @Override
            protected void onFinalReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
                InternalAggregations.addTopHitsToReleaseList(aggs, reducedHits, false);
            }

            @Override
            protected void onFetchFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
                fetchFailures.incrementAndGet();
            }
        };
        SearchRequest request = new SearchRequest("test") {
            @Override
            public SearchTask createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
                SearchTask task = super.createTask(id, type, action, parentTaskId, headers);
                task.setProgressListener(progressListener);
                return task;
            }
        };
        request.allowPartialSearchResults(false);
        request.source(
            new SearchSourceBuilder().query(matchQuery("title", "test"))
                .size(10)
                .aggregation(AggregationBuilders.topHits("th").size(1))
                // fvh is invalid for the mapping above. Only the main hits request highlighting;
                // the aggregation's own fetch succeeds during the query phase.
                .highlighter(new HighlightBuilder().field("title").highlighterType(failHighlight ? "fvh" : "unified"))
        );
        ActionFuture<SearchResponse> future = client().execute(TransportSearchAction.TYPE, request);
        if (failHighlight) {
            SearchPhaseExecutionException failure = expectThrows(SearchPhaseExecutionException.class, future::actionGet);
            assertThat(ExceptionsHelper.stackTrace(failure), containsString("should be indexed with term vector"));
            assertThat(fetchFailures.get(), greaterThan(0));
        } else {
            SearchResponse response = future.actionGet();
            try {
                assertEquals(0, response.getFailedShards());
                assertEquals(10, response.getHits().getHits().length);
                assertEquals(0, fetchFailures.get());
            } finally {
                response.decRef();
            }
        }
        assertFalse("query reduction must have produced hits", reducedHits.isEmpty());
        for (SearchHits hits : reducedHits) {
            assertTrue(hits.isPooled());
            assertFalse("top_hits must be released after search completes (failHighlight=" + failHighlight + ")", hits.hasReferences());
        }
    }
}

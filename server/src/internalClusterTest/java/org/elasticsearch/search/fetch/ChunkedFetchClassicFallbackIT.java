/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.hamcrest.Matchers.equalTo;

/**
 * A coordinating node asks for a chunked fetch from every data node that supports it, but a data node streams chunks only when
 * its own {@code search.fetch_phase_chunked_enabled} is on and it can connect back to the coordinator. Otherwise it answers with a
 * classic fetch. These tests run searches through a coordinating node with chunked fetch on against shards that live on a data
 * node with it off, as during a rolling upgrade from a version where chunked fetch is off by default.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class ChunkedFetchClassicFallbackIT extends ESIntegTestCase {

    private static final String INDEX = "classic_fallback";
    private static final int DOCS = 12;
    private static final int NO_RANK = -1;

    private String startClusterAndIndex() throws Exception {
        internalCluster().startMasterOnlyNode();
        String classicFetchNode = internalCluster().startDataOnlyNode(
            Settings.builder().put(SearchService.FETCH_PHASE_CHUNKED_ENABLED.getKey(), false).build()
        );
        String coordinator = internalCluster().startCoordinatingOnlyNode(
            Settings.builder().put(SearchService.FETCH_PHASE_CHUNKED_ENABLED.getKey(), true).build()
        );
        assertAcked(
            prepareCreate(INDEX).setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put("index.routing.allocation.require._name", classicFetchNode)
            ).setMapping("value", "type=integer", "text", "type=text")
        );
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 1; i <= DOCS; i++) {
            docs.add(prepareIndex(INDEX).setId(Integer.toString(i)).setSource("value", i, "text", "fox " + "quick ".repeat(i)));
        }
        indexRandom(true, docs);
        ensureGreen(INDEX);
        return coordinator;
    }

    public void testRelevanceSearchHasNoRank() throws Exception {
        String coordinator = startClusterAndIndex();
        assertNoFailuresAndResponse(
            internalCluster().client(coordinator).prepareSearch(INDEX).setQuery(QueryBuilders.matchQuery("text", "quick")).setSize(DOCS),
            response -> {
                assertThat(response.getHits().getHits().length, equalTo(DOCS));
                for (SearchHit hit : response.getHits().getHits()) {
                    assertThat("rank of hit " + hit.getId(), hit.getRank(), equalTo(NO_RANK));
                }
            }
        );
    }

    public void testFieldSortLeavesScoreUnset() throws Exception {
        String coordinator = startClusterAndIndex();
        assertNoFailuresAndResponse(
            internalCluster().client(coordinator)
                .prepareSearch(INDEX)
                .setQuery(QueryBuilders.matchAllQuery())
                .addSort("value", SortOrder.ASC)
                .setSize(DOCS),
            response -> {
                assertThat(response.getHits().getHits().length, equalTo(DOCS));
                for (SearchHit hit : response.getHits().getHits()) {
                    assertTrue("score of hit " + hit.getId() + " is " + hit.getScore(), Float.isNaN(hit.getScore()));
                }
            }
        );
    }

    public void testFieldSortWithTrackedScores() throws Exception {
        String coordinator = startClusterAndIndex();
        assertNoFailuresAndResponse(
            internalCluster().client(coordinator)
                .prepareSearch(INDEX)
                .setQuery(
                    QueryBuilders.functionScoreQuery(ScoreFunctionBuilders.fieldValueFactorFunction("value"))
                        .boostMode(CombineFunction.REPLACE)
                )
                .addSort("value", SortOrder.DESC)
                .setTrackScores(true)
                .setSize(DOCS),
            response -> {
                SearchHit[] hits = response.getHits().getHits();
                assertThat(hits.length, equalTo(DOCS));
                for (int i = 0; i < hits.length; i++) {
                    SearchHit hit = hits[i];
                    int value = ((Number) hit.getSourceAsMap().get("value")).intValue();
                    assertThat("source value at position " + i, value, equalTo(DOCS - i));
                    assertThat("sort value of hit " + hit.getId(), ((Number) hit.getSortValues()[0]).intValue(), equalTo(value));
                    assertThat("score of hit " + hit.getId(), hit.getScore(), equalTo((float) value));
                    assertThat("rank of hit " + hit.getId(), hit.getRank(), equalTo(NO_RANK));
                }
            }
        );
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.scroll;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAllSuccessful;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class SearchScrollWithFailingNodesIT extends ESIntegTestCase {
    @Override
    protected int numberOfShards() {
        return 2;
    }

    @Override
    protected int numberOfReplicas() {
        return 0;
    }

    public void testScanScrollWithShardExceptions() throws Exception {
        internalCluster().startNode();
        internalCluster().startNode();
        assertAcked(
                prepareCreate("test")
                        // Enforces that only one shard can only be allocated to a single node
                        .setSettings(Settings.builder().put(indexSettings())
                                .put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1))
        );

        List<IndexRequestBuilder> writes = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            writes.add(
                    client().prepareIndex("test")
                            .setSource(jsonBuilder().startObject().field("field", i).endObject())
            );
        }
        indexRandom(false, writes);
        refresh();

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .setScroll(TimeValue.timeValueMinutes(1))
                .get();
        assertAllSuccessful(searchResponse);
        long numHits = 0;
        do {
            numHits += searchResponse.getHits().getHits().length;
            searchResponse = client()
                    .prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueMinutes(1))
                    .get();
            assertAllSuccessful(searchResponse);
        } while (searchResponse.getHits().getHits().length > 0);
        assertThat(numHits, equalTo(100L));
        clearScroll("_all");

        internalCluster().stopRandomNonMasterNode();

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .setScroll(TimeValue.timeValueMinutes(1))
                .get();
        assertThat(searchResponse.getSuccessfulShards(), lessThan(searchResponse.getTotalShards()));
        numHits = 0;
        int numberOfSuccessfulShards = searchResponse.getSuccessfulShards();
        do {
            numHits += searchResponse.getHits().getHits().length;
            searchResponse = client()
                    .prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueMinutes(1))
                    .get();
            assertThat(searchResponse.getSuccessfulShards(), equalTo(numberOfSuccessfulShards));
        } while (searchResponse.getHits().getHits().length > 0);
        assertThat(numHits, greaterThan(0L));

        clearScroll(searchResponse.getScrollId());
    }

}

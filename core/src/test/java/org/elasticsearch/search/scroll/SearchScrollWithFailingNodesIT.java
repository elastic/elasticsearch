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

package org.elasticsearch.search.scroll;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAllSuccessful;
import static org.hamcrest.Matchers.*;

/**
 */
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

    @Test
    public void testScanScrollWithShardExceptions() throws Exception {
        internalCluster().startNode();
        internalCluster().startNode();
        assertAcked(
                prepareCreate("test")
                        // Enforces that only one shard can only be allocated to a single node
                        .setSettings(Settings.builder().put(indexSettings()).put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE, 1))
        );

        List<IndexRequestBuilder> writes = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            writes.add(
                    client().prepareIndex("test", "type1")
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
            numHits += searchResponse.getHits().hits().length;
            searchResponse = client()
                    .prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueMinutes(1))
                    .get();
            assertAllSuccessful(searchResponse);
        } while (searchResponse.getHits().hits().length > 0);
        assertThat(numHits, equalTo(100l));
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
            numHits += searchResponse.getHits().hits().length;
            searchResponse = client()
                    .prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueMinutes(1))
                    .get();
            assertThat(searchResponse.getSuccessfulShards(), equalTo(numberOfSuccessfulShards));
        } while (searchResponse.getHits().hits().length > 0);
        assertThat(numHits, greaterThan(0l));

        clearScroll(searchResponse.getScrollId());
    }

}

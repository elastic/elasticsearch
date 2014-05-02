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

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numDataNodes = 2)
public class SearchScrollWithFailingNodesTests extends ElasticsearchIntegrationTest {

    @Test
    @TestLogging("action.search:TRACE")
    public void testScanScrollWithShardExceptions() throws Exception {
        int numPrimaryShards = between(2, 6); // We need at least 2 shards.
        assertAcked(
                client().admin().indices().prepareCreate("test")
                        .setSettings(
                                ImmutableSettings.settingsBuilder()
                                        .put("index.number_of_shards", numPrimaryShards)
                                        .put("index.number_of_replicas", 0)
                        )
        );

        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource(jsonBuilder().startObject().field("field", i).endObject())
                    .get();
        }
        refresh();

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .setScroll(TimeValue.timeValueMinutes(1))
                .get();
        long numHits = 0;
        do {
            numHits += searchResponse.getHits().hits().length;
            searchResponse = client()
                    .prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueMinutes(1))
                    .get();
        } while (searchResponse.getHits().hits().length > 0);
        assertThat(numHits, equalTo(100l));
        clearScroll("_all");

        cluster().stopRandomNonMasterNode();

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .setScroll(TimeValue.timeValueMinutes(1))
                .get();
        numHits = 0;
        do {
            numHits += searchResponse.getHits().hits().length;
            searchResponse = client()
                    .prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueMinutes(1))
                    .get();
        } while (searchResponse.getHits().hits().length > 0);
        assertThat(numHits, greaterThan(0l));
        clearScroll("_all");
    }

}

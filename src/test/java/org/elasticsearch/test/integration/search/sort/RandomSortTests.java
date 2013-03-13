/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test.integration.search.sort;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.test.integration.AbstractSharedClusterTest;
import org.junit.Test;

import java.util.Arrays;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class RandomSortTests extends AbstractSharedClusterTest {

    @Override
    public Settings getSettings() {
        return randomSettingsBuilder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 0)
                .build();
    }

    @Override
    protected int numberOfNodes() {
        return 2;
    }

    @Test
    public void testConsistentSeed() throws Exception {
        Client client = client();
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .endObject()).execute().actionGet();

        client.prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .endObject()).execute().actionGet();

        client.prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject()
                .endObject()).execute().actionGet();


        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();

        long seed = System.currentTimeMillis();

        String[] ids = null;

        for (int i = 0; i < 2; i++) {

            SearchResponse searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addSort(SortBuilders.randomSort(seed))
                    .execute().actionGet();

            assertThat("Failures " + Arrays.toString(searchResponse.getShardFailures()), searchResponse.getShardFailures().length, equalTo(0));

            assertThat(searchResponse.getHits().getTotalHits(), equalTo(3l));

            if (ids == null) {
                ids = new String[3];
                ids[0] = searchResponse.getHits().getAt(0).id();
                ids[1] = searchResponse.getHits().getAt(1).id();
                ids[2] = searchResponse.getHits().getAt(2).id();
            } else {
                assertThat(searchResponse.getHits().getAt(0).id(), equalTo(ids[0]));
                assertThat(searchResponse.getHits().getAt(1).id(), equalTo(ids[1]));
                assertThat(searchResponse.getHits().getAt(2).id(), equalTo(ids[2]));
            }
        }
    }

}

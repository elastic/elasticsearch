/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.test.integration.search.basic;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.integration.AbstractSharedClusterTest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class SearchWhileCreatingIndexTests extends AbstractSharedClusterTest {


    /**
     * This test basically verifies that search with a single shard active (cause we indexed to it) and other
     * shards possibly not active at all (cause they haven't allocated) will still work.
     */
    @Test
    public void searchWhileCreatingIndex() {
        try {
            client().admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }

        for (int i = 0; i < 20; i++) {
            client().admin().indices().prepareCreate("test").setSettings(settingsBuilder().put("index.number_of_shards", 10)).execute().actionGet();

            client().prepareIndex("test", "type1").setSource("field", "test").execute().actionGet();
            client().admin().indices().prepareRefresh().execute().actionGet();

            SearchResponse searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.termQuery("field", "test")).execute().actionGet();
            assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

            client().admin().indices().prepareDelete("test").execute().actionGet();
        }

        try {
            client().admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
    }
}
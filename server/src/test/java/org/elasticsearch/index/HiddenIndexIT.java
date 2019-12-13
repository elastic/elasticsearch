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

package org.elasticsearch.index;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;

public class HiddenIndexIT extends ESIntegTestCase {

    public void testHiddenIndexSearch() {
        CreateIndexResponse createResponse = client().admin().indices().prepareCreate("hidden-index")
            .setSettings(Settings.builder().put("index.hidden", true).build())
            .setWaitForActiveShards(ActiveShardCount.ALL)
            .get();
        assertTrue(createResponse.isAcknowledged());
        assertTrue(createResponse.isShardsAcknowledged());

        // default not visible
        client().prepareIndex("hidden-index").setSource("foo", "bar").setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();
        SearchResponse searchResponse =
            client().prepareSearch(randomFrom("*", "_all", "h*", "*index")).setSize(1000).setQuery(QueryBuilders.matchAllQuery()).get();
        boolean matchedHidden = Arrays.stream(searchResponse.getHits().getHits()).anyMatch(hit -> "hidden-index".equals(hit.getIndex()));
        assertFalse(matchedHidden);

        // direct access allowed
        searchResponse = client().prepareSearch("hidden-index").setSize(1000).setQuery(QueryBuilders.matchAllQuery()).get();
        matchedHidden = Arrays.stream(searchResponse.getHits().getHits()).anyMatch(hit -> "hidden-index".equals(hit.getIndex()));
        assertTrue(matchedHidden);

        // with indices option to include hidden
        searchResponse = client().prepareSearch(randomFrom("*", "_all", "h*", "*index"))
            .setSize(1000)
            .setQuery(QueryBuilders.matchAllQuery())
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_HIDDEN)
            .get();
        matchedHidden = Arrays.stream(searchResponse.getHits().getHits()).anyMatch(hit -> "hidden-index".equals(hit.getIndex()));
        assertTrue(matchedHidden);
    }
}

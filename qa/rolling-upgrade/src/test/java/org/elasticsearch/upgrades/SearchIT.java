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

package org.elasticsearch.upgrades;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.rest.action.document.RestBulkAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SearchIT extends AbstractRollingTestCase {

    public void testSingleIndexScoring() throws IOException {
        if (CLUSTER_TYPE == ClusterType.OLD) {
            bulk("old");
            search("old");
        }
        if (CLUSTER_TYPE == ClusterType.MIXED) {
            waitForYellow();
            if (indexExists("mixed") == false) {
                bulk("mixed");
            }
            search("old");
            search("mixed");
        }
        if (CLUSTER_TYPE == ClusterType.UPGRADED) {
            waitForYellow();
            bulk("upgraded");
            search("old");
            search("mixed");
            search("upgraded");
        }
    }

    private static void waitForYellow() throws IOException {
        Request waitForYellow = new Request("GET", "/_cluster/health");
        waitForYellow.addParameter("wait_for_nodes", "3");
        waitForYellow.addParameter("wait_for_status", "yellow");
        client().performRequest(waitForYellow);
    }

    private static void search(String index) throws IOException {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(new MatchQueryBuilder("text", "quick brown fox dog"));
        Request request = new Request("GET", "/" + index + "/_search");
        request.addParameter("search_type", "dfs_query_then_fetch");
        request.setJsonEntity(Strings.toString(sourceBuilder));
        Response response = client().performRequest(request);
        String responseString = EntityUtils.toString(response.getEntity());
        SearchResponse searchResponse = SearchResponse.fromXContent(JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION, responseString));
        assertEquals(5, searchResponse.getHits().getTotalHits().value);
        SearchHit[] hits = searchResponse.getHits().getHits();
        assertEquals(5, hits.length);
        //System.out.println(Strings.toString(searchResponse.getHits(), true, true));
        for (int i = 0; i < hits.length; i++) {
            assertEquals(String.valueOf(i), hits[i].getId());
        }
    }

    private static void bulk(String index) throws IOException {
        createIndex(index, Settings.builder().put("index.number_of_shards", randomIntBetween(2, 4))
            .put("index.number_of_replicas", randomIntBetween(0, 1)).build());
        String[] values = new String[]{
            "The quick brown fox jumps over the lazy dog",
            "The quick brown dog jumps over the lazy cat",
            "The brown fox jumps over the lazy dog",
            "The quick fox jumps over the lazy cat",
            "The fox jumps over the lazy dog",
        };
        List<String> bulkItems = new ArrayList<>(values.length);
        for (int i = 0; i < values.length; i++) {
            bulkItems.add("{\"index\": {\"_index\": \"" + index + "\", \"_type\": \"_doc\", \"_id\":" + i + "}}\n" +
                "{\"text\": \"" + values[i] + "\"}\n");
        }
        Collections.shuffle(bulkItems, random());
        StringBuilder b = new StringBuilder();
        for (String bulkItem : bulkItems) {
            b.append(bulkItem);
        }
        Request bulk = new Request("POST", "/_bulk");
        bulk.addParameter("refresh", "true");
        bulk.setOptions(expectWarnings(RestBulkAction.TYPES_DEPRECATION_MESSAGE));
        bulk.setJsonEntity(b.toString());
        client().performRequest(bulk);
    }
}

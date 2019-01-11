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
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Test that checks search api behaviour while performing a rolling upgrade
 */
public class SearchIT extends AbstractRollingTestCase {

    private static final String[] VALUES = new String[]{
        //values are in the expected scoring order when searchging for "quick brown fox dog", docs wil be shuffled
        //but the ids get assigned to them based on the order in this array
        "The quick brown fox jumps over the lazy dog",
        "The quick brown dog jumps over the lazy cat",
        "The brown fox jumps over the lazy dog",
        "The quick fox jumps over the lazy cat",
        "The fox jumps over the lazy dog",
    };

    private static final List<Tuple<String, String>> DOCS = new ArrayList<>(VALUES.length);

    @BeforeClass
    public static void setupDocs() {
        for (int i = 0; i < VALUES.length; i++) {
            DOCS.add(Tuple.tuple(String.valueOf(i), "{\"text\": \"" + VALUES[i] + "\"}"));
        }
        Collections.shuffle(DOCS, random());
    }

    /**
     * Check that documents ordering stays the same during the upgrade and after when searching against an index made of multiple shards.
     * Tests the bw compatibility layer introduced after removing the k1+1 constant factor from the numerator of the bm25 scoring formula.
     */
    public void testSingleIndexScoring() throws IOException {
        if (CLUSTER_TYPE == ClusterType.OLD) {
            index("single-old");
            search("single-old");
        }
        if (CLUSTER_TYPE == ClusterType.MIXED) {
            waitForGreen();
            if (indexExists("single-mixed") == false) {
                index("single-mixed");
            }
            search("single-old");
            search("single-mixed");
        }
        if (CLUSTER_TYPE == ClusterType.UPGRADED) {
            waitForGreen();
            index("single-upgraded");
            search("single-old");
            search("single-mixed");
            search("single-upgraded");
        }
    }

    /**
     * Check that documents ordering stays the same during the upgrade and after when searching against multiple indices, and newer
     * indices are explcitly configured to use the LegacyBM25 similarity.
     */
    public void testMultipleIndicesScoring() throws IOException {
        if (CLUSTER_TYPE == ClusterType.OLD) {
            createIndex("multi-old", Settings.builder().put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0).build());
            Tuple<String, String> document = randomFrom(DOCS);
            //index 1 of the 5 documents n an index created before the upgrade
            index("multi-old", document);
        }
        if (CLUSTER_TYPE == ClusterType.MIXED) {
            waitForGreen();
            if (indexExists("multi-mixed") == false) {
                createIndex("multi-mixed", Settings.builder().put("index.number_of_shards", randomBoolean() ? 1 : 2)
                    .put("index.number_of_replicas", 0).build());
                //index 3 of the 5 documents in an index created during the upgrade
                indexNewDocs("multi-mixed", 3);
            }
        }
        if (CLUSTER_TYPE == ClusterType.UPGRADED) {
            waitForGreen();
            createIndex("multi-upgraded", Settings.builder().put("index.similarity.default.type", "LegacyBM25").build());
            //index the last document in an index created after the upgrade, which has explicitly set LegacyBM25
            indexNewDocs("multi-upgraded", 1);
            //finally test that the order is the same as if the documents were all indexed in the same index
            search("multi-old,multi-mixed,multi-upgraded");
        }
    }

    private static void indexNewDocs(String index, int numDocs) throws IOException {
        for (int i = 0; i < numDocs; i++) {
            Tuple<String, String> document;
            do {
                document = randomFrom(DOCS);
            } while (docExists("multi-old", document.v1()) || docExists("multi-mixed", document.v1()));
            index(index, document);
        }
    }

    private static boolean docExists(String index, String id) throws IOException {
        Request existsRequest = new Request("HEAD", "/" + index + "/_doc/" + id);
        return client().performRequest(existsRequest).getStatusLine().getStatusCode() == 200;
    }

    private static void waitForGreen() throws IOException {
        Request waitForGreen = new Request("GET", "/_cluster/health");
        waitForGreen.addParameter("wait_for_nodes", "3");
        waitForGreen.addParameter("wait_for_status", "green");
        waitForGreen.addParameter("timeout", "70s");
        client().performRequest(waitForGreen);
    }

    private static void search(String index) throws IOException {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(new MatchQueryBuilder("text", "quick brown fox dog"));
        Request request = new Request("GET", "/" + index + "/_search");
        request.addParameter("search_type", "dfs_query_then_fetch");
        request.setJsonEntity(Strings.toString(sourceBuilder));
        SearchResponse searchResponse = search(request);
        assertEquals(5, searchResponse.getHits().getTotalHits().value);
        SearchHit[] hits = searchResponse.getHits().getHits();
        assertEquals(5, hits.length);
        for (int i = 0; i < hits.length; i++) {
            assertEquals(String.valueOf(i), hits[i].getId());
        }
    }

    private static SearchResponse search(Request request) throws IOException {
        Response response = client().performRequest(request);
        String responseString = EntityUtils.toString(response.getEntity());
        return SearchResponse.fromXContent(JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION, responseString));
    }

    private static void index(String index) throws IOException {
        createIndex(index, Settings.builder().put("index.number_of_shards", randomIntBetween(2, 4))
            .put("index.number_of_replicas", 0).build());
        for (Tuple<String, String> document : DOCS) {
            index(index, document);
        }
    }

    private static void index(String index, Tuple<String, String> doc) throws IOException {
        String id = doc.v1();
        String body = doc.v2();
        Request request = new Request("PUT", "/" + index + "/_doc/" + id);
        request.addParameter("refresh", "wait_for");
        request.setJsonEntity(body);
        assertEquals(201, client().performRequest(request).getStatusLine().getStatusCode());
    }
}

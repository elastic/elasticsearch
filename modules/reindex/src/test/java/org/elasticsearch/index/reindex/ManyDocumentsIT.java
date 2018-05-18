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

package org.elasticsearch.index.reindex;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.hasEntry;

/**
 * Tests {@code _update_by_query}, {@code _delete_by_query}, and {@code _reindex}
 * of many documents over REST. It is important to test many documents to make
 * sure that we don't change the default behavior of touching <strong>all</strong>
 * documents in the request.
 */
public class ManyDocumentsIT extends ESRestTestCase {
    private final int count = between(150, 2000);

    @Before
    public void setupTestIndex() throws IOException {
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < count; i++) {
            bulk.append("{\"index\":{}}\n");
            bulk.append("{\"test\":\"test\"}\n");
        }
        client().performRequest("POST", "/test/test/_bulk", singletonMap("refresh", "true"),
                new StringEntity(bulk.toString(), ContentType.APPLICATION_JSON));
    }

    public void testReindex() throws IOException {
        Map<String, Object> response = toMap(client().performRequest("POST", "/_reindex", emptyMap(), new StringEntity(
                "{\"source\":{\"index\":\"test\"}, \"dest\":{\"index\":\"des\"}}",
                ContentType.APPLICATION_JSON)));
        assertThat(response, hasEntry("total", count));
        assertThat(response, hasEntry("created", count));
    }

    public void testReindexFromRemote() throws IOException {
        Map<?, ?> nodesInfo = toMap(client().performRequest("GET", "/_nodes/http"));
        nodesInfo = (Map<?, ?>) nodesInfo.get("nodes");
        Map<?, ?> nodeInfo = (Map<?, ?>) nodesInfo.values().iterator().next();
        Map<?, ?> http = (Map<?, ?>) nodeInfo.get("http");
        String remote = "http://"+ http.get("publish_address");
        Map<String, Object> response = toMap(client().performRequest("POST", "/_reindex", emptyMap(), new StringEntity(
                "{\"source\":{\"index\":\"test\",\"remote\":{\"host\":\"" + remote + "\"}}, \"dest\":{\"index\":\"des\"}}",
                ContentType.APPLICATION_JSON)));
        assertThat(response, hasEntry("total", count));
        assertThat(response, hasEntry("created", count));
    }


    public void testUpdateByQuery() throws IOException {
        Map<String, Object> response = toMap(client().performRequest("POST", "/test/_update_by_query"));
        assertThat(response, hasEntry("total", count));
        assertThat(response, hasEntry("updated", count));
    }

    public void testDeleteByQuery() throws IOException {
        Map<String, Object> response = toMap(client().performRequest("POST", "/test/_delete_by_query", emptyMap(), new StringEntity(
                "{\"query\":{\"match_all\":{}}}",
                ContentType.APPLICATION_JSON)));
        assertThat(response, hasEntry("total", count));
        assertThat(response, hasEntry("deleted", count));
    }

    static Map<String, Object> toMap(Response response) throws IOException {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), false);
    }

}

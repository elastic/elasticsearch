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

package org.elasticsearch.client;

import org.apache.http.entity.StringEntity;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class HLClientSearchIT extends ESRestTestCase {

    private HighlevelClient aClient;

    @Before
    public void init() {
        this.aClient =  new HighlevelClient(client());
    }

    public void createTestDoc() throws IOException {
        Map<String, String> params = new HashMap<>();
        client().performRequest("PUT", "test", params,
                new StringEntity("{ \"mappings\": { \"type\" : { \"properties\" : { \"foo\": { \"type\": \"text\", \"store\": true },"
                        + "  \"title\": { \"type\": \"text\", \"store\" : true } } } } }"));
        params.put("refresh", "wait_for");
        client().performRequest("PUT", "test/type/1", params, new StringEntity("{\"foo\": \"bar\", \"title\" : \"baz\"}"));
    }

    public void testSearch() throws IOException {
        createTestDoc();
        SearchResponse searchResponse = aClient.performSearchRequest(new SearchRequest(
                new SearchSourceBuilder().version(true).storedFields(Arrays.asList("_source", "foo", "title"))));
        assertFalse(searchResponse.isTimedOut());
        assertTrue(searchResponse.getTookInMillis() > 0);
        assertEquals(5, searchResponse.getTotalShards());
        assertEquals(5, searchResponse.getSuccessfulShards());
        assertEquals(0, searchResponse.getFailedShards());
        assertEquals(1, searchResponse.getHits().getTotalHits());
        assertEquals(1, searchResponse.getHits().totalHits());
        assertEquals(1.0, searchResponse.getHits().maxScore(), Float.MIN_VALUE);
        assertEquals(1.0, searchResponse.getHits().getMaxScore(), Float.MIN_VALUE);
        assertEquals("bar", searchResponse.getHits().hits()[0].sourceAsMap().get("foo"));
        assertEquals("test", searchResponse.getHits().hits()[0].index());
        assertEquals("type", searchResponse.getHits().hits()[0].type());
        assertEquals("1", searchResponse.getHits().hits()[0].id());
        assertEquals(1, searchResponse.getHits().hits()[0].version());
        assertEquals(1.0, searchResponse.getHits().hits()[0].score(), Float.MIN_VALUE);
        assertEquals(2, searchResponse.getHits().hits()[0].fields().size());
        assertEquals("bar", searchResponse.getHits().hits()[0].field("foo").getValue());
        assertEquals("baz", searchResponse.getHits().hits()[0].field("title").getValue());
        assertNull(searchResponse.getHits().hits()[0].field("something"));
    }

    @After
    public void shutDown() throws IOException {
        this.aClient.close();
    }

}

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

package org.elasticsearch.client.highlevel.search;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.RequestLine;
import org.apache.http.StatusLine;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicRequestLine;
import org.apache.http.message.BasicStatusLine;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.highlevel.search.SearchResponse;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;

import static org.elasticsearch.client.RestClientTestUtil.mockResponse;

public class SearchResponseTests extends ESTestCase {

    public void testGet() throws IOException {
        Response restResponse = mockResponse("{ \"foo\" : { \"bar\" : \"baz\" } }");
        SearchResponse response = new SearchResponse(restResponse);
        assertEquals(LinkedHashMap.class, response.get("foo").getClass());
        assertEquals("baz", response.get("foo.bar"));
    }

    public void testBasicProperties() throws IOException {
        Response restResponse = mockResponse(
                "{ \"took\" : 63, \"timed_out\" : false, \"_shards\" : { \"total\" : 5, \"successful\" : 4, \"failed\" : 1 }}");
        SearchResponse response = new SearchResponse(restResponse);
        assertEquals(63L, response.getTookInMillis());
        assertFalse(response.isTimedOut());
        assertEquals(5, response.getTotalShards());
        assertEquals(4, response.getSuccessfulShards());
        assertEquals(1, response.getFailedShards());
    }
}

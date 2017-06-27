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

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
import static org.mockito.Mockito.mock;

public class RestReindexActionTests extends ESTestCase {
    public void testBuildRemoteInfoNoRemote() throws IOException {
        assertNull(RestReindexAction.buildRemoteInfo(new HashMap<>()));
    }

    public void testBuildRemoteInfoFullyLoaded() throws IOException {
        Map<String, String> headers = new HashMap<>();
        headers.put("first", "a");
        headers.put("second", "b");
        headers.put("third", "");

        Map<String, Object> remote = new HashMap<>();
        remote.put("host", "https://example.com:9200");
        remote.put("username", "testuser");
        remote.put("password", "testpass");
        remote.put("headers", headers);
        remote.put("socket_timeout", "90s");
        remote.put("connect_timeout", "10s");

        Map<String, Object> query = new HashMap<>();
        query.put("a", "b");

        Map<String, Object> source = new HashMap<>();
        source.put("remote", remote);
        source.put("query", query);

        RemoteInfo remoteInfo = RestReindexAction.buildRemoteInfo(source);
        assertEquals("https", remoteInfo.getScheme());
        assertEquals("example.com", remoteInfo.getHost());
        assertEquals(9200, remoteInfo.getPort());
        assertEquals("{\n  \"a\" : \"b\"\n}", remoteInfo.getQuery().utf8ToString());
        assertEquals("testuser", remoteInfo.getUsername());
        assertEquals("testpass", remoteInfo.getPassword());
        assertEquals(headers, remoteInfo.getHeaders());
        assertEquals(timeValueSeconds(90), remoteInfo.getSocketTimeout());
        assertEquals(timeValueSeconds(10), remoteInfo.getConnectTimeout());
    }

    public void testBuildRemoteInfoWithoutAllParts() throws IOException {
        expectThrows(IllegalArgumentException.class, () -> buildRemoteInfoHostTestCase("example.com"));
        expectThrows(IllegalArgumentException.class, () -> buildRemoteInfoHostTestCase("example.com:9200"));
        expectThrows(IllegalArgumentException.class, () -> buildRemoteInfoHostTestCase("http://example.com"));
    }

    public void testBuildRemoteInfoWithAllHostParts() throws IOException {
        RemoteInfo info = buildRemoteInfoHostTestCase("http://example.com:9200");
        assertEquals("http", info.getScheme());
        assertEquals("example.com", info.getHost());
        assertEquals(9200, info.getPort());
        assertEquals(RemoteInfo.DEFAULT_SOCKET_TIMEOUT, info.getSocketTimeout()); // Didn't set the timeout so we should get the default
        assertEquals(RemoteInfo.DEFAULT_CONNECT_TIMEOUT, info.getConnectTimeout()); // Didn't set the timeout so we should get the default

        info = buildRemoteInfoHostTestCase("https://other.example.com:9201");
        assertEquals("https", info.getScheme());
        assertEquals("other.example.com", info.getHost());
        assertEquals(9201, info.getPort());
        assertEquals(RemoteInfo.DEFAULT_SOCKET_TIMEOUT, info.getSocketTimeout());
        assertEquals(RemoteInfo.DEFAULT_CONNECT_TIMEOUT, info.getConnectTimeout());
    }

    public void testReindexFromRemoteRequestParsing() throws IOException {
        BytesReference request;
        try (XContentBuilder b = JsonXContent.contentBuilder()) {
            b.startObject(); {
                b.startObject("source"); {
                    b.startObject("remote"); {
                        b.field("host", "http://localhost:9200");
                    }
                    b.endObject();
                    b.field("index", "source");
                }
                b.endObject();
                b.startObject("dest"); {
                    b.field("index", "dest");
                }
                b.endObject();
            }
            b.endObject();
            request = b.bytes();
        }
        try (XContentParser p = createParser(JsonXContent.jsonXContent, request)) {
            ReindexRequest r = new ReindexRequest(new SearchRequest(), new IndexRequest());
            RestReindexAction.PARSER.parse(p, r, null);
            assertEquals("localhost", r.getRemoteInfo().getHost());
            assertArrayEquals(new String[] {"source"}, r.getSearchRequest().indices());
        }
    }

    public void testPipelineQueryParameterIsError() throws IOException {
        RestReindexAction action = new RestReindexAction(Settings.EMPTY, mock(RestController.class));

        FakeRestRequest.Builder request = new FakeRestRequest.Builder(xContentRegistry());
        try (XContentBuilder body = JsonXContent.contentBuilder().prettyPrint()) {
            body.startObject(); {
                body.startObject("source"); {
                    body.field("index", "source");
                }
                body.endObject();
                body.startObject("dest"); {
                    body.field("index", "dest");
                }
                body.endObject();
            }
            body.endObject();
            request.withContent(body.bytes(), body.contentType());
        }
        request.withParams(singletonMap("pipeline", "doesn't matter"));
        Exception e = expectThrows(IllegalArgumentException.class, () -> action.buildRequest(request.build()));

        assertEquals("_reindex doesn't support [pipeline] as a query parmaeter. Specify it in the [dest] object instead.", e.getMessage());
    }

    private RemoteInfo buildRemoteInfoHostTestCase(String hostInRest) throws IOException {
        Map<String, Object> remote = new HashMap<>();
        remote.put("host", hostInRest);

        Map<String, Object> source = new HashMap<>();
        source.put("remote", remote);

        return RestReindexAction.buildRemoteInfo(source);
    }
}

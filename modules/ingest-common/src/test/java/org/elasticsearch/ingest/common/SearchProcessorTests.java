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

package org.elasticsearch.ingest.common;

import org.apache.http.HttpEntity;
import org.apache.http.RequestLine;
import org.apache.http.StatusLine;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentGenerator;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchProcessorTests extends ESTestCase {

    private static final String EMPTY = "{\"took\":1,\"timed_out\":false,\"_shards\":{\"total\":0,\"successful\":0,\"failed\":0}," +
        "\"hits\":{\"total\":0,\"max_score\":0.0,\"hits\":[]}}";

    private static final String BAR = "{\"took\":1,\"timed_out\":false,\"_shards\":{\"total\":5,\"successful\":5,\"failed\":0}," +
        "\"hits\":{\"total\":1,\"max_score\":1.0,\"hits\":[{\"_index\":\"foo\",\"_type\":\"bar\",\"_id\":\"1\",\"_score\":1.0," +
        "\"_source\":{ \"foo\" : \"bar\" }}]}}";

    private static final String BAZ = "{\"took\":1,\"timed_out\":false,\"_shards\":{\"total\":5,\"successful\":5,\"failed\":0}," +
        "\"hits\":{\"total\":1,\"max_score\":1.0,\"hits\":[{\"_index\":\"foo\",\"_type\":\"bar\",\"_id\":\"2\",\"_score\":1.0," +
        "\"_source\":{ \"foo\" : \"baz\" }}]}}";

    private static final String BAR_BAZ = "{\"took\":2,\"timed_out\":false,\"_shards\":{\"total\":5,\"successful\":5,\"failed\":0}," +
        "\"hits\":{\"total\":2,\"max_score\":1.0,\"hits\":[{\"_index\":\"foo\",\"_type\":\"bar\",\"_id\":\"2\",\"_score\":1.0," +
        "\"_source\":{ \"foo\" : \"baz\" }},{\"_index\":\"foo\",\"_type\":\"bar\",\"_id\":\"1\",\"_score\":1.0,\"_source\":{ \"foo\" : " +
        "\"bar\" }}]}}";

    public void testSearchNoResult() throws Exception {
        SearchProcessor processor = mockSearchEngine(SearchProcessor.Factory.DEFAULT_QUERY, EMPTY,
            SearchProcessor.Factory.DEFAULT_PATH, "target");

        Map<String, Object> document = new HashMap<>();
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        processor.execute(ingestDocument);

        assertThat(ingestDocument.hasField("target.foo"), is(false));
    }

    public void testSearchOneResult() throws Exception {
        SearchProcessor processor = mockSearchEngine(SearchProcessor.Factory.DEFAULT_QUERY, randomFrom(BAR, BAZ),
            SearchProcessor.Factory.DEFAULT_PATH, "target");

        Map<String, Object> document = new HashMap<>();
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        processor.execute(ingestDocument);

        assertThat(ingestDocument.getFieldValue("target.foo", String.class), isOneOf("bar", "baz"));
    }

    public void testSearchTwoResult() throws Exception {
        SearchProcessor processor = mockSearchEngine(SearchProcessor.Factory.DEFAULT_QUERY, BAR_BAZ,
            SearchProcessor.Factory.DEFAULT_PATH, "target");

        Map<String, Object> document = new HashMap<>();
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        processor.execute(ingestDocument);

        assertThat(ingestDocument.getFieldValue("target.foo", String.class), is("baz"));
    }

    public void testSearchDeeperPath() throws Exception {
        SearchProcessor processor = mockSearchEngine(SearchProcessor.Factory.DEFAULT_QUERY, randomFrom(BAR, BAZ),
            SearchProcessor.Factory.DEFAULT_PATH, "target.something");

        Map<String, Object> document = new HashMap<>();
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        processor.execute(ingestDocument);

        assertThat(ingestDocument.getFieldValue("target.something.foo", String.class), isOneOf("bar", "baz"));
    }

    public void testMergingObjectsInDocument() throws Exception {
        SearchProcessor processor = mockSearchEngine(SearchProcessor.Factory.DEFAULT_QUERY, randomFrom(BAR, BAZ),
            SearchProcessor.Factory.DEFAULT_PATH, "origin");

        Map<String, Object> document = new HashMap<>();
        Map<String, Object> origin = new HashMap<>();
        origin.put("my_foo", "my_bar");
        document.put("origin", origin);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        processor.execute(ingestDocument);

        assertThat(ingestDocument.getFieldValue("origin.foo", String.class), isOneOf("bar", "baz"));
        assertThat(ingestDocument.getFieldValue("origin.my_foo", String.class), is("my_bar"));
    }

    public void testOverwritingValueInDocument() throws Exception {
        SearchProcessor processor = mockSearchEngine(SearchProcessor.Factory.DEFAULT_QUERY, randomFrom(BAR, BAZ),
            SearchProcessor.Factory.DEFAULT_PATH, "origin");

        Map<String, Object> document = new HashMap<>();
        document.put("origin", "my_origin");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        processor.execute(ingestDocument);

        assertThat(ingestDocument.getFieldValue("origin.foo", String.class), isOneOf("bar", "baz"));
    }

    public void testSearchTopLevelMerge() throws Exception {
        SearchProcessor processor = mockSearchEngine(SearchProcessor.Factory.DEFAULT_QUERY, randomFrom(BAR, BAZ),
            SearchProcessor.Factory.DEFAULT_PATH, null);

        Map<String, Object> document = new HashMap<>();
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        processor.execute(ingestDocument);

        assertThat(ingestDocument.getFieldValue("foo", String.class), isOneOf("bar", "baz"));
    }

    protected SearchProcessor mockSearchEngine(String query, String jsonResponse, String path, String target) throws IOException {
        RestClient restClient = mock(RestClient.class);
        final Response response = response("GET", "/_search", RestStatus.OK);
        final StringEntity entity = new StringEntity(jsonResponse, ContentType.APPLICATION_JSON);
        when(response.getEntity()).thenReturn(entity);
        when(restClient.performRequest(eq("GET"), eq("/_search"), anyMapOf(String.class, String.class), any(HttpEntity.class)))
            .thenReturn(response);

        return new SearchProcessor(randomAsciiOfLength(10), restClient, null, null, query, path, target);
    }

    protected Response response(final String method, final String endpoint, final RestStatus status) {
        final Response response = mock(Response.class);
        final RequestLine requestLine = mock(RequestLine.class);
        when(requestLine.getMethod()).thenReturn(method);
        when(requestLine.getUri()).thenReturn(endpoint);
        final StatusLine statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(status.getStatus());

        when(response.getRequestLine()).thenReturn(requestLine);
        when(response.getStatusLine()).thenReturn(statusLine);

        return response;
    }

}

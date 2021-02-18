/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.datastreams;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.After;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class DataStreamsRestIT extends ESRestTestCase {

    @After
    public void cleanUp() throws IOException {
        adminClient().performRequest(new Request("DELETE", "_data_stream/*?expand_wildcards=hidden"));
    }

    public void testHiddenDataStream() throws IOException {
        // Create a template
        Request putComposableIndexTemplateRequest = new Request("POST", "/_index_template/hidden");
        putComposableIndexTemplateRequest.setJsonEntity("{\"index_patterns\": [\"hidden\"], \"data_stream\": {\"hidden\": true}}");
        assertOK(client().performRequest(putComposableIndexTemplateRequest));

        Request createDocRequest = new Request("POST", "/hidden/_doc?refresh=true");
        createDocRequest.setJsonEntity("{ \"@timestamp\": \"2020-10-22\", \"a\": 1 }");

        assertOK(client().performRequest(createDocRequest));

        Request getDataStreamsRequest = new Request("GET", "/_data_stream?expand_wildcards=hidden");
        Response response = client().performRequest(getDataStreamsRequest);
        Map<String, Object> dataStreams = entityAsMap(response);
        assertEquals(Collections.singletonList("hidden"), XContentMapValues.extractValue("data_streams.name", dataStreams));
        assertEquals(Collections.singletonList("hidden"), XContentMapValues.extractValue("data_streams.template", dataStreams));
        assertEquals(Collections.singletonList(1), XContentMapValues.extractValue("data_streams.generation", dataStreams));
        assertEquals(Collections.singletonList(true), XContentMapValues.extractValue("data_streams.hidden", dataStreams));

        Request searchRequest = new Request("GET", "/hidd*/_search");
        response = client().performRequest(searchRequest);
        Map<String, Object> results = entityAsMap(response);
        assertEquals(0, XContentMapValues.extractValue("hits.total.value", results));

        searchRequest = new Request("GET", "/hidd*/_search?expand_wildcards=open,hidden");
        response = client().performRequest(searchRequest);
        results = entityAsMap(response);
        assertEquals(1, XContentMapValues.extractValue("hits.total.value", results));
    }

    public void testHiddenDataStreamImplicitHiddenSearch() throws IOException {
        // Create a template
        Request putComposableIndexTemplateRequest = new Request("POST", "/_index_template/hidden");
        putComposableIndexTemplateRequest.setJsonEntity("{\"index_patterns\": [\".hidden\"], \"data_stream\": {\"hidden\": true}}");
        assertOK(client().performRequest(putComposableIndexTemplateRequest));

        Request createDocRequest = new Request("POST", "/.hidden/_doc?refresh=true");
        createDocRequest.setJsonEntity("{ \"@timestamp\": \"2020-10-22\", \"a\": 1 }");

        assertOK(client().performRequest(createDocRequest));

        Request getDataStreamsRequest = new Request("GET", "/_data_stream?expand_wildcards=hidden");
        Response response = client().performRequest(getDataStreamsRequest);
        Map<String, Object> dataStreams = entityAsMap(response);
        assertEquals(Collections.singletonList(".hidden"), XContentMapValues.extractValue("data_streams.name", dataStreams));
        assertEquals(Collections.singletonList("hidden"), XContentMapValues.extractValue("data_streams.template", dataStreams));
        assertEquals(Collections.singletonList(1), XContentMapValues.extractValue("data_streams.generation", dataStreams));
        assertEquals(Collections.singletonList(true), XContentMapValues.extractValue("data_streams.hidden", dataStreams));

        Request searchRequest = new Request("GET", "/.hidd*/_search");
        response = client().performRequest(searchRequest);
        Map<String, Object> results = entityAsMap(response);
        assertEquals(1, XContentMapValues.extractValue("hits.total.value", results));
    }

    public void testAddingIndexTemplateWithAliasesAndDataStream() {
        Request putComposableIndexTemplateRequest = new Request("PUT", "/_index_template/my-template");
        String body = "{\"index_patterns\":[\"mypattern*\"],\"data_stream\":{},\"template\":{\"aliases\":{\"my-alias\":{}}}}";
        putComposableIndexTemplateRequest.setJsonEntity(body);
        Exception e = expectThrows(ResponseException.class, () -> client().performRequest(putComposableIndexTemplateRequest));
        assertThat(e.getMessage(), containsString("template [my-template] has alias and data stream definitions"));
    }
}

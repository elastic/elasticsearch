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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

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

    public void testDataStreamAliases() throws Exception {
        // Create a template
        Request putComposableIndexTemplateRequest = new Request("POST", "/_index_template/1");
        putComposableIndexTemplateRequest.setJsonEntity("{\"index_patterns\": [\"logs-*\"], \"data_stream\": {}}");
        assertOK(client().performRequest(putComposableIndexTemplateRequest));

        Request createDocRequest = new Request("POST", "/logs-myapp1/_doc?refresh=true");
        createDocRequest.setJsonEntity("{ \"@timestamp\": \"2022-12-12\"}");
        assertOK(client().performRequest(createDocRequest));

        createDocRequest = new Request("POST", "/logs-myapp2/_doc?refresh=true");
        createDocRequest.setJsonEntity("{ \"@timestamp\": \"2022-12-12\"}");
        assertOK(client().performRequest(createDocRequest));

        // Add logs-myapp1 -> logs & logs-myapp2 -> logs
        Request updateAliasesRequest = new Request("POST", "/_aliases");
        updateAliasesRequest.setJsonEntity("{\"actions\":[{\"add\":{\"index\":\"logs-myapp1\",\"alias\":\"logs\"}}," +
            "{\"add\":{\"index\":\"logs-myapp2\",\"alias\":\"logs\"}}]}");
        assertOK(client().performRequest(updateAliasesRequest));

        Request getAliasesRequest = new Request("GET", "/_aliases");
        Map<String, Object> getAliasesResponse = entityAsMap(client().performRequest(getAliasesRequest));
        assertThat(getAliasesResponse.keySet(), containsInAnyOrder("logs-myapp1", "logs-myapp2"));
        assertEquals(Map.of("logs", Map.of()), XContentMapValues.extractValue("logs-myapp1.aliases", getAliasesResponse));
        assertEquals(Map.of("logs", Map.of()), XContentMapValues.extractValue("logs-myapp2.aliases", getAliasesResponse));

        Request searchRequest = new Request("GET", "/logs/_search");
        Map<String, Object> searchResponse = entityAsMap(client().performRequest(searchRequest));
        assertEquals(2, XContentMapValues.extractValue("hits.total.value", searchResponse));

        // Remove logs-myapp1 -> logs & logs-myapp2 -> logs
        updateAliasesRequest = new Request("POST", "/_aliases");
        updateAliasesRequest.setJsonEntity("{\"actions\":[{\"remove\":{\"index\":\"logs-myapp1\",\"alias\":\"logs\"}}," +
            "{\"remove\":{\"index\":\"logs-myapp2\",\"alias\":\"logs\"}}]}");
        assertOK(client().performRequest(updateAliasesRequest));

        getAliasesRequest = new Request("GET", "/_aliases");
        getAliasesResponse = entityAsMap(client().performRequest(getAliasesRequest));
        assertEquals(Map.of(), getAliasesResponse);
        expectThrows(ResponseException.class, () -> client().performRequest(new Request("GET", "/logs/_search")));

        // Add logs-* -> logs
        updateAliasesRequest = new Request("POST", "/_aliases");
        updateAliasesRequest.setJsonEntity("{\"actions\":[{\"add\":{\"index\":\"logs-*\",\"alias\":\"logs\"}}]}");
        assertOK(client().performRequest(updateAliasesRequest));

        getAliasesRequest = new Request("GET", "/_aliases");
        getAliasesResponse = entityAsMap(client().performRequest(getAliasesRequest));
        assertEquals(Map.of("logs", Map.of()), XContentMapValues.extractValue("logs-myapp1.aliases", getAliasesResponse));
        assertEquals(Map.of("logs", Map.of()), XContentMapValues.extractValue("logs-myapp2.aliases", getAliasesResponse));

        searchRequest = new Request("GET", "/logs/_search");
        searchResponse = entityAsMap(client().performRequest(searchRequest));
        assertEquals(2, XContentMapValues.extractValue("hits.total.value", searchResponse));

        // Remove logs-* -> logs
        updateAliasesRequest = new Request("POST", "/_aliases");
        updateAliasesRequest.setJsonEntity("{\"actions\":[{\"remove\":{\"index\":\"logs-*\",\"alias\":\"logs\"}}]}");
        assertOK(client().performRequest(updateAliasesRequest));

        getAliasesRequest = new Request("GET", "/_aliases");
        getAliasesResponse = entityAsMap(client().performRequest(getAliasesRequest));
        assertEquals(Map.of(), getAliasesResponse);
        expectThrows(ResponseException.class, () -> client().performRequest(new Request("GET", "/logs/_search")));
    }

    public void testDeleteDataStreamApiWithAliasFails() throws IOException {
        // Create a template
        Request putComposableIndexTemplateRequest = new Request("POST", "/_index_template/1");
        putComposableIndexTemplateRequest.setJsonEntity("{\"index_patterns\": [\"logs-*\"], \"data_stream\": {}}");
        assertOK(client().performRequest(putComposableIndexTemplateRequest));

        Request createDocRequest = new Request("POST", "/logs-emea/_doc?refresh=true");
        createDocRequest.setJsonEntity("{ \"@timestamp\": \"2022-12-12\"}");
        assertOK(client().performRequest(createDocRequest));

        createDocRequest = new Request("POST", "/logs-nasa/_doc?refresh=true");
        createDocRequest.setJsonEntity("{ \"@timestamp\": \"2022-12-12\"}");
        assertOK(client().performRequest(createDocRequest));

        Request updateAliasesRequest = new Request("POST", "/_aliases");
        updateAliasesRequest.setJsonEntity(
            "{\"actions\":[{\"add\":{\"index\":\"logs-emea\",\"alias\":\"logs\"}}," +
                "{\"add\":{\"index\":\"logs-nasa\",\"alias\":\"logs\"}}]}");
        assertOK(client().performRequest(updateAliasesRequest));

        Request getAliasesRequest = new Request("GET", "/logs-*/_alias");
        Map<String, Object> getAliasesResponse = entityAsMap(client().performRequest(getAliasesRequest));
        assertEquals(Map.of("logs", Map.of()), XContentMapValues.extractValue("logs-emea.aliases", getAliasesResponse));
        assertEquals(Map.of("logs", Map.of()), XContentMapValues.extractValue("logs-nasa.aliases", getAliasesResponse));

        Exception e = expectThrows(ResponseException.class, () -> client().performRequest(new Request("DELETE", "/_data_stream/logs")));
        assertThat(e.getMessage(), containsString("The provided expression [logs] matches an alias, " +
            "specify the corresponding concrete indices instead"));

        assertOK(client().performRequest(new Request("DELETE", "/_data_stream/logs-emea")));
        assertOK(client().performRequest(new Request("DELETE", "/_data_stream/logs-nasa")));

        getAliasesRequest = new Request("GET", "/logs-*/_alias");
        assertThat(entityAsMap(client().performRequest(getAliasesRequest)), equalTo(Map.of()));
    }

    public void testGetAliasApiFilterByDataStreamAlias() throws Exception {
        Request putComposableIndexTemplateRequest = new Request("POST", "/_index_template/1");
        putComposableIndexTemplateRequest.setJsonEntity("{\"index_patterns\": [\"logs-*\"], \"data_stream\": {}}");
        assertOK(client().performRequest(putComposableIndexTemplateRequest));

        Request createDocRequest = new Request("POST", "/logs-emea/_doc?refresh=true");
        createDocRequest.setJsonEntity("{ \"@timestamp\": \"2022-12-12\"}");
        assertOK(client().performRequest(createDocRequest));

        createDocRequest = new Request("POST", "/logs-nasa/_doc?refresh=true");
        createDocRequest.setJsonEntity("{ \"@timestamp\": \"2022-12-12\"}");
        assertOK(client().performRequest(createDocRequest));

        Request updateAliasesRequest = new Request("POST", "/_aliases");
        updateAliasesRequest.setJsonEntity(
            "{\"actions\":[{\"add\":{\"index\":\"logs-emea\",\"alias\":\"emea\"}}," +
                "{\"add\":{\"index\":\"logs-nasa\",\"alias\":\"nasa\"}}]}");
        assertOK(client().performRequest(updateAliasesRequest));

        Response response = client().performRequest(new Request("GET", "/_alias"));
        assertOK(response);
        Map<String, Object> getAliasesResponse = entityAsMap(response);
        assertThat(getAliasesResponse.size(), equalTo(2));
        assertEquals(Map.of("emea", Map.of()), XContentMapValues.extractValue("logs-emea.aliases", getAliasesResponse));
        assertEquals(Map.of("nasa", Map.of()), XContentMapValues.extractValue("logs-nasa.aliases", getAliasesResponse));

        response = client().performRequest(new Request("GET", "/_alias/emea"));
        assertOK(response);
        getAliasesResponse = entityAsMap(response);
        assertThat(getAliasesResponse.size(), equalTo(1));
        assertEquals(Map.of("emea", Map.of()), XContentMapValues.extractValue("logs-emea.aliases", getAliasesResponse));

        ResponseException exception =
            expectThrows(ResponseException.class, () -> client().performRequest(new Request("GET", "/_alias/wrong_name")));
        response = exception.getResponse();
        assertThat(response.getStatusLine().getStatusCode(), equalTo(404));
        getAliasesResponse = entityAsMap(response);
        assertThat(getAliasesResponse.size(), equalTo(2));
        assertEquals("alias [wrong_name] missing", getAliasesResponse.get("error"));
        assertEquals(404, getAliasesResponse.get("status"));
    }

    public void testDataStreamWithAliasFromTemplate() throws IOException {
        // Create a template
        Request putComposableIndexTemplateRequest = new Request("POST", "/_index_template/1");
        putComposableIndexTemplateRequest.setJsonEntity(
            "{\"index_patterns\": [\"logs-*\"], \"template\": { \"aliases\": { \"logs\": {} } }, \"data_stream\": {}}");
        assertOK(client().performRequest(putComposableIndexTemplateRequest));

        Request createDocRequest = new Request("POST", "/logs-emea/_doc?refresh=true");
        createDocRequest.setJsonEntity("{ \"@timestamp\": \"2022-12-12\"}");
        assertOK(client().performRequest(createDocRequest));

        createDocRequest = new Request("POST", "/logs-nasa/_doc?refresh=true");
        createDocRequest.setJsonEntity("{ \"@timestamp\": \"2022-12-12\"}");
        assertOK(client().performRequest(createDocRequest));

        Request getAliasesRequest = new Request("GET", "/_aliases");
        Map<String, Object> getAliasesResponse = entityAsMap(client().performRequest(getAliasesRequest));
        assertThat(getAliasesResponse.size(), is(2));
        assertEquals(Map.of("logs", Map.of()), XContentMapValues.extractValue("logs-emea.aliases", getAliasesResponse));
        assertEquals(Map.of("logs", Map.of()), XContentMapValues.extractValue("logs-nasa.aliases", getAliasesResponse));

        Request searchRequest = new Request("GET", "/logs/_search");
        Map<String, Object> searchResponse = entityAsMap(client().performRequest(searchRequest));
        assertEquals(2, XContentMapValues.extractValue("hits.total.value", searchResponse));
    }

    public void testDataStreamWriteAlias() throws IOException {
        // Create a template
        Request putComposableIndexTemplateRequest = new Request("POST", "/_index_template/1");
        putComposableIndexTemplateRequest.setJsonEntity("{\"index_patterns\": [\"logs-*\"], \"data_stream\": {}}");
        assertOK(client().performRequest(putComposableIndexTemplateRequest));

        Request createDocRequest = new Request("POST", "/logs-emea/_doc?refresh=true");
        createDocRequest.setJsonEntity("{ \"@timestamp\": \"2022-12-12\"}");
        assertOK(client().performRequest(createDocRequest));

        createDocRequest = new Request("POST", "/logs-nasa/_doc?refresh=true");
        createDocRequest.setJsonEntity("{ \"@timestamp\": \"2022-12-12\"}");
        assertOK(client().performRequest(createDocRequest));

        Request updateAliasesRequest = new Request("POST", "/_aliases");
        updateAliasesRequest.setJsonEntity(
            "{\"actions\":[{\"add\":{\"index\":\"logs-emea\",\"alias\":\"logs\",\"is_write_index\":true}}," +
                "{\"add\":{\"index\":\"logs-nasa\",\"alias\":\"logs\"}}]}");
        assertOK(client().performRequest(updateAliasesRequest));

        Request getAliasesRequest = new Request("GET", "/_aliases");
        Map<String, Object> getAliasesResponse = entityAsMap(client().performRequest(getAliasesRequest));
        assertEquals(
            Map.of("logs", Map.of("is_write_index", true)),
            XContentMapValues.extractValue("logs-emea.aliases", getAliasesResponse)
        );
        assertEquals(Map.of("logs", Map.of()), XContentMapValues.extractValue("logs-nasa.aliases", getAliasesResponse));

        Request searchRequest = new Request("GET", "/logs/_search");
        Map<String, Object> searchResponse = entityAsMap(client().performRequest(searchRequest));
        assertEquals(2, XContentMapValues.extractValue("hits.total.value", searchResponse));

        createDocRequest = new Request("POST", "/logs/_doc?refresh=true");
        createDocRequest.setJsonEntity("{ \"@timestamp\": \"2022-12-12\"}");
        Response createDocResponse = client().performRequest(createDocRequest);
        assertOK(createDocResponse);
        assertThat((String) entityAsMap(createDocResponse).get("_index"), startsWith(".ds-logs-emea"));

        updateAliasesRequest = new Request("POST", "/_aliases");
        updateAliasesRequest.setJsonEntity("{\"actions\":[" +
            "{\"add\":{\"index\":\"logs-emea\",\"alias\":\"logs\",\"is_write_index\":false}}," +
            "{\"add\":{\"index\":\"logs-nasa\",\"alias\":\"logs\",\"is_write_index\":true}}" +
            "]}");
        assertOK(client().performRequest(updateAliasesRequest));

        createDocRequest = new Request("POST", "/logs/_doc?refresh=true");
        createDocRequest.setJsonEntity("{ \"@timestamp\": \"2022-12-12\"}");
        createDocResponse = client().performRequest(createDocRequest);
        assertOK(createDocResponse);
        assertThat((String) entityAsMap(createDocResponse).get("_index"), startsWith(".ds-logs-nasa"));

        getAliasesRequest = new Request("GET", "/_aliases");
        getAliasesResponse = entityAsMap(client().performRequest(getAliasesRequest));
        assertEquals(Map.of("logs", Map.of()), XContentMapValues.extractValue("logs-emea.aliases", getAliasesResponse));
        assertEquals(
            Map.of("logs", Map.of("is_write_index", true)),
            XContentMapValues.extractValue("logs-nasa.aliases", getAliasesResponse)
        );
    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * This should be a yaml test, but in order to write one we would need to expose the new parameter in the rest-api-spec.
 * We do not want to do that until the feature flag is removed. For this reason, we temporarily, test the affected APIs here.
 * Please convert this to a yaml test when the feature flag is removed.
 */
public class FailureStoreQueryParamIT extends DisabledSecurityDataStreamTestCase {

    private static final String DATA_STREAM_NAME = "failure-data-stream";
    private String backingIndex;
    private String failureStoreIndex;

    @SuppressWarnings("unchecked")
    @Before
    public void setup() throws IOException {
        Request putComposableIndexTemplateRequest = new Request("POST", "/_index_template/ds-template");
        putComposableIndexTemplateRequest.setJsonEntity("""
            {
              "index_patterns": ["failure-data-stream"],
              "template": {
                "settings": {
                  "number_of_replicas": 0
                }
              },
              "data_stream": {
                "failure_store": true
              }
            }
            """);
        assertOK(client().performRequest(putComposableIndexTemplateRequest));

        assertOK(client().performRequest(new Request("PUT", "/_data_stream/" + DATA_STREAM_NAME)));
        ensureGreen(DATA_STREAM_NAME);

        final Response dataStreamResponse = client().performRequest(new Request("GET", "/_data_stream/" + DATA_STREAM_NAME));
        List<Object> dataStreams = (List<Object>) entityAsMap(dataStreamResponse).get("data_streams");
        assertThat(dataStreams.size(), is(1));
        Map<String, Object> dataStream = (Map<String, Object>) dataStreams.get(0);
        assertThat(dataStream.get("name"), equalTo(DATA_STREAM_NAME));
        List<String> backingIndices = getBackingIndices(dataStream);
        assertThat(backingIndices.size(), is(1));
        List<String> failureStore = getFailureStore(dataStream);
        assertThat(failureStore.size(), is(1));
        backingIndex = backingIndices.get(0);
        failureStoreIndex = failureStore.get(0);
    }

    public void testGetIndexApi() throws IOException {
        {
            final Response indicesResponse = client().performRequest(new Request("GET", "/" + DATA_STREAM_NAME));
            Map<String, Object> indices = entityAsMap(indicesResponse);
            assertThat(indices.size(), is(2));
            assertThat(indices.containsKey(backingIndex), is(true));
            assertThat(indices.containsKey(failureStoreIndex), is(true));
        }
        {
            final Response indicesResponse = client().performRequest(new Request("GET", "/" + DATA_STREAM_NAME + "?failure_store=false"));
            Map<String, Object> indices = entityAsMap(indicesResponse);
            assertThat(indices.size(), is(1));
            assertThat(indices.containsKey(backingIndex), is(true));
        }
        {
            final Response indicesResponse = client().performRequest(new Request("GET", "/" + DATA_STREAM_NAME + "?failure_store=only"));
            Map<String, Object> indices = entityAsMap(indicesResponse);
            assertThat(indices.size(), is(1));
            assertThat(indices.containsKey(failureStoreIndex), is(true));
        }
    }

    @SuppressWarnings("unchecked")
    public void testGetIndexStatsApi() throws IOException {
        {
            final Response statsResponse = client().performRequest(new Request("GET", "/" + DATA_STREAM_NAME + "/_stats"));
            Map<String, Object> indices = (Map<String, Object>) entityAsMap(statsResponse).get("indices");
            assertThat(indices.size(), is(1));
            assertThat(indices.containsKey(backingIndex), is(true));
        }
        {
            final Response statsResponse = client().performRequest(
                new Request("GET", "/" + DATA_STREAM_NAME + "/_stats?failure_store=true")
            );
            Map<String, Object> indices = (Map<String, Object>) entityAsMap(statsResponse).get("indices");
            assertThat(indices.size(), is(2));
            assertThat(indices.containsKey(backingIndex), is(true));
            assertThat(indices.containsKey(failureStoreIndex), is(true));
        }
        {
            final Response statsResponse = client().performRequest(
                new Request("GET", "/" + DATA_STREAM_NAME + "/_stats?failure_store=only")
            );
            Map<String, Object> indices = (Map<String, Object>) entityAsMap(statsResponse).get("indices");
            assertThat(indices.size(), is(1));
            assertThat(indices.containsKey(failureStoreIndex), is(true));
        }
    }

    public void testGetIndexSettingsApi() throws IOException {
        {
            final Response indicesResponse = client().performRequest(new Request("GET", "/" + DATA_STREAM_NAME + "/_settings"));
            Map<String, Object> indices = entityAsMap(indicesResponse);
            assertThat(indices.size(), is(1));
            assertThat(indices.containsKey(backingIndex), is(true));
        }
        {
            final Response indicesResponse = client().performRequest(
                new Request("GET", "/" + DATA_STREAM_NAME + "/_settings?failure_store=true")
            );
            Map<String, Object> indices = entityAsMap(indicesResponse);
            assertThat(indices.size(), is(2));
            assertThat(indices.containsKey(backingIndex), is(true));
            assertThat(indices.containsKey(failureStoreIndex), is(true));
        }
        {
            final Response indicesResponse = client().performRequest(
                new Request("GET", "/" + DATA_STREAM_NAME + "/_settings?failure_store=only")
            );
            Map<String, Object> indices = entityAsMap(indicesResponse);
            assertThat(indices.size(), is(1));
            assertThat(indices.containsKey(failureStoreIndex), is(true));
        }
    }

    public void testGetIndexMappingApi() throws IOException {
        {
            final Response indicesResponse = client().performRequest(new Request("GET", "/" + DATA_STREAM_NAME + "/_mapping"));
            Map<String, Object> indices = entityAsMap(indicesResponse);
            assertThat(indices.size(), is(1));
            assertThat(indices.containsKey(backingIndex), is(true));
        }
        {
            final Response indicesResponse = client().performRequest(
                new Request("GET", "/" + DATA_STREAM_NAME + "/_mapping?failure_store=true")
            );
            Map<String, Object> indices = entityAsMap(indicesResponse);
            assertThat(indices.size(), is(2));
            assertThat(indices.containsKey(backingIndex), is(true));
            assertThat(indices.containsKey(failureStoreIndex), is(true));
        }
        {
            final Response indicesResponse = client().performRequest(
                new Request("GET", "/" + DATA_STREAM_NAME + "/_mapping?failure_store=only")
            );
            Map<String, Object> indices = entityAsMap(indicesResponse);
            assertThat(indices.size(), is(1));
            assertThat(indices.containsKey(failureStoreIndex), is(true));
        }
    }

    @SuppressWarnings("unchecked")
    public void testPutIndexMappingApi() throws IOException {
        {
            final Request mappingRequest = new Request("PUT", "/" + DATA_STREAM_NAME + "/_mapping");
            mappingRequest.setJsonEntity("""
                {
                  "properties": {
                    "email": {
                      "type": "keyword"
                    }
                  }
                }
                """);
            assertAcknowledged(client().performRequest(mappingRequest));
        }
        {
            final Request mappingRequest = new Request("PUT", "/" + DATA_STREAM_NAME + "/_mapping?failure_store=true");
            mappingRequest.setJsonEntity("""
                {
                  "properties": {
                    "email": {
                      "type": "keyword"
                    }
                  }
                }
                """);
            ResponseException responseException = expectThrows(ResponseException.class, () -> client().performRequest(mappingRequest));
            Map<String, Object> response = entityAsMap(responseException.getResponse());
            assertThat(((Map<String, Object>) response.get("error")).get("reason"), is("failure index not supported"));
        }
    }

    private List<String> getBackingIndices(Map<String, Object> response) {
        return getIndices(response, "indices");
    }

    private List<String> getFailureStore(Map<String, Object> response) {
        return getIndices(response, "failure_indices");

    }

    @SuppressWarnings("unchecked")
    private List<String> getIndices(Map<String, Object> response, String fieldName) {
        List<Map<String, String>> indices = (List<Map<String, String>>) response.get(fieldName);
        return indices.stream().map(index -> index.get("index_name")).toList();
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * This should be a yaml test, but in order to write one we would need to expose the new APIs in the rest-api-spec.
 * We do not want to do that until the feature flag is removed. For this reason, we temporarily, test the new APIs here.
 * Please convert this to a yaml test when the feature flag is removed.
 */
public class DataStreamOptionsIT extends DisabledSecurityDataStreamTestCase {

    private static final String DATA_STREAM_NAME = "failure-data-stream";

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
        // Initialize the failure store.
        assertOK(client().performRequest(new Request("POST", DATA_STREAM_NAME + "/_rollover?target_failure_store")));
        ensureGreen(DATA_STREAM_NAME);

        final Response dataStreamResponse = client().performRequest(new Request("GET", "/_data_stream/" + DATA_STREAM_NAME));
        List<Object> dataStreams = (List<Object>) entityAsMap(dataStreamResponse).get("data_streams");
        assertThat(dataStreams.size(), is(1));
        Map<String, Object> dataStream = (Map<String, Object>) dataStreams.get(0);
        assertThat(dataStream.get("name"), equalTo(DATA_STREAM_NAME));
        List<String> backingIndices = getIndices(dataStream);
        assertThat(backingIndices.size(), is(1));
        List<String> failureStore = getFailureStore(dataStream);
        assertThat(failureStore.size(), is(1));
    }

    public void testEnableDisableFailureStore() throws IOException {
        {
            assertAcknowledged(client().performRequest(new Request("DELETE", "/_data_stream/" + DATA_STREAM_NAME + "/_options")));
            assertFailureStore(false, 1);
            assertDataStreamOptions(null);
        }
        {
            Request enableRequest = new Request("PUT", "/_data_stream/" + DATA_STREAM_NAME + "/_options");
            enableRequest.setJsonEntity("""
                {
                  "failure_store": {
                    "enabled": true
                  }
                }""");
            assertAcknowledged(client().performRequest(enableRequest));
            assertFailureStore(true, 1);
            assertDataStreamOptions(true);
        }

        {
            Request disableRequest = new Request("PUT", "/_data_stream/" + DATA_STREAM_NAME + "/_options");
            disableRequest.setJsonEntity("""
                {
                  "failure_store": {
                    "enabled": false
                  }
                }""");
            assertAcknowledged(client().performRequest(disableRequest));
            assertFailureStore(false, 1);
            assertDataStreamOptions(false);
        }
    }

    @SuppressWarnings("unchecked")
    private void assertFailureStore(boolean failureStoreEnabled, int failureStoreSize) throws IOException {
        final Response dataStreamResponse = client().performRequest(new Request("GET", "/_data_stream/" + DATA_STREAM_NAME));
        List<Object> dataStreams = (List<Object>) entityAsMap(dataStreamResponse).get("data_streams");
        assertThat(dataStreams.size(), is(1));
        Map<String, Object> dataStream = (Map<String, Object>) dataStreams.get(0);
        assertThat(dataStream.get("name"), equalTo(DATA_STREAM_NAME));
        assertThat(dataStream.containsKey("failure_store"), is(true));
        // Ensure the failure store is set to the provided value
        assertThat(((Map<String, Object>) dataStream.get("failure_store")).get("enabled"), equalTo(failureStoreEnabled));
        // And the failure indices preserved
        List<String> failureStore = getFailureStore(dataStream);
        assertThat(failureStore.size(), is(failureStoreSize));
    }

    @SuppressWarnings("unchecked")
    private void assertDataStreamOptions(Boolean failureStoreEnabled) throws IOException {
        final Response dataStreamResponse = client().performRequest(new Request("GET", "/_data_stream/" + DATA_STREAM_NAME + "/_options"));
        List<Object> dataStreams = (List<Object>) entityAsMap(dataStreamResponse).get("data_streams");
        assertThat(dataStreams.size(), is(1));
        Map<String, Object> dataStream = (Map<String, Object>) dataStreams.get(0);
        assertThat(dataStream.get("name"), equalTo(DATA_STREAM_NAME));
        Map<String, Map<String, Object>> options = (Map<String, Map<String, Object>>) dataStream.get("options");
        if (failureStoreEnabled == null) {
            assertThat(options, nullValue());
        } else {
            assertThat(options.containsKey("failure_store"), is(true));
            assertThat(options.get("failure_store").get("enabled"), equalTo(failureStoreEnabled));
        }
    }

    @SuppressWarnings("unchecked")
    private List<String> getFailureStore(Map<String, Object> response) {
        var failureStore = (Map<String, Object>) response.get("failure_store");
        return getIndices(failureStore);

    }

    @SuppressWarnings("unchecked")
    private List<String> getIndices(Map<String, Object> response) {
        List<Map<String, String>> indices = (List<Map<String, String>>) response.get("indices");
        return indices.stream().map(index -> index.get("index_name")).toList();
    }
}

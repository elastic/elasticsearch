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
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.metadata.DataStreamFailureStoreSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
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
                },
                "data_stream_options": {
                  "failure_store": {
                      "enabled": true
                  }
                }
              },
              "data_stream": {
              }
            }
            """);
        assertOK(client().performRequest(putComposableIndexTemplateRequest));

        assertOK(client().performRequest(new Request("PUT", "/_data_stream/" + DATA_STREAM_NAME)));
        // Initialize the failure store.
        assertOK(client().performRequest(new Request("POST", DATA_STREAM_NAME + "::failures/_rollover")));
        ensureGreen(DATA_STREAM_NAME);

        final Response dataStreamResponse = client().performRequest(new Request("GET", "/_data_stream/" + DATA_STREAM_NAME));
        List<Object> dataStreams = (List<Object>) entityAsMap(dataStreamResponse).get("data_streams");
        assertThat(dataStreams.size(), is(1));
        Map<String, Object> dataStream = (Map<String, Object>) dataStreams.get(0);
        assertThat(dataStream.get("name"), equalTo(DATA_STREAM_NAME));
        assertThat(((Map<String, Object>) dataStream.get("failure_store")).get("enabled"), is(true));
        List<String> backingIndices = getIndices(dataStream);
        assertThat(backingIndices.size(), is(1));
        List<String> failureStore = getFailureStore(dataStream);
        assertThat(failureStore.size(), is(1));
    }

    public void testExplicitlyResetDataStreamOptions() throws IOException {
        Request putComponentTemplateRequest = new Request("POST", "/_component_template/with-options");
        putComponentTemplateRequest.setJsonEntity("""
            {
              "template": {
                "data_stream_options": {
                  "failure_store": {
                      "enabled": true
                  }
                }
              }
            }
            """);
        assertOK(client().performRequest(putComponentTemplateRequest));

        Request invalidRequest = new Request("POST", "/_index_template/other-template");
        invalidRequest.setJsonEntity("""
            {
              "index_patterns": ["something-else"],
              "composed_of" : ["with-options"],
              "template": {
                "settings": {
                  "number_of_replicas": 0
                }
              }
            }
            """);
        Exception error = expectThrows(ResponseException.class, () -> client().performRequest(invalidRequest));
        assertThat(
            error.getMessage(),
            containsString("specifies data stream options that can only be used in combination with a data stream")
        );

        // Check that when we nullify the data stream options we can create use any component template in a non data stream template
        Request otherRequest = new Request("POST", "/_index_template/other-template");
        otherRequest.setJsonEntity("""
            {
              "index_patterns": ["something-else"],
              "composed_of" : ["with-options"],
              "template": {
                "settings": {
                  "number_of_replicas": 0
                },
                "data_stream_options": null
              }
            }
            """);
        assertOK(client().performRequest(otherRequest));
    }

    public void testBehaviorWithEachFailureStoreOptionAndClusterSetting() throws IOException {
        {
            // Default data stream options
            assertAcknowledged(client().performRequest(new Request("DELETE", "/_data_stream/" + DATA_STREAM_NAME + "/_options")));
            setDataStreamFailureStoreClusterSetting(DATA_STREAM_NAME);
            assertDataStreamOptions(null);
            assertFailureStoreValuesInGetDataStreamResponse(true, 1);
            assertRedirectsDocWithBadMappingToFailureStore();
            setDataStreamFailureStoreClusterSetting("does-not-match-failure-data-stream");
            assertDataStreamOptions(null);
            assertFailureStoreValuesInGetDataStreamResponse(false, 1);
            assertFailsDocWithBadMapping();
            setDataStreamFailureStoreClusterSetting(null); // should get same behaviour as when we set it to something non-matching
            assertDataStreamOptions(null);
            assertFailureStoreValuesInGetDataStreamResponse(false, 1);
            assertFailsDocWithBadMapping();
        }
        {
            // Data stream options with failure store enabled
            Request enableRequest = new Request("PUT", "/_data_stream/" + DATA_STREAM_NAME + "/_options");
            enableRequest.setJsonEntity("""
                {
                  "failure_store": {
                    "enabled": true
                  }
                }""");
            assertAcknowledged(client().performRequest(enableRequest));
            setDataStreamFailureStoreClusterSetting(DATA_STREAM_NAME);
            assertDataStreamOptions(true);
            assertFailureStoreValuesInGetDataStreamResponse(true, 1);
            assertRedirectsDocWithBadMappingToFailureStore();
            setDataStreamFailureStoreClusterSetting("does-not-match-failure-data-stream"); // should have no effect as enabled in options
            assertDataStreamOptions(true);
            assertFailureStoreValuesInGetDataStreamResponse(true, 1);
            assertRedirectsDocWithBadMappingToFailureStore();
            setDataStreamFailureStoreClusterSetting(null); // same as previous
            assertDataStreamOptions(true);
            assertFailureStoreValuesInGetDataStreamResponse(true, 1);
            assertRedirectsDocWithBadMappingToFailureStore();
        }
        {
            // Data stream options with failure store disabled
            Request disableRequest = new Request("PUT", "/_data_stream/" + DATA_STREAM_NAME + "/_options");
            disableRequest.setJsonEntity("""
                {
                  "failure_store": {
                    "enabled": false
                  }
                }""");
            assertAcknowledged(client().performRequest(disableRequest));
            setDataStreamFailureStoreClusterSetting(DATA_STREAM_NAME); // should have no effect as disabled in options
            assertDataStreamOptions(false);
            assertFailureStoreValuesInGetDataStreamResponse(false, 1);
            assertFailsDocWithBadMapping();
            setDataStreamFailureStoreClusterSetting("does-not-match-failure-data-stream");
            assertDataStreamOptions(false);
            assertFailureStoreValuesInGetDataStreamResponse(false, 1);
            assertFailsDocWithBadMapping();
            setDataStreamFailureStoreClusterSetting(null);
            assertDataStreamOptions(false);
            assertFailureStoreValuesInGetDataStreamResponse(false, 1);
            assertFailsDocWithBadMapping();
        }
    }

    @SuppressWarnings("unchecked")
    private void assertFailureStoreValuesInGetDataStreamResponse(boolean failureStoreEnabled, int failureStoreSize) throws IOException {
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

    private static void setDataStreamFailureStoreClusterSetting(String value) throws IOException {
        updateClusterSettings(
            Settings.builder().put(DataStreamFailureStoreSettings.DATA_STREAM_FAILURE_STORED_ENABLED_SETTING.getKey(), value).build()
        );
    }

    private Response putDocumentWithBadMapping() throws IOException {
        Request request = new Request("POST", DATA_STREAM_NAME + "/_doc");
        request.setJsonEntity("""
            {
              "@timestamp": "not a timestamp",
              "foo": "bar"
            }
            """);
        return client().performRequest(request);
    }

    private void assertRedirectsDocWithBadMappingToFailureStore() throws IOException {
        Response response = putDocumentWithBadMapping();
        String failureStoreResponse = (String) entityAsMap(response).get("failure_store");
        assertThat(failureStoreResponse, is("used"));
    }

    private void assertFailsDocWithBadMapping() {
        ResponseException e = assertThrows(ResponseException.class, this::putDocumentWithBadMapping);
        assertThat(e.getResponse().getStatusLine().getStatusCode(), is(RestStatus.BAD_REQUEST.getStatus()));
    }
}

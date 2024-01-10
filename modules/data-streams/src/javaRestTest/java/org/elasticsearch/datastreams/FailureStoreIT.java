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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.hamcrest.MatcherAssert;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class FailureStoreIT extends DisabledSecurityDataStreamTestCase {

    /**
     * This should be a yaml tests, but in order to write one we would need to expose in the rest-api-spec the new parameter.
     * We do not want to do that until the feature flag is removed. For this reason, we temporarily, test the API here.
     * Please convert this to a yaml test when the feature flag is removed.
     */
    @SuppressWarnings("unchecked")
    public void testRestApi() throws IOException {
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

        String dataStreamName = "failure-data-stream";
        assertOK(client().performRequest(new Request("PUT", "/_data_stream/" + dataStreamName)));
        ensureGreen(dataStreamName);

        final Response dataStreamResponse = client().performRequest(new Request("GET", "/_data_stream/" + dataStreamName));
        List<Object> dataStreams = (List<Object>) entityAsMap(dataStreamResponse).get("data_streams");
        MatcherAssert.assertThat(dataStreams.size(), is(1));
        Map<String, Object> dataStream = (Map<String, Object>) dataStreams.get(0);
        MatcherAssert.assertThat(dataStream.get("name"), equalTo(dataStreamName));
        List<Map<String, String>> backingIndices = (List<Map<String, String>>) dataStream.get("indices");
        MatcherAssert.assertThat(backingIndices.size(), is(1));
        List<Map<String, String>> failureStore = (List<Map<String, String>>) dataStream.get("failure_indices");
        MatcherAssert.assertThat(failureStore.size(), is(1));
        String backingIndex = backingIndices.get(0).get("index_name");
        String failureStoreIndex = failureStore.get(0).get("index_name");

        {
            final Response indicesResponse = client().performRequest(new Request("GET", "/" + dataStreamName));
            Map<String, Object> indices = entityAsMap(indicesResponse);
            MatcherAssert.assertThat(indices.size(), is(1));
            MatcherAssert.assertThat(indices.containsKey(backingIndex), is(true));
        }
        {
            final Response indicesResponse = client().performRequest(new Request("GET", "/" + dataStreamName + "?failure_store=include"));
            Map<String, Object> indices = entityAsMap(indicesResponse);
            MatcherAssert.assertThat(indices.size(), is(2));
            MatcherAssert.assertThat(indices.containsKey(backingIndex), is(true));
            MatcherAssert.assertThat(indices.containsKey(failureStoreIndex), is(true));
        }
        {
            final Response indicesResponse = client().performRequest(new Request("GET", "/" + dataStreamName + "?failure_store=only"));
            Map<String, Object> indices = entityAsMap(indicesResponse);
            MatcherAssert.assertThat(indices.size(), is(1));
            MatcherAssert.assertThat(indices.containsKey(failureStoreIndex), is(true));
        }
    }
}

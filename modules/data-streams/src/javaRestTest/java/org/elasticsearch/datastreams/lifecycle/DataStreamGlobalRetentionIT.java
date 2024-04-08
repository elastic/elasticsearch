/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.datastreams.lifecycle;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datastreams.DisabledSecurityDataStreamTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class DataStreamGlobalRetentionIT extends DisabledSecurityDataStreamTestCase {

    @Before
    public void setup() throws IOException {
        updateClusterSettings(
            Settings.builder()
                .put("data_streams.lifecycle.poll_interval", "1s")
                .put("cluster.lifecycle.default.rollover", "min_docs=1,max_docs=1")
                .build()
        );
        // Create a template with the default lifecycle
        Request putComposableIndexTemplateRequest = new Request("POST", "/_index_template/1");
        putComposableIndexTemplateRequest.setJsonEntity("""
            {
              "index_patterns": ["my-data-stream*"],
              "data_stream": {},
              "template": {
                "lifecycle": {}
              }
            }
            """);
        assertOK(client().performRequest(putComposableIndexTemplateRequest));

        // Create a data streams with one doc
        Request createDocRequest = new Request("POST", "/my-data-stream/_doc?refresh=true");
        createDocRequest.setJsonEntity("{ \"@timestamp\": \"2022-12-12\"}");
        assertOK(client().performRequest(createDocRequest));
    }

    @After
    public void cleanUp() throws IOException {
        adminClient().performRequest(new Request("DELETE", "_data_stream/*"));
    }

    @SuppressWarnings("unchecked")
    public void testDefaultRetention() throws Exception {
        {
            // Set global retention
            Request request = new Request("PUT", "_data_stream/_global_retention");
            request.setJsonEntity("""
                {
                  "default_retention": "10s"
                }""");
            assertAcknowledged(client().performRequest(request));
        }

        // Verify that the effective retention matches the default retention
        {
            Request request = new Request("GET", "/_data_stream/my-data-stream");
            Response response = client().performRequest(request);
            List<Object> dataStreams = (List<Object>) entityAsMap(response).get("data_streams");
            assertThat(dataStreams.size(), is(1));
            Map<String, Object> dataStream = (Map<String, Object>) dataStreams.get(0);
            assertThat(dataStream.get("name"), is("my-data-stream"));
            Map<String, Object> lifecycle = (Map<String, Object>) dataStream.get("lifecycle");
            assertThat(lifecycle.get("effective_retention"), is("10s"));
            assertThat(lifecycle.get("retention_determined_by"), is("default_global_retention"));
            assertThat(lifecycle.get("data_retention"), nullValue());
        }

        // Verify that the first generation index was removed
        assertBusy(() -> {
            Response response = client().performRequest(new Request("GET", "/_data_stream/my-data-stream"));
            Map<String, Object> dataStream = ((List<Map<String, Object>>) entityAsMap(response).get("data_streams")).get(0);
            assertThat(dataStream.get("name"), is("my-data-stream"));
            List<Object> backingIndices = (List<Object>) dataStream.get("indices");
            assertThat(backingIndices.size(), is(1));
            // 2 backing indices created + 1 for the deleted index
            assertThat(dataStream.get("generation"), is(3));
        }, 20, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    public void testMaxRetention() throws Exception {
        {
            // Set global retention
            Request request = new Request("PUT", "_data_stream/_global_retention");
            request.setJsonEntity("""
                {
                  "max_retention": "10s"
                }""");
            assertAcknowledged(client().performRequest(request));
        }
        boolean withDataStreamLevelRetention = randomBoolean();
        if (withDataStreamLevelRetention) {
            Request request = new Request("PUT", "_data_stream/my-data-stream/_lifecycle");
            request.setJsonEntity("""
                {
                  "data_retention": "30d"
                }""");
            assertAcknowledged(client().performRequest(request));
        }

        // Verify that the effective retention matches the max retention
        {
            Request request = new Request("GET", "/_data_stream/my-data-stream");
            Response response = client().performRequest(request);
            List<Object> dataStreams = (List<Object>) entityAsMap(response).get("data_streams");
            assertThat(dataStreams.size(), is(1));
            Map<String, Object> dataStream = (Map<String, Object>) dataStreams.get(0);
            assertThat(dataStream.get("name"), is("my-data-stream"));
            Map<String, Object> lifecycle = (Map<String, Object>) dataStream.get("lifecycle");
            assertThat(lifecycle.get("effective_retention"), is("10s"));
            assertThat(lifecycle.get("retention_determined_by"), is("max_global_retention"));
            if (withDataStreamLevelRetention) {
                assertThat(lifecycle.get("data_retention"), is("30d"));
            } else {
                assertThat(lifecycle.get("data_retention"), nullValue());
            }
        }

        // Verify that the first generation index was removed
        assertBusy(() -> {
            Response response = client().performRequest(new Request("GET", "/_data_stream/my-data-stream"));
            Map<String, Object> dataStream = ((List<Map<String, Object>>) entityAsMap(response).get("data_streams")).get(0);
            assertThat(dataStream.get("name"), is("my-data-stream"));
            List<Object> backingIndices = (List<Object>) dataStream.get("indices");
            assertThat(backingIndices.size(), is(1));
            // 2 backing indices created + 1 for the deleted index
            assertThat(dataStream.get("generation"), is(3));
        }, 20, TimeUnit.SECONDS);
    }
}

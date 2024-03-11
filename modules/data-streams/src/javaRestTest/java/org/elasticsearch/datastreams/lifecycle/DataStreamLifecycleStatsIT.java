/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.datastreams.lifecycle;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datastreams.DisabledSecurityDataStreamTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

public class DataStreamLifecycleStatsIT extends DisabledSecurityDataStreamTestCase {

    @Before
    public void updateClusterSettings() throws IOException {
        updateClusterSettings(
            Settings.builder()
                .put("data_streams.lifecycle.poll_interval", "1s")
                .put("cluster.lifecycle.default.rollover", "min_docs=1,max_docs=1")
                .build()
        );
    }

    @After
    public void cleanUp() throws IOException {
        adminClient().performRequest(new Request("DELETE", "_data_stream/*?expand_wildcards=hidden"));
    }

    @SuppressWarnings("unchecked")
    public void testStats() throws Exception {
        // Check empty stats and wait until we have 2 executions
        assertBusy(() -> {
            Request request = new Request("GET", "/_lifecycle/stats");
            Map<String, Object> response = entityAsMap(client().performRequest(request));
            assertThat(response.get("data_stream_count"), is(0));
            assertThat(response.get("data_streams"), is(List.of()));
            assertThat(response.containsKey("last_run_duration_in_millis"), is(true));
            assertThat(response.containsKey("time_between_starts_in_millis"), is(true));
        });

        // Create a template
        Request putComposableIndexTemplateRequest = new Request("POST", "/_index_template/1");
        putComposableIndexTemplateRequest.setJsonEntity("""
            {
              "index_patterns": ["my-data-stream-*"],
              "data_stream": {},
              "template": {
                "lifecycle": {}
              }
            }
            """);
        assertOK(client().performRequest(putComposableIndexTemplateRequest));

        // Create two data streams with one doc each
        Request createDocRequest = new Request("POST", "/my-data-stream-1/_doc?refresh=true");
        createDocRequest.setJsonEntity("{ \"@timestamp\": \"2022-12-12\"}");
        assertOK(client().performRequest(createDocRequest));
        createDocRequest = new Request("POST", "/my-data-stream-2/_doc?refresh=true");
        createDocRequest.setJsonEntity("{ \"@timestamp\": \"2022-12-12\"}");
        assertOK(client().performRequest(createDocRequest));

        Request request = new Request("GET", "/_lifecycle/stats");
        Map<String, Object> response = entityAsMap(client().performRequest(request));
        assertThat(response.get("data_stream_count"), is(2));
        List<Map<String, Object>> dataStreams = (List<Map<String, Object>>) response.get("data_streams");
        assertThat(dataStreams.get(0).get("name"), is("my-data-stream-1"));
        assertThat((Integer) dataStreams.get(0).get("backing_indices_in_total"), greaterThanOrEqualTo(1));
        assertThat((Integer) dataStreams.get(0).get("backing_indices_in_error"), is(0));
        assertThat(dataStreams.get(1).get("name"), is("my-data-stream-2"));
        assertThat((Integer) dataStreams.get(1).get("backing_indices_in_total"), greaterThanOrEqualTo(1));
        assertThat((Integer) dataStreams.get(0).get("backing_indices_in_error"), is(0));
        assertThat(response.containsKey("last_run_duration_in_millis"), is(true));
        assertThat(response.containsKey("time_between_starts_in_millis"), is(true));
    }
}

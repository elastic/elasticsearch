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

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

public class LazyRolloverDataStreamIT extends DisabledSecurityDataStreamTestCase {

    @SuppressWarnings("unchecked")
    public void testLazyRollover() throws Exception {
        Request putComposableIndexTemplateRequest = new Request("POST", "/_index_template/lazy-ds-template");
        putComposableIndexTemplateRequest.setJsonEntity("""
            {
              "index_patterns": ["lazy-ds*"],
              "data_stream": {}
            }
            """);
        assertOK(client().performRequest(putComposableIndexTemplateRequest));

        String dataStreamName = "lazy-ds";

        Request createDocRequest = new Request("POST", "/" + dataStreamName + "/_doc?refresh=true");
        createDocRequest.setJsonEntity("{ \"@timestamp\": \"2020-10-22\", \"a\": 1 }");
        assertOK(client().performRequest(createDocRequest));

        final Response rolloverResponse = client().performRequest(new Request("POST", "/" + dataStreamName + "/_rollover?lazy"));
        Map<String, Object> rolloverResponseMap = entityAsMap(rolloverResponse);
        assertThat((String) rolloverResponseMap.get("old_index"), startsWith(".ds-lazy-ds-"));
        assertThat((String) rolloverResponseMap.get("old_index"), endsWith("-000001"));
        assertThat((String) rolloverResponseMap.get("new_index"), startsWith(".ds-lazy-ds-"));
        assertThat((String) rolloverResponseMap.get("new_index"), endsWith("-000002"));
        assertThat(rolloverResponseMap.get("lazy"), equalTo(true));
        assertThat(rolloverResponseMap.get("dry_run"), equalTo(false));
        assertThat(rolloverResponseMap.get("acknowledged"), equalTo(true));
        assertThat(rolloverResponseMap.get("rolled_over"), equalTo(false));
        assertThat(rolloverResponseMap.get("conditions"), equalTo(Map.of()));

        {
            final Response dataStreamResponse = client().performRequest(new Request("GET", "/_data_stream/" + dataStreamName));
            List<Object> dataStreams = (List<Object>) entityAsMap(dataStreamResponse).get("data_streams");
            assertThat(dataStreams.size(), is(1));
            Map<String, Object> dataStream = (Map<String, Object>) dataStreams.get(0);
            assertThat(dataStream.get("name"), equalTo(dataStreamName));
            assertThat(dataStream.get("rollover_on_write"), is(true));
            assertThat(((List<Object>) dataStream.get("indices")).size(), is(1));
        }

        createDocRequest = new Request("POST", "/" + dataStreamName + "/_doc?refresh=true");
        createDocRequest.setJsonEntity("{ \"@timestamp\": \"2020-10-23\", \"a\": 2 }");
        assertOK(client().performRequest(createDocRequest));

        {
            final Response dataStreamResponse = client().performRequest(new Request("GET", "/_data_stream/" + dataStreamName));
            List<Object> dataStreams = (List<Object>) entityAsMap(dataStreamResponse).get("data_streams");
            assertThat(dataStreams.size(), is(1));
            Map<String, Object> dataStream = (Map<String, Object>) dataStreams.get(0);
            assertThat(dataStream.get("name"), equalTo(dataStreamName));
            assertThat(dataStream.get("rollover_on_write"), is(false));
            assertThat(((List<Object>) dataStream.get("indices")).size(), is(2));
        }
    }

    @SuppressWarnings("unchecked")
    public void testLazyRolloverFailsIndexing() throws Exception {
        Request putComposableIndexTemplateRequest = new Request("POST", "/_index_template/lazy-ds-template");
        putComposableIndexTemplateRequest.setJsonEntity("""
            {
              "index_patterns": ["lazy-ds*"],
              "data_stream": {}
            }
            """);
        assertOK(client().performRequest(putComposableIndexTemplateRequest));

        String dataStreamName = "lazy-ds";

        Request createDocRequest = new Request("POST", "/" + dataStreamName + "/_doc?refresh=true");
        createDocRequest.setJsonEntity("{ \"@timestamp\": \"2020-10-22\", \"a\": 1 }");
        assertOK(client().performRequest(createDocRequest));

        Request updateClusterSettingsRequest = new Request("PUT", "_cluster/settings");
        updateClusterSettingsRequest.setJsonEntity("""
            {
              "persistent": {
                "cluster.max_shards_per_node": 1
              }
            }""");
        assertAcknowledged(client().performRequest(updateClusterSettingsRequest));

        final Response rolloverResponse = client().performRequest(new Request("POST", "/" + dataStreamName + "/_rollover?lazy"));
        Map<String, Object> rolloverResponseMap = entityAsMap(rolloverResponse);
        assertThat((String) rolloverResponseMap.get("old_index"), startsWith(".ds-lazy-ds-"));
        assertThat((String) rolloverResponseMap.get("old_index"), endsWith("-000001"));
        assertThat((String) rolloverResponseMap.get("new_index"), startsWith(".ds-lazy-ds-"));
        assertThat((String) rolloverResponseMap.get("new_index"), endsWith("-000002"));
        assertThat(rolloverResponseMap.get("lazy"), equalTo(true));
        assertThat(rolloverResponseMap.get("dry_run"), equalTo(false));
        assertThat(rolloverResponseMap.get("acknowledged"), equalTo(true));
        assertThat(rolloverResponseMap.get("rolled_over"), equalTo(false));
        assertThat(rolloverResponseMap.get("conditions"), equalTo(Map.of()));

        {
            final Response dataStreamResponse = client().performRequest(new Request("GET", "/_data_stream/" + dataStreamName));
            List<Object> dataStreams = (List<Object>) entityAsMap(dataStreamResponse).get("data_streams");
            assertThat(dataStreams.size(), is(1));
            Map<String, Object> dataStream = (Map<String, Object>) dataStreams.get(0);
            assertThat(dataStream.get("name"), equalTo(dataStreamName));
            assertThat(dataStream.get("rollover_on_write"), is(true));
            assertThat(((List<Object>) dataStream.get("indices")).size(), is(1));
        }

        try {
            createDocRequest = new Request("POST", "/" + dataStreamName + "/_doc?refresh=true");
            createDocRequest.setJsonEntity("{ \"@timestamp\": \"2020-10-23\", \"a\": 2 }");
            client().performRequest(createDocRequest);
            fail("Indexing should have failed.");
        } catch (ResponseException responseException) {
            assertThat(responseException.getMessage(), containsString("this action would add [2] shards"));
        }

        updateClusterSettingsRequest = new Request("PUT", "_cluster/settings");
        updateClusterSettingsRequest.setJsonEntity("""
            {
              "persistent": {
                "cluster.max_shards_per_node": null
              }
            }""");
        assertAcknowledged(client().performRequest(updateClusterSettingsRequest));
        createDocRequest = new Request("POST", "/" + dataStreamName + "/_doc?refresh=true");
        createDocRequest.setJsonEntity("{ \"@timestamp\": \"2020-10-23\", \"a\": 2 }");
        assertOK(client().performRequest(createDocRequest));
        {
            final Response dataStreamResponse = client().performRequest(new Request("GET", "/_data_stream/" + dataStreamName));
            List<Object> dataStreams = (List<Object>) entityAsMap(dataStreamResponse).get("data_streams");
            assertThat(dataStreams.size(), is(1));
            Map<String, Object> dataStream = (Map<String, Object>) dataStreams.get(0);
            assertThat(dataStream.get("name"), equalTo(dataStreamName));
            assertThat(dataStream.get("rollover_on_write"), is(false));
            assertThat(((List<Object>) dataStream.get("indices")).size(), is(2));
        }
    }

    @SuppressWarnings("unchecked")
    public void testLazyRolloverWithConditions() throws Exception {
        Request putComposableIndexTemplateRequest = new Request("POST", "/_index_template/lazy-ds-template");
        putComposableIndexTemplateRequest.setJsonEntity("""
            {
              "index_patterns": ["lazy-ds*"],
              "data_stream": {}
            }
            """);
        assertOK(client().performRequest(putComposableIndexTemplateRequest));

        String dataStreamName = "lazy-ds";

        Request createDocRequest = new Request("POST", "/" + dataStreamName + "/_doc?refresh=true");
        createDocRequest.setJsonEntity("{ \"@timestamp\": \"2020-10-22\", \"a\": 1 }");

        assertOK(client().performRequest(createDocRequest));

        Request rolloverRequest = new Request("POST", "/" + dataStreamName + "/_rollover?lazy");
        rolloverRequest.setJsonEntity("{\"conditions\": {\"max_docs\": 1}}");
        ResponseException responseError = expectThrows(ResponseException.class, () -> client().performRequest(rolloverRequest));
        assertThat(responseError.getResponse().getStatusLine().getStatusCode(), is(400));
        assertThat(responseError.getMessage(), containsString("only without any conditions"));
    }
}

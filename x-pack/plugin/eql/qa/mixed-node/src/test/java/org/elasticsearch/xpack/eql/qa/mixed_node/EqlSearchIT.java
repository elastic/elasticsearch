/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.qa.mixed_node;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.NotEqualMessageBuilder;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.ql.TestNode;
import org.elasticsearch.xpack.ql.TestNodes;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.xpack.ql.TestUtils.buildNodeAndVersions;
import static org.elasticsearch.xpack.ql.TestUtils.readResource;

/**
 * Class testing the behavior of events and sequence queries in a mixed cluster scenario (during rolling upgrade).
 * The test is against a three-node cluster where one node is upgraded, the other two are on the old version.
 *  
 */
public class EqlSearchIT extends ESRestTestCase {

    private static final String index = "test_eql_mixed_versions";
    private static int numShards;
    private static int numReplicas = 1;
    private static int numDocs;
    private static TestNodes nodes;
    private static List<TestNode> newNodes;
    private static List<TestNode> bwcNodes;

    @Before
    public void createIndex() throws IOException {
        nodes = buildNodeAndVersions(client());
        numShards = nodes.size();
        numDocs = randomIntBetween(numShards, 15);
        newNodes = new ArrayList<>(nodes.getNewNodes());
        bwcNodes = new ArrayList<>(nodes.getBWCNodes());
        
        String mappings = readResource(EqlSearchIT.class.getResourceAsStream("/eql_mapping.json"));
        createIndex(
            index,
            Settings.builder()
                .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), numShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
                .build(),
            mappings
        );
    }

    @After
    public void cleanUpIndex() throws IOException {
        if (indexExists(index)) {
            deleteIndex(index);
        }
    }

    public void testEventsWithRequestToOldNodes() throws Exception {
        assertEventsQueryOnNodes(bwcNodes);
    }

    public void testEventsWithRequestToUpgradedNodes() throws Exception {
        assertEventsQueryOnNodes(newNodes);
    }

    public void testSequencesWithRequestToOldNodes() throws Exception {
        assertSequncesQueryOnNodes(bwcNodes);
    }

    public void testSequencesWithRequestToUpgradedNodes() throws Exception {
        assertSequncesQueryOnNodes(newNodes);
    }

    public void testMultiValueFields() throws Exception {
        final String bulkEntries = readResource(EqlSearchIT.class.getResourceAsStream("/eql_data.json"));
        Request bulkRequst = new Request("POST", index + "/_bulk?refresh");
        bulkRequst.setJsonEntity(bulkEntries);
        assertOK(client().performRequest(bulkRequst));

        List<Map<String, Object>> sourceEvents = new ArrayList<Map<String, Object>>();
        Map<String, Object> expectedResponse = singletonMap("hits", singletonMap("events", sourceEvents));
        sourceEvents.add(singletonMap("_id", "116"));
        sourceEvents.add(singletonMap("_id", "117"));
        sourceEvents.add(singletonMap("_id", "120"));
        sourceEvents.add(singletonMap("_id", "121"));
        sourceEvents.add(singletonMap("_id", "122"));

        try (
            RestClient client = buildClient(restClientSettings(),
                newNodes.stream().map(TestNode::getPublishAddress).toArray(HttpHost[]::new))
        ) {
            // filter only the relevant bits of the response
            String filterPath = "filter_path=hits.events._id";

            Request request = new Request("POST", index + "/_eql/search?" + filterPath);
            request.setJsonEntity("{\"query\":\"PROCESS where concat(file_name, process_name) == \\\"foo\\\" or add(pid, ppid) > 100\"}");
            System.out.println("{\"query\":\"PROCESS where concat(file_name, process_name) == \\\"foo\\\" or add(pid, ppid) > 100\"}");
            assertBusy(() -> { assertResponse(expectedResponse, runEql(client, request)); });
        }
    }

    private void assertEventsQueryOnNodes(List<TestNode> nodesList) throws Exception {
        final String event = randomEvent();
        Map<String, Object> expectedResponse = prepareEventsTestData(event);
        try (
            RestClient client = buildClient(restClientSettings(),
                nodesList.stream().map(TestNode::getPublishAddress).toArray(HttpHost[]::new))
        ) {
            // filter only the relevant bits of the response
            String filterPath = "filter_path=hits.events._source.@timestamp,hits.events._source.event_type,hits.events._source.sequence";

            Request request = new Request("POST", index + "/_eql/search?" + filterPath);
            request.setJsonEntity("{\"query\":\"" + event + " where true\",\"size\":15}");
            assertBusy(() -> { assertResponse(expectedResponse, runEql(client, request)); });
        }
    }

    private void assertSequncesQueryOnNodes(List<TestNode> nodesList) throws Exception {
        Map<String, Object> expectedResponse = prepareSequencesTestData();
        try (
            RestClient client = buildClient(restClientSettings(),
                nodesList.stream().map(TestNode::getPublishAddress).toArray(HttpHost[]::new))
        ) {
            String filterPath = "filter_path=hits.sequences.join_keys,hits.sequences.events._id,hits.sequences.events._source";
            String query = "sequence by `sequence` with maxspan=100ms [success where true] by correlation_success1, correlation_success2 "
                + "[failure where true] by correlation_failure1, correlation_failure2";
            String filter = "{\"range\":{\"@timestamp\":{\"gte\":\"1970-05-01\"}}}";

            Request request = new Request("POST", index + "/_eql/search?" + filterPath);
            request.setJsonEntity("{\"query\":\"" + query + "\",\"filter\":" + filter + "}");
            assertBusy(() -> { assertResponse(expectedResponse, runEql(client, request)); });
        }
    }

    private String randomEvent() {
        return randomFrom("success", "failure");
    }

    private Map<String, Object> prepareEventsTestData(String event) throws IOException {
        List<Map<String, Object>> sourceEvents = new ArrayList<Map<String, Object>>();
        Map<String, Object> expectedResponse = singletonMap("hits", singletonMap("events", sourceEvents));
        StringBuilder builder = new StringBuilder();

        for (int i = 0; i < numDocs; i++) {
            final String randomEvent = randomEvent();
            builder.append("{\"index\":{\"_id\":" + i + "}}\n");
            builder.append("{");
            builder.append("\"@timestamp\":" + i + ",");
            builder.append("\"event_type\":\"" + randomEvent + "\",");
            builder.append("\"sequence\":" + i);
            builder.append("}\n");
            if (randomEvent.equals(event)) {
                Map<String, Object> eventSource = new HashMap<>();
                eventSource.put("@timestamp", i);
                eventSource.put("event_type", randomEvent);
                eventSource.put("sequence", i);
                sourceEvents.add(singletonMap("_source", eventSource));
            }
        }
        Request request = new Request("PUT", index + "/_bulk?refresh");
        request.setJsonEntity(builder.toString());
        assertOK(client().performRequest(request));
        if (sourceEvents.isEmpty()) {
            return emptyMap();
        }
        return expectedResponse;
    }

    /*
     * Output to compare with looks like this:
     * {
     *  "hits": {
     *  "sequences": [
     *      {
     *          "join_keys": [
     *              44,
     *              "C",
     *              "D"
     *          ],
     *          "events": [
     *              {
     *                  "_id": "14",
     *                  "_source": {
     *                  ...
     *                  }
     *              }
     *           ]
     *      }
     *  }
     * }
     *
     */
    private Map<String, Object> prepareSequencesTestData() throws IOException {
        Map<String, Object> event14 = new HashMap<>();
        Map<String, Object> event14Source = new HashMap<>();
        event14.put("_id", "14");
        event14.put("_source", event14Source);
        event14Source.put("@timestamp", "12345678914");
        event14Source.put("event_type", "success");
        event14Source.put("sequence", 44);
        event14Source.put("correlation_success1", "C");
        event14Source.put("correlation_success2", "D");

        Map<String, Object> event15 = new HashMap<>();
        Map<String, Object> event15Source = new HashMap<>();
        event15.put("_id", "15");
        event15.put("_source", event15Source);
        event15Source.put("@timestamp", "12345678999");
        event15Source.put("event_type", "failure");
        event15Source.put("sequence", 44);
        event15Source.put("correlation_failure1", "C");
        event15Source.put("correlation_failure2", "D");

        Map<String, Object> sequence = new HashMap<>();
        List<Map<String, Object>> events = unmodifiableList(asList(event14, event15));
        List<Map<String, Object>> sequences = singletonList(sequence);
        Map<String, Object> expectedResponse = singletonMap("hits", singletonMap("sequences", sequences));

        sequence.put("join_keys", asList(44, "C", "D"));
        sequence.put("events", events);

        final String bulkEntries = readResource(EqlSearchIT.class.getResourceAsStream("/eql_data.json"));
        Request request = new Request("POST", index + "/_bulk?refresh");
        request.setJsonEntity(bulkEntries);
        assertOK(client().performRequest(request));

        return expectedResponse;
    }

    private void assertResponse(Map<String, Object> expected, Map<String, Object> actual) {
        if (false == expected.equals(actual)) {
            NotEqualMessageBuilder message = new NotEqualMessageBuilder();
            message.compareMaps(actual, expected);
            fail("Response does not match:\n" + message.toString());
        }
    }

    private Map<String, Object> runEql(RestClient client, Request request) throws IOException {
        Response response = client.performRequest(request);
        try (InputStream content = response.getEntity().getContent()) {
            return XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false);
        }
    }
}

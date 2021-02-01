/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.qa.mixed_node;

import org.apache.http.HttpHost;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.NotEqualMessageBuilder;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ObjectPath;
import org.junit.After;
import org.junit.Before;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableList;

/**
 * Class testing the behavior of events and sequence queries in a mixed cluster scenario (during rolling upgrade).
 * The test is against a three-node cluster where one node is upgraded, the other two are on the old version.
 */
public class EqlSearchIT extends ESRestTestCase {

    private static final Version FIELDS_API_QL_INTRODUCTION = Version.V_7_12_0;
    private static final String index = "test_eql_mixed_versions";
    private static int numShards;
    private static int numReplicas = 1;
    private static int numDocs;
    private static Nodes nodes;
    private static List<Node> allNodes;
    private static List<Node> newNodes;
    private static List<Node> bwcNodes;
    private static Version bwcVersion;
    private static Version newVersion;
    private static boolean isBeforeFieldsApiInQL;

    @Before
    public void createIndex() throws IOException {
        nodes = buildNodeAndVersions(client());
        numShards = nodes.size();
        numDocs = randomIntBetween(numShards, 16);
        allNodes = new ArrayList<>();
        allNodes.addAll(nodes.getBWCNodes());
        allNodes.addAll(nodes.getNewNodes());
        newNodes = new ArrayList<>();
        newNodes.addAll(nodes.getNewNodes());
        bwcNodes = new ArrayList<>();
        bwcNodes.addAll(nodes.getBWCNodes());
        bwcVersion = nodes.getBWCNodes().get(0).getVersion();
        newVersion = nodes.getNewNodes().get(0).getVersion();
        isBeforeFieldsApiInQL = bwcVersion.before(FIELDS_API_QL_INTRODUCTION);
        
        String mappings = readResource("/eql_mapping.json");
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
        if (indexExists(index) == true) {
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

    private void assertEventsQueryOnNodes(List<Node> nodesList) throws Exception {
        final String event = randomEvent();
        Map<String, Object> expectedResponse = prepareTestData(event);
        try (
            RestClient client = buildClient(restClientSettings(), nodesList.stream().map(Node::getPublishAddress).toArray(HttpHost[]::new))
        ) {
            String filterPath = "filter_path=hits.events._source.@timestamp,hits.events._source.event_type,hits.events._source.sequence";

            Request request = new Request("POST", index + "/_eql/search?" + filterPath);
            request.setJsonEntity("{\"query\":\"" + event + " where true\"}");
            assertBusy(() -> { assertResponse(expectedResponse, runEql(client, request)); });
        }
    }

    private void assertSequncesQueryOnNodes(List<Node> nodesList) throws Exception {
        Map<String, Object> expectedResponse = prepareSequencesTestData();
        try (
            RestClient client = buildClient(restClientSettings(), nodesList.stream().map(Node::getPublishAddress).toArray(HttpHost[]::new))
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

    @SuppressWarnings("unchecked")
    private Map<String, Object> prepareTestData(String event) throws IOException {
        List<Map<String, Object>> sourceEvents = new ArrayList<Map<String, Object>>();
        Map<String, Object> expectedResponse = singletonMap("hits", singletonMap("events", sourceEvents));

        for (int i = 0; i < numDocs; i++) {
            StringBuilder builder = new StringBuilder();
            final String randomEvent = randomEvent();
            builder.append("{");
            builder.append("\"@timestamp\":" + i + ",");
            builder.append("\"event_type\":\"" + randomEvent + "\",");
            builder.append("\"sequence\":" + i);
            builder.append("}");
            if (randomEvent.equals(event)) {
                Map<String, Object> eventSource = new HashMap<>();
                eventSource.put("@timestamp", i);
                eventSource.put("event_type", randomEvent);
                eventSource.put("sequence", i);
                sourceEvents.add(singletonMap("_source", eventSource));
            }
            
            Request request = new Request("PUT", index + "/_doc/" + i);
            request.setJsonEntity(builder.toString());
            assertOK(client().performRequest(request));
        }
        if (sourceEvents.isEmpty()) {
            return Collections.EMPTY_MAP;
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

        final List<String> bulkEntries = getSequencesBulkEntries();
        StringBuilder builder = new StringBuilder();
        for (int i = 1; i < 16; i++) {
            builder.append("{\"index\": {\"_id\":" + i + "}}\n");
            builder.append(bulkEntries.get(i - 1));
        }
        
        Request request = new Request("POST", index + "/_bulk?refresh");
        request.setJsonEntity(builder.toString());
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

    private static String readResource(String location) throws IOException {
        StringBuilder builder = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(EqlSearchIT.class.getResourceAsStream(location),
            StandardCharsets.UTF_8)))
        {
            String line = reader.readLine();
            while (line != null) {
                if (line.trim().startsWith("//") == false) {
                    builder.append(line);
                    builder.append('\n');
                }
                line = reader.readLine();
            }
            return builder.toString();
        }
    }

    private List<String> getSequencesBulkEntries() {
        List<String> bulkEntries = new ArrayList<>(15);
        bulkEntries.add("{\"@timestamp\":\"1234567891\",\"event_type\":\"success\",\"sequence\":1,\"correlation_success1\":\"A\","
            + "\"correlation_success2\":\"B\"}\n");
        bulkEntries.add("{\"@timestamp\":\"1234567892\",\"event_type\":\"failure\",\"sequence\":2,\"correlation_failure1\":\"A\","
            + "\"correlation_failure2\":\"B\"}\n");
        bulkEntries.add("{\"@timestamp\":\"1234567893\",\"event_type\":\"success\",\"sequence\":3,\"correlation_success1\":\"A\","
            + "\"correlation_success2\":\"A\"}\n");
        bulkEntries.add("{\"@timestamp\":\"1234567894\",\"event_type\":\"success\",\"sequence\":4,\"correlation_success1\":\"C\","
            + "\"correlation_success2\":\"C\"}\n");
        bulkEntries.add("{\"@timestamp\":\"1234567895\",\"event_type\":\"failure\",\"sequence\":5,\"correlation_failure1\":\"B\","
            + "\"correlation_failure2\":\"C\"}\n");
        bulkEntries.add("{\"@timestamp\":\"1234567896\",\"event_type\":\"success\",\"sequence\":1,\"correlation_success1\":\"A\","
            + "\"correlation_success2\":\"A\"}\n");
        bulkEntries.add("{\"@timestamp\":\"1234567897\",\"event_type\":\"failure\",\"sequence\":1,\"correlation_failure1\":\"A\","
            + "\"correlation_failure2\":\"A\"}\n");
        bulkEntries.add("{\"@timestamp\":\"1234567898\",\"event_type\":\"success\",\"sequence\":3,\"correlation_success1\":\"A\","
            + "\"correlation_success2\":\"A\"}\n");
        bulkEntries.add("{\"@timestamp\":\"1234567899\",\"event_type\":\"success\",\"sequence\":4,\"correlation_success1\":\"C\","
            + "\"correlation_success2\":\"B\"}\n");
        bulkEntries.add("{\"@timestamp\":\"12345678910\",\"event_type\":\"failure\",\"sequence\":4,\"correlation_failure1\":\"B\","
            + "\"correlation_failure2\":\"B\"}\n");
        bulkEntries.add("{\"@timestamp\":\"12345678911\",\"event_type\":\"success\",\"sequence\":1,\"correlation_success1\":\"A\","
            + "\"correlation_success2\":\"A\"}\n");
        bulkEntries.add("{\"@timestamp\":\"12345678912\",\"event_type\":\"failure\",\"sequence\":1,\"correlation_failure1\":\"A\","
            + "\"correlation_failure2\":\"B\"}\n");
        bulkEntries.add("{\"@timestamp\":\"12345678913\",\"event_type\":\"success\",\"sequence\":3,\"correlation_success1\":\"A\","
            + "\"correlation_success2\":\"A\"}\n");
        bulkEntries.add("{\"@timestamp\":\"12345678914\",\"event_type\":\"success\",\"sequence\":44,\"correlation_success1\":\"C\","
            + "\"correlation_success2\":\"D\"}\n");
        bulkEntries.add("{\"@timestamp\":\"12345678999\",\"event_type\":\"failure\",\"sequence\":44,\"correlation_failure1\":\"C\","
            + "\"correlation_failure2\":\"D\"}\n");
        return unmodifiableList(bulkEntries);
    }

    static Nodes buildNodeAndVersions(RestClient client) throws IOException {
        Response response = client.performRequest(new Request("GET", "_nodes"));
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        Map<String, Object> nodesAsMap = objectPath.evaluate("nodes");
        Nodes nodes = new Nodes();
        for (String id : nodesAsMap.keySet()) {
            nodes.add(
                new Node(
                    id,
                    Version.fromString(objectPath.evaluate("nodes." + id + ".version")),
                    HttpHost.create(objectPath.evaluate("nodes." + id + ".http.publish_address"))
                )
            );
        }
        return nodes;
    }

    private static final class Nodes extends HashMap<String, Node> {

        public void add(Node node) {
            put(node.getId(), node);
        }

        public List<Node> getNewNodes() {
            Version bwcVersion = getBWCVersion();
            return values().stream().filter(n -> n.getVersion().after(bwcVersion)).collect(Collectors.toList());
        }

        public List<Node> getBWCNodes() {
            Version bwcVersion = getBWCVersion();
            return values().stream().filter(n -> n.getVersion().equals(bwcVersion)).collect(Collectors.toList());
        }

        public Version getBWCVersion() {
            if (isEmpty()) {
                throw new IllegalStateException("no nodes available");
            }
            return Version.fromId(values().stream().map(node -> node.getVersion().id).min(Integer::compareTo).get());
        }

        @Override
        public String toString() {
            return "Nodes{"
                + values().stream().map(Node::toString).collect(Collectors.joining("\n"))
                + '}';
        }
    }

    private static final class Node {
        private final String id;
        private final Version version;
        private final HttpHost publishAddress;

        Node(String id, Version version, HttpHost publishAddress) {
            this.id = id;
            this.version = version;
            this.publishAddress = publishAddress;
        }

        public String getId() {
            return id;
        }

        public HttpHost getPublishAddress() {
            return publishAddress;
        }

        public Version getVersion() {
            return version;
        }

        @Override
        public String toString() {
            return "Node{" + "id='" + id + '\'' + ", version=" + version + '}';
        }
    }
}

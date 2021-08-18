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
import org.elasticsearch.xpack.eql.execution.search.RuntimeUtils;
import org.elasticsearch.xpack.eql.expression.function.EqlFunctionRegistry;
import org.elasticsearch.xpack.ql.TestNode;
import org.elasticsearch.xpack.ql.TestNodes;
import org.elasticsearch.xpack.ql.expression.function.FunctionDefinition;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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

    /**
     * Requests are sent to the new (upgraded) version nodes of the cluster. The request should be redirected to the old nodes at this point
     * if their version is lower than {@code org.elasticsearch.xpack.eql.execution.search.RuntimeUtils.SWITCH_TO_MULTI_VALUE_FIELDS_VERSION}
     * version.
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/76618")
    public void testMultiValueFields() throws Exception {
        final String bulkEntries = readResource(EqlSearchIT.class.getResourceAsStream("/eql_data.json"));
        Request bulkRequst = new Request("POST", index + "/_bulk?refresh");
        bulkRequst.setJsonEntity(bulkEntries);
        assertOK(client().performRequest(bulkRequst));

        // build a set of functions names to check if all functions are tested with multi-value fields
        final Set<String> availableFunctions = new EqlFunctionRegistry().listFunctions()
            .stream()
            .map(FunctionDefinition::name)
            .collect(Collectors.toSet());
        // each function has a query and query results associated to it
        Set<String> testedFunctions = new HashSet<>();
        boolean multiValued = nodes.getBWCVersion().onOrAfter(RuntimeUtils.SWITCH_TO_MULTI_VALUE_FIELDS_VERSION);
        try (
            RestClient client = buildClient(restClientSettings(),
                newNodes.stream().map(TestNode::getPublishAddress).toArray(HttpHost[]::new))
        ) {
            // filter only the relevant bits of the response
            String filterPath = "filter_path=hits.events._id";
            Request request = new Request("POST", index + "/_eql/search?" + filterPath);

            assertMultiValueFunctionQuery(availableFunctions, testedFunctions, request, client, "between",
                "PROCESS where between(process_name, \\\"w\\\", \\\"s\\\") : \\\"indow\\\"",
                multiValued ? new int[] {120, 121} : new int[] {121});
            assertMultiValueFunctionQuery(availableFunctions, testedFunctions, request, client, "cidrmatch",
                "PROCESS where string(cidrmatch(source_address, \\\"10.6.48.157/24\\\")) : \\\"true\\\"",
                multiValued ? new int[] {121, 122} : new int[] {122});
            assertMultiValueFunctionQuery(availableFunctions, testedFunctions, request, client, "concat",
                "PROCESS where concat(file_name, process_name) == \\\"foo\\\" or add(pid, ppid) > 100",
                multiValued ? new int[] {116, 117, 120, 121, 122} : new int[] {120, 121});
            assertMultiValueFunctionQuery(availableFunctions, testedFunctions, request, client, "endswith",
                "PROCESS where string(endswith(process_name, \\\"s\\\")) : \\\"true\\\"",
                multiValued ? new int[] {120, 121} : new int[] {121});
            assertMultiValueFunctionQuery(availableFunctions, testedFunctions, request, client, "indexof",
                "PROCESS where indexof(file_name, \\\"x\\\", 2) > 0",
                multiValued ? new int[] {116, 117} : new int[] {117});
            assertMultiValueFunctionQuery(availableFunctions, testedFunctions, request, client, "length",
                "PROCESS where length(file_name) >= 3 and length(file_name) == 1",
                multiValued ? new int[] {116} : new int[] {});
            assertMultiValueFunctionQuery(availableFunctions, testedFunctions, request, client, "startswith",
                "PROCESS where string(startswith~(file_name, \\\"F\\\")) : \\\"true\\\"",
                multiValued ? new int[] {116, 117, 120, 121} : new int[] {116, 120, 121});
            assertMultiValueFunctionQuery(availableFunctions, testedFunctions, request, client, "string",
                "PROCESS where string(concat(file_name, process_name) == \\\"foo\\\") : \\\"true\\\"",
                multiValued ? new int[] {116, 120} : new int[] {120});
            assertMultiValueFunctionQuery(availableFunctions, testedFunctions, request, client, "stringcontains",
                "PROCESS where string(stringcontains(file_name, \\\"txt\\\")) : \\\"true\\\"",
                multiValued ? new int[] {117} : new int[] {});
            assertMultiValueFunctionQuery(availableFunctions, testedFunctions, request, client, "substring",
                "PROCESS where substring(file_name, -4) : \\\".txt\\\"",
                multiValued ? new int[] {117} : new int[] {});
            assertMultiValueFunctionQuery(availableFunctions, testedFunctions, request, client, "add",
                "PROCESS where add(pid, 1) == 2",
                multiValued ? new int[] {120, 121, 122} : new int[] {120, 121, 122});
            assertMultiValueFunctionQuery(availableFunctions, testedFunctions, request, client, "divide",
                "PROCESS where divide(pid, 12) == 1",
                multiValued ? new int[] {116, 117, 118, 119, 120, 122} : new int[] {116, 117, 118, 119});
            assertMultiValueFunctionQuery(availableFunctions, testedFunctions, request, client, "modulo",
                "PROCESS where modulo(ppid, 10) == 0",
                multiValued ? new int[] {121, 122} : new int[] {121});
            assertMultiValueFunctionQuery(availableFunctions, testedFunctions, request, client, "multiply",
                "PROCESS where multiply(pid, 10) == 120",
                multiValued ? new int[] {116, 117, 118, 119, 120, 122} : new int[] {116, 117, 118, 119, 120, 122});
            assertMultiValueFunctionQuery(availableFunctions, testedFunctions, request, client, "number",
                "PROCESS where number(command_line) + pid >= 360",
                multiValued ? new int[] {122, 123} : new int[] {123});
            assertMultiValueFunctionQuery(availableFunctions, testedFunctions, request, client, "subtract",
                "PROCESS where subtract(pid, 1) == 0",
                multiValued ? new int[] {120, 121, 122} : new int[] {120, 121, 122});
        }

        // check that ALL functions from the function registry have a test query. We don't want to miss any of the functions, since this
        // is about painless scripting
        assertTrue(testedFunctions.containsAll(availableFunctions));
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

    private void assertMultiValueFunctionQuery(Set<String> availableFunctions, Set<String> testedFunctions, Request request,
        RestClient client, String functionName, String query, int[] ids) throws IOException {
        List<Object> eventIds = new ArrayList<>();
        for (int id : ids) {
            eventIds.add(String.valueOf(id));
        }
        request.setJsonEntity("{\"query\":\"" + query + "\"}");
        assertResponse(query, eventIds, runEql(client, request));
        testedFunctions.add(functionName);
    }

    private void assertResponse(Map<String, Object> expected, Map<String, Object> actual) {
        if (false == expected.equals(actual)) {
            NotEqualMessageBuilder message = new NotEqualMessageBuilder();
            message.compareMaps(actual, expected);
            fail("Response does not match:\n" + message.toString());
        }
    }

    @SuppressWarnings("unchecked")
    private void assertResponse(String query, List<Object> expected, Map<String, Object> actual) {
        List<Map<String, Object>> events = new ArrayList<>();
        Map<String, Object> hits = (Map<String, Object>) actual.get("hits");
        if (hits == null || hits.isEmpty()) {
            if (expected.isEmpty()) {
                return;
            }
            fail("For query [" + query + "]\nResponse does not match: the returned list of resuts is empty.\nExpected " + expected);
        } else {
            events = (List<Map<String, Object>>) hits.get("events");
        }

        List<Object> actualList = new ArrayList<>();
        events.stream().forEach(m -> actualList.add(m.get("_id")));

        if (false == expected.equals(actualList)) {
            NotEqualMessageBuilder message = new NotEqualMessageBuilder();
            message.compareLists(actualList, expected);
            fail("For query [" + query + "]\nResponse does not match:\n" + message.toString());
        }
    }

    private Map<String, Object> runEql(RestClient client, Request request) throws IOException {
        Response response = client.performRequest(request);
        try (InputStream content = response.getEntity().getContent()) {
            return XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false);
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.qa.mixed_node;

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
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableMap;

public class SqlSearchIT extends ESRestTestCase {

    private static String index = "test_sql_mixed_versions";
    private static int numShards;
    private static int numReplicas = 1;
    private static int numDocs;
    private static Nodes nodes;
    private static List<Node> allNodes;
    private static List<Node> newNodes;
    private static List<Node> bwcNodes;
    private static Version bwcVersion;
    private static Version newVersion;
    private static Map<String, Object> expectedResponse;

    @Before
    public void prepareTestData() throws IOException {
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

        if (indexExists(index) == false) {
            expectedResponse = new HashMap<>();
            expectedResponse.put("columns", singletonList(columnInfo("test", "text")));
            List<List<String>> rows = new ArrayList<>(numDocs);
            expectedResponse.put("rows", rows);

            createIndex(
                index,
                Settings.builder()
                    .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), numShards)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
                    .build()
            );
            for (int i = 0; i < numDocs; i++) {
                String randomValue = "test_" + randomAlphaOfLength(3);
                Request request = new Request("PUT", index + "/_doc/" + i);
                request.setJsonEntity("{\"test\": \"" + randomValue + "\",\"id\":" + i + "}");
                assertOK(client().performRequest(request));
                rows.add(singletonList(randomValue));
            }
        }
    }

    public void testQueryToUpgradedNodes() throws Exception {
        try (
            RestClient client = buildClient(restClientSettings(), newNodes.stream().map(Node::getPublishAddress).toArray(HttpHost[]::new))
        ) {
            Request request = new Request("POST", "_sql");
            request.setJsonEntity(
                "{\"mode\":\"jdbc\",\"version\":\""
                    + newVersion.toString()
                    + "\",\"binary_format\":\"false\",\"query\":\"SELECT test FROM "
                    + index
                    + " ORDER BY id\"}"
            );
            assertBusy(() -> { assertResponse(expectedResponse, runSql(client, request)); });
        }
    }

    public void testQueryToOldNodes() throws Exception {
        try (
            RestClient client = buildClient(restClientSettings(), bwcNodes.stream().map(Node::getPublishAddress).toArray(HttpHost[]::new))
        ) {
            Request request = new Request("POST", "_sql");
            String versionSupport = bwcVersion.onOrAfter(Version.V_7_7_0) ? ",\"version\":\"" + newVersion.toString() + "\"" : "";
            String binaryFormatSupport = bwcVersion.onOrAfter(Version.V_7_6_0) ? ",\"binary_format\":\"false\"" : "";
            request.setJsonEntity(
                "{\"mode\":\"jdbc\"" + versionSupport + binaryFormatSupport + ",\"query\":\"SELECT test FROM "
                    + index
                    + " ORDER BY id\"}"
            );
            assertBusy(() -> { assertResponse(expectedResponse, runSql(client, request)); });
        }
    }

    private Map<String, Object> columnInfo(String name, String type) {
        Map<String, Object> column = new HashMap<>();
        column.put("name", name);
        column.put("type", type);
        if (bwcVersion.onOrAfter(Version.V_7_2_0)) {
            column.put("display_size", 2147483647);
        } else {
            column.put("display_size", 0);
        }
        return unmodifiableMap(column);
    }

    private void assertResponse(Map<String, Object> expected, Map<String, Object> actual) {
        if (false == expected.equals(actual)) {
            NotEqualMessageBuilder message = new NotEqualMessageBuilder();
            message.compareMaps(actual, expected);
            fail("Response does not match:\n" + message.toString());
        }
    }

    private Map<String, Object> runSql(RestClient client, Request request) throws IOException {
        Response response = client.performRequest(request);
        try (InputStream content = response.getEntity().getContent()) {
            return XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false);
        }
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
                    objectPath.evaluate("nodes." + id + ".name"),
                    Version.fromString(objectPath.evaluate("nodes." + id + ".version")),
                    HttpHost.create(objectPath.evaluate("nodes." + id + ".http.publish_address"))
                )
            );
        }
        response = client.performRequest(new Request("GET", "_cluster/state"));
        nodes.setMasterNodeId(ObjectPath.createFromResponse(response).evaluate("master_node"));
        return nodes;
    }

    static final class Nodes extends HashMap<String, Node> {

        private String masterNodeId = null;

        public Node getMaster() {
            return get(masterNodeId);
        }

        public void setMasterNodeId(String id) {
            if (get(id) == null) {
                throw new IllegalArgumentException("node with id [" + id + "] not found. got:" + toString());
            }
            masterNodeId = id;
        }

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
                + "masterNodeId='"
                + masterNodeId
                + "'\n"
                + values().stream().map(Node::toString).collect(Collectors.joining("\n"))
                + '}';
        }
    }

    static final class Node {
        private final String id;
        private final String nodeName;
        private final Version version;
        private final HttpHost publishAddress;

        Node(String id, String nodeName, Version version, HttpHost publishAddress) {
            this.id = id;
            this.nodeName = nodeName;
            this.version = version;
            this.publishAddress = publishAddress;
        }

        public String getId() {
            return id;
        }

        public String getNodeName() {
            return nodeName;
        }

        public HttpHost getPublishAddress() {
            return publishAddress;
        }

        public Version getVersion() {
            return version;
        }

        @Override
        public String toString() {
            return "Node{" + "id='" + id + '\'' + ", nodeName='" + nodeName + '\'' + ", version=" + version + '}';
        }
    }
}

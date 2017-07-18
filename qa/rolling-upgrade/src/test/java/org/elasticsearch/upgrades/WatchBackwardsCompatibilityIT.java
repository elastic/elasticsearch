/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.upgrades;

import com.google.common.base.Charsets;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ObjectPath;
import org.elasticsearch.xpack.watcher.condition.AlwaysCondition;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.security.SecurityLifecycleService.SECURITY_TEMPLATE_NAME;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public class WatchBackwardsCompatibilityIT extends ESRestTestCase {

    private Nodes nodes;

    @Before
    public void waitForSecuritySetup() throws Exception {
        String masterNode = null;
        String catNodesResponse = EntityUtils.toString(
                client().performRequest("GET", "/_cat/nodes?h=id,master").getEntity(),
                StandardCharsets.UTF_8
        );
        for (String line : catNodesResponse.split("\n")) {
            int indexOfStar = line.indexOf('*'); // * in the node's output denotes it is master
            if (indexOfStar != -1) {
                masterNode = line.substring(0, indexOfStar).trim();
                break;
            }
        }
        assertNotNull(masterNode);
        final String masterNodeId = masterNode;

        assertBusy(() -> {
            try {
                Response nodeDetailsResponse = client().performRequest("GET", "/_nodes");
                ObjectPath path = ObjectPath.createFromResponse(nodeDetailsResponse);
                Map<String, Object> nodes = path.evaluate("nodes");
                assertThat(nodes.size(), greaterThanOrEqualTo(2));
                String masterVersion = null;
                for (String key : nodes.keySet()) {
                    // get the ES version number master is on
                    if (key.startsWith(masterNodeId)) {
                        masterVersion = path.evaluate("nodes." + key + ".version");
                        break;
                    }
                }
                assertNotNull(masterVersion);
                final String masterTemplateVersion = masterVersion;

                Response response = client().performRequest("GET", "/_cluster/state/metadata");
                ObjectPath objectPath = ObjectPath.createFromResponse(response);
                final String mappingsPath = "metadata.templates." + SECURITY_TEMPLATE_NAME + "" +
                        ".mappings";
                Map<String, Object> mappings = objectPath.evaluate(mappingsPath);
                assertNotNull(mappings);
                assertThat(mappings.size(), greaterThanOrEqualTo(1));
                for (String key : mappings.keySet()) {
                    String templateVersion = objectPath.evaluate(mappingsPath + "." + key + "" +
                            "._meta.security-version");
                    assertEquals(masterTemplateVersion, templateVersion);
                }
            } catch (Exception e) {
                throw new AssertionError("failed to get cluster state", e);
            }
        });

        nodes = buildNodeAndVersions();
        logger.info("Nodes in cluster before test: bwc [{}], new [{}], master [{}]", nodes.getBWCNodes(), nodes.getNewNodes(),
                nodes.getMaster());
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected Settings restClientSettings() {
        String token = "Basic " + Base64.getEncoder()
                .encodeToString(("test_user:x-pack-test-password").getBytes(StandardCharsets.UTF_8));
        return Settings.builder()
                .put(ThreadContext.PREFIX + ".Authorization", token)
                .build();
    }

    public void testWatcherStats() throws Exception {
        executeAgainstAllNodes(client ->
            assertOK(client.performRequest("GET", "/_xpack/watcher/stats"))
        );
    }

    public void testWatcherRestart() throws Exception {
        // TODO we should be able to run this against any node, once the bwc serialization issues are fixed
        executeAgainstMasterNode(client -> {
            assertOK(client.performRequest("POST", "/_xpack/watcher/_stop"));
            assertBusy(() -> {
                try (InputStream is = client.performRequest("GET", "_xpack/watcher/stats").getEntity().getContent()) {
                    // TODO once the serialization fix is in here, we can check for concrete fields if the run against a 5.x or a 6.x node
                    // using a checkedbiconsumer, that provides info against which node the request runs
                    String responseBody = Streams.copyToString(new InputStreamReader(is, Charsets.UTF_8));
                    assertThat(responseBody, not(containsString("\"watcher_state\":\"starting\"")));
                    assertThat(responseBody, not(containsString("\"watcher_state\":\"started\"")));
                    assertThat(responseBody, not(containsString("\"watcher_state\":\"stopping\"")));
                }
            });
        });

        // TODO we should be able to run this against any node, once the bwc serialization issues are fixed
        executeAgainstMasterNode(client -> {
            assertOK(client.performRequest("POST", "/_xpack/watcher/_start"));
            assertBusy(() -> {
                try (InputStream is = client.performRequest("GET", "_xpack/watcher/stats").getEntity().getContent()) {
                    // TODO once the serialization fix is in here, we can check for concrete fields if the run against a 5.x or a 6.x node
                    // using a checkedbiconsumer, that provides info against which node the request runs
                    String responseBody = Streams.copyToString(new InputStreamReader(is, Charsets.UTF_8));
                    assertThat(responseBody, not(containsString("\"watcher_state\":\"starting\"")));
                    assertThat(responseBody, not(containsString("\"watcher_state\":\"stopping\"")));
                    assertThat(responseBody, not(containsString("\"watcher_state\":\"stopped\"")));
                }
            });
        });
    }

    public void testWatchCrudApis() throws IOException {
        assumeFalse("new nodes is empty", nodes.getNewNodes().isEmpty());

        BytesReference bytesReference = watchBuilder()
                .trigger(schedule(interval("5m")))
                .input(simpleInput())
                .condition(AlwaysCondition.INSTANCE)
                .addAction("_action1", loggingAction("{{ctx.watch_id}}"))
                .buildAsBytes(XContentType.JSON);
        StringEntity entity = new StringEntity(bytesReference.utf8ToString(),
                ContentType.APPLICATION_JSON);

        // execute upgrade if new nodes are in the cluster
        executeUpgradeIfClusterHasNewNode();

        executeAgainstAllNodes(client -> {
            Map<String, String> params = Collections.singletonMap("error_trace", "true");
            assertOK(client.performRequest("PUT", "/_xpack/watcher/watch/my-watch", params, entity));
            assertOK(client.performRequest("GET", "/_xpack/watcher/watch/my-watch", params));
            assertOK(client.performRequest("POST", "/_xpack/watcher/watch/my-watch/_execute", params));
            assertOK(client.performRequest("PUT", "/_xpack/watcher/watch/my-watch/_deactivate", params));
            assertOK(client.performRequest("PUT", "/_xpack/watcher/watch/my-watch/_activate", params));
        });
    }

    public void executeUpgradeIfClusterHasNewNode()
            throws IOException {
        HttpHost[] newHosts = nodes.getNewNodes().stream().map(Node::getPublishAddress).toArray(HttpHost[]::new);
        if (newHosts.length > 0) {
            try (RestClient client = buildClient(restClientSettings(), newHosts)) {
                logger.info("checking that upgrade procedure on the new cluster is required, hosts [{}]", Arrays.asList(newHosts));
                Map<String, String> params = Collections.singletonMap("error_trace", "true");
                Map<String, Object> response = toMap(client().performRequest("GET", "_xpack/migration/assistance", params));
                String action = ObjectPath.evaluate(response, "indices.\\.watches.action_required");
                logger.info("migration assistance response [{}]", action);
                if ("upgrade".equals(action)) {
                    client.performRequest("POST", "_xpack/migration/upgrade/.watches", params);
                }
            }
        }
    }

    private void executeAgainstRandomNode(CheckedConsumer<RestClient, Exception> consumer) throws Exception {
        List<Node> nodes = new ArrayList<>(this.nodes.values());
        nodes.sort(Comparator.comparing(Node::getId));
        Node node = randomFrom(nodes);

        try (RestClient client = buildClient(restClientSettings(), new HttpHost[] { node.getPublishAddress() })) {
            consumer.accept(client);
        }
    }

    private void executeAgainstMasterNode(CheckedConsumer<RestClient, Exception> consumer) throws Exception {
        try (RestClient client = buildClient(restClientSettings(), new HttpHost[] { this.nodes.getMaster().publishAddress })) {
            consumer.accept(client);
        }
    }

    private void executeAgainstAllNodes(CheckedConsumer<RestClient, IOException> consumer)
            throws IOException {
        HttpHost[] newHosts = nodes.getNewNodes().stream().map(Node::getPublishAddress).toArray(HttpHost[]::new);
        HttpHost[] bwcHosts = nodes.getBWCNodes().stream().map(Node::getPublishAddress).toArray(HttpHost[]::new);
        assertTrue("No nodes in cluster, cannot run any tests", newHosts.length > 0 || bwcHosts.length > 0);

        if (newHosts.length > 0) {
            try (RestClient newClient = buildClient(restClientSettings(), newHosts)) {
                consumer.accept(newClient);
            }
        }

        if (bwcHosts.length > 0) {
            try (RestClient bwcClient = buildClient(restClientSettings(), bwcHosts)) {
                consumer.accept(bwcClient);
            }
        }
    }

    private void assertOK(Response response) {
        assertThat(response.getStatusLine().getStatusCode(), anyOf(equalTo(200), equalTo(201)));
    }

    private Nodes buildNodeAndVersions() throws IOException {
        Response response = client().performRequest("GET", "_nodes");
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        Map<String, Object> nodesAsMap = objectPath.evaluate("nodes");
        Nodes nodes = new Nodes();
        for (String id : nodesAsMap.keySet()) {
            nodes.add(new Node(
                    id,
                    objectPath.evaluate("nodes." + id + ".name"),
                    Version.fromString(objectPath.evaluate("nodes." + id + ".version")),
                    HttpHost.create(objectPath.evaluate("nodes." + id + ".http.publish_address"))));
        }
        response = client().performRequest("GET", "_cluster/state");
        nodes.setMasterNodeId(ObjectPath.createFromResponse(response).evaluate("master_node"));
        return nodes;
    }

    final class Nodes extends HashMap<String, Node> {

        private String masterNodeId = null;

        public Node getMaster() {
            return get(masterNodeId);
        }

        public void setMasterNodeId(String id) {
            if (get(id) == null) {
                throw new IllegalArgumentException("node with id [" + id + "] not found. got:" +
                        toString());
            }
            masterNodeId = id;
        }

        public void add(Node node) {
            put(node.getId(), node);
        }

        public List<Node> getNewNodes() {
            Version bwcVersion = getBWCVersion();
            return values().stream().filter(n -> n.getVersion().after(bwcVersion))
                    .collect(Collectors.toList());
        }

        public List<Node> getBWCNodes() {
            Version bwcVersion = getBWCVersion();
            return values().stream().filter(n -> n.getVersion().equals(bwcVersion))
                    .collect(Collectors.toList());
        }

        public Version getBWCVersion() {
            if (isEmpty()) {
                throw new IllegalStateException("no nodes available");
            }
            return Version.fromId(values().stream().map(node -> node.getVersion().id).min(Integer::compareTo).get());
        }

        public Node getSafe(String id) {
            Node node = get(id);
            if (node == null) {
                throw new IllegalArgumentException("node with id [" + id + "] not found");
            }
            return node;
        }

        @Override
        public String toString() {
            return "Nodes{" +
                    "masterNodeId='" + masterNodeId + "'\n" +
                    values().stream().map(Node::toString).collect(Collectors.joining("\n")) +
                    '}';
        }
    }

    final class Node {
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
            return "Node{" +
                    "id='" + id + '\'' +
                    ", nodeName='" + nodeName + '\'' +
                    ", version=" + version +
                    ", address=" + publishAddress +
                    '}';
        }
    }

    static Map<String, Object> toMap(Response response) throws IOException {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(response.getEntity()), false);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.upgrades;

import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.rest.yaml.ObjectPath;
import org.elasticsearch.xpack.core.watcher.condition.AlwaysCondition;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;

@TestLogging("org.elasticsearch.client:TRACE")
public class WatchBackwardsCompatibilityIT extends AbstractUpgradeTestCase {

    private final StringEntity entity = new StringEntity(watchBuilder()
            .trigger(schedule(interval("5m")))
            .input(simpleInput())
            .condition(AlwaysCondition.INSTANCE)
            .addAction("_action1", loggingAction("{{ctx.watch_id}}"))
            .buildAsBytes(XContentType.JSON)
            .utf8ToString(),
            ContentType.APPLICATION_JSON);
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
                final String mappingsPath = "metadata.templates.security-index-template.mappings";
                Map<String, Object> mappings = objectPath.evaluate(mappingsPath);
                assertNotNull(mappings);
                assertThat(mappings.size(), greaterThanOrEqualTo(1));
                for (String key : mappings.keySet()) {
                    String templateVersion = objectPath.evaluate(mappingsPath + "." + key + "" +
                            "._meta.security-version");
                    final Version mVersion = Version.fromString(masterTemplateVersion);
                    final Version tVersion = Version.fromString(templateVersion);
                    assertEquals(mVersion, tVersion);
                }
            } catch (Exception e) {
                throw new AssertionError("failed to get cluster state", e);
            }
        });

        nodes = buildNodeAndVersions();
        logger.info("Nodes in cluster before test: bwc [{}], new [{}], master [{}]", nodes.getBWCNodes(), nodes.getNewNodes(),
                nodes.getMaster());

        Map<String, String> params = Collections.singletonMap("error_trace", "true");
        executeAgainstMasterNode(client -> {
            // create a watch before each test, most of the time this is just overwriting...
            assertOK(client.performRequest("PUT", "/_xpack/watcher/watch/my-watch", params, entity));
            // just a check to see if we can execute a watch, purely optional
            if (randomBoolean()) {
                assertOK(client.performRequest("POST", "/_xpack/watcher/watch/my-watch/_execute", params,
                        new StringEntity("{ \"record_execution\" : true }", ContentType.APPLICATION_JSON)));
            }
            if (randomBoolean()) {
                Map<String, String> ignore404Params = MapBuilder.newMapBuilder(params).put("ignore", "404").immutableMap();
                Response indexExistsResponse = client.performRequest("HEAD", "/.triggered_watches", ignore404Params);
                if (indexExistsResponse.getStatusLine().getStatusCode() == 404) {
                    logger.info("Created triggered watches index to ensure it gets upgraded");
                    client.performRequest("PUT", "/.triggered_watches");
                }
            }
        });

        // helping debugging output
        executeAgainstMasterNode(client -> {
            Map<String, String> filterPathParams = MapBuilder.newMapBuilder(params)
                    .put("filter_path", "*.template,*.index_patterns").immutableMap();
            Response r = client.performRequest("GET", "_template/*watch*", filterPathParams);
            logger.info("existing watcher templates response [{}]", EntityUtils.toString(r.getEntity(), StandardCharsets.UTF_8));
        });

        // set logging to debug
//        executeAgainstMasterNode(client -> {
//            StringEntity entity = new StringEntity("{ \"transient\" : { \"logger.org.elasticsearch.xpack.watcher\" : \"TRACE\" } }",
//                    ContentType.APPLICATION_JSON);
//            Response response = client.performRequest("PUT", "_cluster/settings", params, entity);
//            logger.info("cluster update settings response [{}]", EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8));
//        });
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveTemplatesUponCompletion() {
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

    @AwaitsFix(bugUrl="https://github.com/elastic/elasticsearch/issues/33753")
    public void testWatcherRestart() throws Exception {
        executeUpgradeIfNeeded();

        executeAgainstRandomNode(client -> assertOK(client.performRequest("POST", "/_xpack/watcher/_stop")));
        ensureWatcherStopped();

        executeAgainstRandomNode(client -> assertOK(client.performRequest("POST", "/_xpack/watcher/_start")));
        // Watcher should be started on at least the nodes with the new version.
        ensureWatcherStartedOnExpectedNodes();
    }

    public void testWatchCrudApis() throws Exception {
        assumeFalse("new nodes is empty", nodes.getNewNodes().isEmpty());

        // execute upgrade if new nodes are in the cluster
        executeUpgradeIfNeeded();

        executeAgainstAllNodes(client -> {
            Map<String, String> params = Collections.singletonMap("error_trace", "true");
            assertOK(client.performRequest("PUT", "/_xpack/watcher/watch/my-watch", params, entity));
            assertOK(client.performRequest("GET", "/_xpack/watcher/watch/my-watch", params));
            assertOK(client.performRequest("POST", "/_xpack/watcher/watch/my-watch/_execute", params));
            assertOK(client.performRequest("PUT", "/_xpack/watcher/watch/my-watch/_deactivate", params));
            assertOK(client.performRequest("PUT", "/_xpack/watcher/watch/my-watch/_activate", params));
        });
    }

    public void executeUpgradeIfNeeded() throws Exception {
        // if new nodes exists, this is a mixed cluster
        boolean only6xNodes = nodes.getBWCVersion().major >= 6;
        final List<Node> nodesToQuery = only6xNodes ? nodes.getBWCNodes() : nodes.getNewNodes();
        final HttpHost[] httpHosts = nodesToQuery.stream().map(Node::getPublishAddress).toArray(HttpHost[]::new);
        if (httpHosts.length > 0) {
            try (RestClient client = buildClient(restClientSettings(), httpHosts)) {
                logger.info("checking that upgrade procedure on the new cluster is required, hosts [{}]", Arrays.asList(httpHosts));
                Map<String, String> params = Collections.singletonMap("error_trace", "true");
                Response assistanceResponse = client().performRequest("GET", "_xpack/migration/assistance", params);
                String assistanceResponseData = EntityUtils.toString(assistanceResponse.getEntity());
                logger.info("assistance response is: [{}]", assistanceResponseData);
                Map<String, Object> response = toMap(assistanceResponseData);
                String watchIndexUpgradeRequired = ObjectPath.evaluate(response, "indices.\\.watches.action_required");
                String triggeredWatchIndexUpgradeRequired = ObjectPath.evaluate(response, "indices.\\.triggered_watches.action_required");
                if ("upgrade".equals(watchIndexUpgradeRequired) || "upgrade".equals(triggeredWatchIndexUpgradeRequired)) {
                    boolean stopWatcherBeforeUpgrade = randomBoolean();
                    logger.info("Stopping watcher before upgrade [{}]", stopWatcherBeforeUpgrade);
                    if (stopWatcherBeforeUpgrade) {
                        assertOK(client.performRequest("POST", "/_xpack/watcher/_stop"));
                        logger.info("stopped watcher manually before starting upgrade");
                    }

                    if ("upgrade".equals(watchIndexUpgradeRequired)) {
                        Response upgradeResponse = client.performRequest("POST", "_xpack/migration/upgrade/.watches", params);
                        logger.info("Upgrade .watches response is: [{}]", EntityUtils.toString(upgradeResponse.getEntity()));
                    }

                    if ("upgrade".equals(triggeredWatchIndexUpgradeRequired)) {
                        Response upgradeResponse = client.performRequest("POST", "_xpack/migration/upgrade/.triggered_watches", params);
                        logger.info("Upgrade .triggered_watches response is: [{}]", EntityUtils.toString(upgradeResponse.getEntity()));
                    }

                    // show templates after upgrade
                    executeAgainstMasterNode(c -> {
                        Map<String, String> filterPathParams = MapBuilder.newMapBuilder(params)
                                .put("filter_path", "*.template,*.index_patterns").immutableMap();
                        Response r = c.performRequest("GET", "_template/*watch*", filterPathParams);
                        logger.info("existing watcher templates response after upgrade [{}]",
                                EntityUtils.toString(r.getEntity(), StandardCharsets.UTF_8));
                    });


                    if (stopWatcherBeforeUpgrade) {
                        ensureWatcherStopped();
                        assertOK(client.performRequest("POST", "/_xpack/watcher/_start"));
                        logger.info("started watcher manually after running upgrade");
                    }
                    ensureWatcherStarted();
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
        try (RestClient client = buildClient(restClientSettings(), new HttpHost[]{this.nodes.getMaster().publishAddress})) {
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

    private void ensureWatcherStopped() throws Exception {
        executeAgainstMasterNode(client -> assertBusy(() -> {
            Response stats = client.performRequest("GET", "_xpack/watcher/stats");
            String responseBody = EntityUtils.toString(stats.getEntity());
            logger.info("ensureWatcherStopped(), stats response [{}]", responseBody);
            assertThat(responseBody, not(containsString("\"watcher_state\":\"starting\"")));
            assertThat(responseBody, not(containsString("\"watcher_state\":\"started\"")));
            assertThat(responseBody, not(containsString("\"watcher_state\":\"stopping\"")));
        }));
    }

    private void ensureWatcherStarted() throws Exception {
        executeAgainstMasterNode(client -> assertBusy(() -> {
            Response response = client.performRequest("GET", "_xpack/watcher/stats");
            String responseBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            logger.info("ensureWatcherStarted(), stats response [{}]", responseBody);
            assertThat(responseBody, not(containsString("\"watcher_state\":\"starting\"")));
            assertThat(responseBody, not(containsString("\"watcher_state\":\"stopping\"")));
            assertThat(responseBody, not(containsString("\"watcher_state\":\"stopped\"")));
        }));
    }

    private void ensureWatcherStartedOnExpectedNodes() throws Exception {
        if (nodes.getMaster().getVersion().before(Version.V_6_0_0)) {
            /*
             * Versions before 6.0 ran watcher on the master node and the
             * logic in ensureWatcherStarted is fine.
             */
            ensureWatcherStarted();
            return;
        }
        executeAgainstMasterNode(client -> assertBusy(() -> {
            Map<?, ?> responseBody = entityAsMap(client.performRequest("GET", "_xpack/watcher/stats"));
            logger.info("ensureWatcherStartedOnModernNodes(), stats response [{}]", responseBody);
            Map<?, ?> stats = ((List<?>) responseBody.get("stats")).stream()
                .map(o -> (Map<?, ?>) o)
                .collect(Collectors.toMap(m -> m.get("node_id"), Function.identity()));
            if (nodes.getBWCVersion().before(Version.V_6_0_0)) {
                Map<?, ?> nodeStats = (Map<?, ?>) stats.get(nodes.getMaster().getId());
                // If the old version is before 6.0 then only the master is allowed to start
                assertEquals("master node [" + nodes.getMaster().getId() + "] is not started",
                        "started", nodeStats.get("watcher_state"));
                return;
            }
            for (Node node : nodes.getNewNodes()) {
                Map<?, ?> nodeStats = (Map<?, ?>) stats.get(node.getId());
                assertEquals("modern node [" + node.getId() + "] is not started",
                        "started", nodeStats.get("watcher_state"));
            }
        }));
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

    static Map<String, Object> toMap(String response) throws IOException {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, response, false);
    }
}

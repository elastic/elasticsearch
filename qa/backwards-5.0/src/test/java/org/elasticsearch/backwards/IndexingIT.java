/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.backwards;

import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbersService;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ObjectPath;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

public class IndexingIT extends ESRestTestCase {

    private ObjectPath objectPath(Response response) throws IOException {
        String body = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
        String contentType = response.getHeader("Content-Type");
        XContentType xContentType = XContentType.fromMediaTypeOrFormat(contentType);
        return ObjectPath.createFromXContent(xContentType.xContent(), body);
    }

    private void assertOK(Response response) {
        assertThat(response.getStatusLine().getStatusCode(), anyOf(equalTo(200), equalTo(201)));
    }

    private void ensureGreen() throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("wait_for_status", "green");
        params.put("wait_for_no_relocating_shards", "true");
        assertOK(client().performRequest("GET", "_cluster/health", params));
    }

    private void createIndex(String name, Settings settings) throws IOException {
        assertOK(client().performRequest("PUT", name, Collections.emptyMap(),
            new StringEntity("{ \"settings\": " + Strings.toString(settings) + " }", ContentType.APPLICATION_JSON)));
    }

    private void updateIndexSetting(String name, Settings.Builder settings) throws IOException {
        updateIndexSetting(name, settings.build());
    }
    private void updateIndexSetting(String name, Settings settings) throws IOException {
        assertOK(client().performRequest("PUT", name + "/_settings", Collections.emptyMap(),
            new StringEntity(Strings.toString(settings), ContentType.APPLICATION_JSON)));
    }

    private int indexDocs(String index, final int idStart, final int numDocs) throws IOException {
        for (int i = 0; i < numDocs; i++) {
            final int id = idStart + i;
            assertOK(client().performRequest("PUT", index + "/test/" + id, emptyMap(),
                new StringEntity("{\"test\": \"test_" + id + "\"}", ContentType.APPLICATION_JSON)));
        }
        return numDocs;
    }

    /**
     * Indexes a document in <code>index</code> with <code>docId</code> then concurrently updates the same document
     * <code>nUpdates</code> times
     *
     * @return the document version after updates
     */
    private int indexDocWithConcurrentUpdates(String index, final int docId, int nUpdates) throws IOException, InterruptedException {
        indexDocs(index, docId, 1);
        Thread[] indexThreads = new Thread[nUpdates];
        for (int i = 0; i < nUpdates; i++) {
            indexThreads[i] = new Thread(() -> {
                try {
                    indexDocs(index, docId, 1);
                } catch (IOException e) {
                    throw new AssertionError("failed while indexing [" + e.getMessage() + "]");
                }
            });
            indexThreads[i].start();
        }
        for (Thread indexThread : indexThreads) {
            indexThread.join();
        }
        return nUpdates + 1;
    }

    public void testIndexVersionPropagation() throws Exception {
        Nodes nodes = buildNodeAndVersions();
        assumeFalse("new nodes is empty", nodes.getNewNodes().isEmpty());
        logger.info("cluster discovered: {}", nodes.toString());
        final List<String> bwcNamesList = nodes.getBWCNodes().stream().map(Node::getNodeName).collect(Collectors.toList());
        final String bwcNames = bwcNamesList.stream().collect(Collectors.joining(","));
        Settings.Builder settings = Settings.builder()
                .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 2)
                .put("index.routing.allocation.include._name", bwcNames);
        final String index = "test";
        final int minUpdates = 5;
        final int maxUpdates = 10;
        createIndex(index, settings.build());
        try (RestClient newNodeClient = buildClient(restClientSettings(),
                nodes.getNewNodes().stream().map(Node::getPublishAddress).toArray(HttpHost[]::new))) {

            int nUpdates = randomIntBetween(minUpdates, maxUpdates);
            logger.info("indexing docs with [{}] concurrent updates initially", nUpdates);
            final int finalVersionForDoc1 = indexDocWithConcurrentUpdates(index, 1, nUpdates);
            logger.info("allowing shards on all nodes");
            updateIndexSetting(index, Settings.builder().putNull("index.routing.allocation.include._name"));
            ensureGreen();
            assertOK(client().performRequest("POST", index + "/_refresh"));
            List<Shard> shards = buildShards(nodes, newNodeClient);
            for (Shard shard : shards) {
                assertVersion(index, 1, "_only_nodes:" + shard.getNode().getNodeName(), finalVersionForDoc1);
                assertCount(index, "_only_nodes:" + shard.getNode().getNodeName(), 1);
            }

            nUpdates = randomIntBetween(minUpdates, maxUpdates);
            logger.info("indexing docs with [{}] concurrent updates after allowing shards on all nodes", nUpdates);
            final int finalVersionForDoc2 = indexDocWithConcurrentUpdates(index, 2, nUpdates);
            assertOK(client().performRequest("POST", index + "/_refresh"));
            shards = buildShards(nodes, newNodeClient);
            for (Shard shard : shards) {
                assertVersion(index, 2, "_only_nodes:" + shard.getNode().getNodeName(), finalVersionForDoc2);
                assertCount(index, "_only_nodes:" + shard.getNode().getNodeName(), 2);
            }

            Shard primary = buildShards(nodes, newNodeClient).stream().filter(Shard::isPrimary).findFirst().get();
            logger.info("moving primary to new node by excluding {}", primary.getNode().getNodeName());
            updateIndexSetting(index, Settings.builder().put("index.routing.allocation.exclude._name", primary.getNode().getNodeName()));
            ensureGreen();
            nUpdates = randomIntBetween(minUpdates, maxUpdates);
            logger.info("indexing docs with [{}] concurrent updates after moving primary", nUpdates);
            final int finalVersionForDoc3 = indexDocWithConcurrentUpdates(index, 3, nUpdates);
            assertOK(client().performRequest("POST", index + "/_refresh"));
            shards = buildShards(nodes, newNodeClient);
            for (Shard shard : shards) {
                assertVersion(index, 3, "_only_nodes:" + shard.getNode().getNodeName(), finalVersionForDoc3);
                assertCount(index, "_only_nodes:" + shard.getNode().getNodeName(), 3);
            }

            logger.info("setting number of replicas to 0");
            updateIndexSetting(index, Settings.builder().put("index.number_of_replicas", 0));
            ensureGreen();
            nUpdates = randomIntBetween(minUpdates, maxUpdates);
            logger.info("indexing doc with [{}] concurrent updates after setting number of replicas to 0", nUpdates);
            final int finalVersionForDoc4 = indexDocWithConcurrentUpdates(index, 4, nUpdates);
            assertOK(client().performRequest("POST", index + "/_refresh"));
            shards = buildShards(nodes, newNodeClient);
            for (Shard shard : shards) {
                assertVersion(index, 4, "_only_nodes:" + shard.getNode().getNodeName(), finalVersionForDoc4);
                assertCount(index, "_only_nodes:" + shard.getNode().getNodeName(), 4);
            }

            logger.info("setting number of replicas to 1");
            updateIndexSetting(index, Settings.builder().put("index.number_of_replicas", 1));
            ensureGreen();
            nUpdates = randomIntBetween(minUpdates, maxUpdates);
            logger.info("indexing doc with [{}] concurrent updates after setting number of replicas to 1", nUpdates);
            final int finalVersionForDoc5 = indexDocWithConcurrentUpdates(index, 5, nUpdates);
            assertOK(client().performRequest("POST", index + "/_refresh"));
            shards = buildShards(nodes, newNodeClient);
            for (Shard shard : shards) {
                assertVersion(index, 5, "_only_nodes:" + shard.getNode().getNodeName(), finalVersionForDoc5);
                assertCount(index, "_only_nodes:" + shard.getNode().getNodeName(), 5);
            }
            // the number of documents on the primary and on the recovered replica should match the number of indexed documents
            assertCount(index, "_primary", 5);
            assertCount(index, "_replica", 5);
        }
    }

    public void testSeqNoCheckpoints() throws Exception {
        Nodes nodes = buildNodeAndVersions();
        assumeFalse("new nodes is empty", nodes.getNewNodes().isEmpty());
        logger.info("cluster discovered: {}", nodes.toString());
        final List<String> bwcNamesList = nodes.getBWCNodes().stream().map(Node::getNodeName).collect(Collectors.toList());
        final String bwcNames = bwcNamesList.stream().collect(Collectors.joining(","));
        Settings.Builder settings = Settings.builder()
            .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 2)
            .put("index.routing.allocation.include._name", bwcNames);

        final boolean checkGlobalCheckpoints = nodes.getMaster().getVersion().onOrAfter(Version.V_6_0_0_alpha1_UNRELEASED);
        logger.info("master version is [{}], global checkpoints will be [{}]", nodes.getMaster().getVersion(),
            checkGlobalCheckpoints ? "checked" : "not be checked");
        if (checkGlobalCheckpoints) {
            settings.put(IndexSettings.INDEX_SEQ_NO_CHECKPOINT_SYNC_INTERVAL.getKey(), "100ms");
        }
        final String index = "test";
        createIndex(index, settings.build());
        try (RestClient newNodeClient = buildClient(restClientSettings(),
            nodes.getNewNodes().stream().map(Node::getPublishAddress).toArray(HttpHost[]::new))) {
            int numDocs = 0;
            final int numberOfInitialDocs = 1 + randomInt(5);
            logger.info("indexing [{}] docs initially", numberOfInitialDocs);
            numDocs += indexDocs(index, 0, numberOfInitialDocs);
            assertSeqNoOnShards(nodes, checkGlobalCheckpoints, 0, newNodeClient);
            logger.info("allowing shards on all nodes");
            updateIndexSetting(index, Settings.builder().putNull("index.routing.allocation.include._name"));
            ensureGreen();
            assertOK(client().performRequest("POST", index + "/_refresh"));
            for (final String bwcName : bwcNamesList) {
                assertCount(index, "_only_nodes:" + bwcName, numDocs);
            }
            final int numberOfDocsAfterAllowingShardsOnAllNodes = 1 + randomInt(5);
            logger.info("indexing [{}] docs after allowing shards on all nodes", numberOfDocsAfterAllowingShardsOnAllNodes);
            numDocs += indexDocs(index, numDocs, numberOfDocsAfterAllowingShardsOnAllNodes);
            assertSeqNoOnShards(nodes, checkGlobalCheckpoints, 0, newNodeClient);
            Shard primary = buildShards(nodes, newNodeClient).stream().filter(Shard::isPrimary).findFirst().get();
            logger.info("moving primary to new node by excluding {}", primary.getNode().getNodeName());
            updateIndexSetting(index, Settings.builder().put("index.routing.allocation.exclude._name", primary.getNode().getNodeName()));
            ensureGreen();
            int numDocsOnNewPrimary = 0;
            final int numberOfDocsAfterMovingPrimary = 1 + randomInt(5);
            logger.info("indexing [{}] docs after moving primary", numberOfDocsAfterMovingPrimary);
            numDocsOnNewPrimary += indexDocs(index, numDocs, numberOfDocsAfterMovingPrimary);
            numDocs += numberOfDocsAfterMovingPrimary;
            assertSeqNoOnShards(nodes, checkGlobalCheckpoints, numDocsOnNewPrimary, newNodeClient);
            /*
             * Dropping the number of replicas to zero, and then increasing it to one triggers a recovery thus exercising any BWC-logic in
             * the recovery code.
             */
            logger.info("setting number of replicas to 0");
            updateIndexSetting(index, Settings.builder().put("index.number_of_replicas", 0));
            final int numberOfDocsAfterDroppingReplicas = 1 + randomInt(5);
            logger.info("indexing [{}] docs after setting number of replicas to 0", numberOfDocsAfterDroppingReplicas);
            numDocsOnNewPrimary += indexDocs(index, numDocs, numberOfDocsAfterDroppingReplicas);
            numDocs += numberOfDocsAfterDroppingReplicas;
            logger.info("setting number of replicas to 1");
            updateIndexSetting(index, Settings.builder().put("index.number_of_replicas", 1));
            ensureGreen();
            assertOK(client().performRequest("POST", index + "/_refresh"));
            // the number of documents on the primary and on the recovered replica should match the number of indexed documents
            assertCount(index, "_primary", numDocs);
            assertCount(index, "_replica", numDocs);
            assertSeqNoOnShards(nodes, checkGlobalCheckpoints, numDocsOnNewPrimary, newNodeClient);
        }
    }

    private void assertCount(final String index, final String preference, final int expectedCount) throws IOException {
        final Response response = client().performRequest("GET", index + "/_count", Collections.singletonMap("preference", preference));
        assertOK(response);
        final int actualCount = Integer.parseInt(objectPath(response).evaluate("count").toString());
        assertThat(actualCount, equalTo(expectedCount));
    }

    private void assertVersion(final String index, final int docId, final String preference, final int expectedVersion) throws IOException {
        final Response response = client().performRequest("GET", index + "/test/" + docId,
                Collections.singletonMap("preference", preference));
        assertOK(response);
        final int actualVersion = Integer.parseInt(objectPath(response).evaluate("_version").toString());
        assertThat("version mismatch for doc [" + docId + "] preference [" + preference + "]", actualVersion, equalTo(expectedVersion));
    }

    private void assertSeqNoOnShards(Nodes nodes, boolean checkGlobalCheckpoints, int numDocs, RestClient client) throws Exception {
        assertBusy(() -> {
            try {
                List<Shard> shards = buildShards(nodes, client);
                Shard primaryShard = shards.stream().filter(Shard::isPrimary).findFirst().get();
                assertNotNull("failed to find primary shard", primaryShard);
                final long expectedGlobalCkp;
                final long expectMaxSeqNo;
                logger.info("primary resolved to node {}", primaryShard.getNode());
                if (primaryShard.getNode().getVersion().onOrAfter(Version.V_6_0_0_alpha1_UNRELEASED)) {
                    expectMaxSeqNo = numDocs - 1;
                    expectedGlobalCkp = numDocs - 1;
                } else {
                    expectedGlobalCkp = SequenceNumbersService.UNASSIGNED_SEQ_NO;
                    expectMaxSeqNo = SequenceNumbersService.NO_OPS_PERFORMED;
                }
                for (Shard shard : shards) {
                    if (shard.getNode().getVersion().onOrAfter(Version.V_6_0_0_alpha1_UNRELEASED)) {
                        final SeqNoStats seqNoStats = shard.getSeqNoStats();
                        logger.info("stats for {}, primary [{}]: [{}]", shard.getNode(), shard.isPrimary(), seqNoStats);
                        assertThat("max_seq no on " + shard.getNode() + " is wrong", seqNoStats.getMaxSeqNo(), equalTo(expectMaxSeqNo));
                        assertThat("localCheckpoint no on " + shard.getNode() + " is wrong",
                            seqNoStats.getLocalCheckpoint(), equalTo(expectMaxSeqNo));
                        if (checkGlobalCheckpoints) {
                            assertThat("globalCheckpoint no on " + shard.getNode() + " is wrong",
                                seqNoStats.getGlobalCheckpoint(), equalTo(expectedGlobalCkp));
                        }
                    } else {
                        logger.info("skipping seq no test on {}", shard.getNode());
                    }
                }
            } catch (IOException e) {
                throw new AssertionError("unexpected io exception", e);
            }
        });
    }

    private List<Shard> buildShards(Nodes nodes, RestClient client) throws IOException {
        Response response = client.performRequest("GET", "test/_stats", singletonMap("level", "shards"));
        List<Object> shardStats = objectPath(response).evaluate("indices.test.shards.0");
        ArrayList<Shard> shards = new ArrayList<>();
        for (Object shard : shardStats) {
            final String nodeId = ObjectPath.evaluate(shard, "routing.node");
            final Boolean primary = ObjectPath.evaluate(shard, "routing.primary");
            final Node node = nodes.getSafe(nodeId);
            final SeqNoStats seqNoStats;
            if (node.getVersion().onOrAfter(Version.V_6_0_0_alpha1_UNRELEASED)) {
                Integer maxSeqNo = ObjectPath.evaluate(shard, "seq_no.max_seq_no");
                Integer localCheckpoint = ObjectPath.evaluate(shard, "seq_no.local_checkpoint");
                Integer globalCheckpoint = ObjectPath.evaluate(shard, "seq_no.global_checkpoint");
                seqNoStats = new SeqNoStats(maxSeqNo, localCheckpoint, globalCheckpoint);
            } else {
                seqNoStats = null;
            }
            shards.add(new Shard(node, primary, seqNoStats));
        }
        return shards;
    }

    private Nodes buildNodeAndVersions() throws IOException {
        Response response = client().performRequest("GET", "_nodes");
        ObjectPath objectPath = objectPath(response);
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
        nodes.setMasterNodeId(objectPath(response).evaluate("master_node"));
        return nodes;
    }

    final class Nodes extends HashMap<String, Node> {

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
                '}';
        }
    }

    final class Shard {
        private final Node node;
        private final boolean Primary;
        private final SeqNoStats seqNoStats;

        Shard(Node node, boolean primary, SeqNoStats seqNoStats) {
            this.node = node;
            Primary = primary;
            this.seqNoStats = seqNoStats;
        }

        public Node getNode() {
            return node;
        }

        public boolean isPrimary() {
            return Primary;
        }

        public SeqNoStats getSeqNoStats() {
            return seqNoStats;
        }

        @Override
        public String toString() {
            return "Shard{" +
                "node=" + node +
                ", Primary=" + Primary +
                ", seqNoStats=" + seqNoStats +
                '}';
        }
    }
}

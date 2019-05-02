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
package org.elasticsearch.upgrades;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.test.rest.yaml.ObjectPath;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomAsciiOfLength;
import static org.elasticsearch.cluster.routing.UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

/**
 * In depth testing of the recovery mechanism during a rolling restart.
 */
public class RecoveryIT extends AbstractRollingTestCase {

    public void testHistoryUUIDIsGenerated() throws Exception {
        final String index = "index_history_uuid";
        if (CLUSTER_TYPE == ClusterType.OLD) {
            Settings.Builder settings = Settings.builder()
                .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
                // if the node with the replica is the first to be restarted, while a replica is still recovering
                // then delayed allocation will kick in. When the node comes back, the master will search for a copy
                // but the recovering copy will be seen as invalid and the cluster health won't return to GREEN
                // before timing out
                .put(INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "100ms")
                // if this index can be allocated to any testing nodes, the rebalance allocation decider may relocate
                // this index. if the relocation from 5.x to 6.x, we will have two different history_uuids because
                // history_uuid is bootstrapped twice by two different 6.x nodes: a replica and a relocating primary.
                .put("index.routing.allocation.include._name", "*node-0,*node-1");
            createIndex(index, settings.build());
        } else if (CLUSTER_TYPE == ClusterType.UPGRADED) {
            ensureGreen(index);
            Request shardStatsRequest = new Request("GET", index + "/_stats");
            shardStatsRequest.addParameter("level", "shards");
            Response response = client().performRequest(shardStatsRequest);
            ObjectPath objectPath = ObjectPath.createFromResponse(response);
            List<Object> shardStats = objectPath.evaluate("indices." + index + ".shards.0");
            assertThat(shardStats, hasSize(2));
            String expectHistoryUUID = null;
            for (int shard = 0; shard < 2; shard++) {
                String nodeID = objectPath.evaluate("indices." + index + ".shards.0." + shard + ".routing.node");
                String historyUUID = objectPath.evaluate("indices." + index + ".shards.0." + shard + ".commit.user_data.history_uuid");
                assertThat("no history uuid found for shard on " + nodeID, historyUUID, notNullValue());
                if (expectHistoryUUID == null) {
                    expectHistoryUUID = historyUUID;
                } else {
                    assertThat("different history uuid found for shard on " + nodeID, historyUUID, equalTo(expectHistoryUUID));
                }
            }
        }
    }

    private int indexDocs(String index, final int idStart, final int numDocs) throws IOException {
        for (int i = 0; i < numDocs; i++) {
            final int id = idStart + i;
            Request indexDoc = new Request("PUT", index + "/test/" + id);
            indexDoc.setJsonEntity("{\"test\": \"test_" + randomAsciiOfLength(2) + "\"}");
            client().performRequest(indexDoc);
        }
        return numDocs;
    }

    private Future<Void> asyncIndexDocs(String index, final int idStart, final int numDocs) throws IOException {
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        Thread background = new Thread(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                future.onFailure(e);
            }

            @Override
            protected void doRun() throws Exception {
                indexDocs(index, idStart, numDocs);
                future.onResponse(null);
            }
        });
        background.start();
        return future;
    }

    public void testRecoveryWithConcurrentIndexing() throws Exception {
        final String index = "recovery_with_concurrent_indexing";
        Response response = client().performRequest(new Request("GET", "_nodes"));
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        final Map<String, Object> nodeMap = objectPath.evaluate("nodes");
        List<String> nodes = new ArrayList<>(nodeMap.keySet());

        switch (CLUSTER_TYPE) {
            case OLD:
                Settings.Builder settings = Settings.builder()
                    .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                    .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 2)
                    // if the node with the replica is the first to be restarted, while a replica is still recovering
                    // then delayed allocation will kick in. When the node comes back, the master will search for a copy
                    // but the recovering copy will be seen as invalid and the cluster health won't return to GREEN
                    // before timing out
                    .put(INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "100ms")
                    .put(SETTING_ALLOCATION_MAX_RETRY.getKey(), "0"); // fail faster
                createIndex(index, settings.build());
                indexDocs(index, 0, 10);
                ensureGreen(index);
                // make sure that we can index while the replicas are recovering
                updateIndexSettings(index, Settings.builder().put(INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "primaries"));
                break;
            case MIXED:
                updateIndexSettings(index, Settings.builder().put(INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), (String)null));
                asyncIndexDocs(index, 10, 50).get();
                ensureGreen(index);
                client().performRequest(new Request("POST", index + "/_refresh"));
                assertCount(index, "_only_nodes:" + nodes.get(0), 60);
                assertCount(index, "_only_nodes:" + nodes.get(1), 60);
                assertCount(index, "_only_nodes:" + nodes.get(2), 60);
                // make sure that we can index while the replicas are recovering
                updateIndexSettings(index, Settings.builder().put(INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "primaries"));
                break;
            case UPGRADED:
                updateIndexSettings(index, Settings.builder().put(INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), (String)null));
                asyncIndexDocs(index, 60, 45).get();
                ensureGreen(index);
                client().performRequest(new Request("POST", index + "/_refresh"));
                assertCount(index, "_only_nodes:" + nodes.get(0), 105);
                assertCount(index, "_only_nodes:" + nodes.get(1), 105);
                assertCount(index, "_only_nodes:" + nodes.get(2), 105);
                break;
            default:
                throw new IllegalStateException("unknown type " + CLUSTER_TYPE);
        }
    }

    private void assertDocCountOnAllCopies(String index, int expectedCount) throws Exception {
        assertBusy(() -> {
            Map<String, ?> state = entityAsMap(client().performRequest(new Request("GET", "/_cluster/state")));
            String xpath = "routing_table.indices." + index + ".shards.0.node";
            @SuppressWarnings("unchecked") List<String> assignedNodes = (List<String>) XContentMapValues.extractValue(xpath, state);
            assertNotNull(state.toString(), assignedNodes);
            for (String assignedNode : assignedNodes) {
                try {
                    assertCount(index, "_only_nodes:" + assignedNode, expectedCount);
                } catch (ResponseException e) {
                    if (e.getMessage().contains("no data nodes with criteria [" + assignedNode + "found for shard: [" + index + "][0]")) {
                        throw new AssertionError(e); // shard is relocating - ask assert busy to retry
                    }
                    throw e;
                }
            }
        });
    }

    private void assertCount(final String index, final String preference, final int expectedCount) throws IOException {
        final Request request = new Request("GET", index + "/_count");
        request.addParameter("preference", preference);
        final Response response = client().performRequest(request);
        final int actualCount = Integer.parseInt(ObjectPath.createFromResponse(response).evaluate("count").toString());
        assertThat("preference [" + preference + "]", actualCount, equalTo(expectedCount));
    }


    private String getNodeId(Predicate<Version> versionPredicate) throws IOException {
        Response response = client().performRequest(new Request("GET", "_nodes"));
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        Map<String, Object> nodesAsMap = objectPath.evaluate("nodes");
        for (String id : nodesAsMap.keySet()) {
            Version version = Version.fromString(objectPath.evaluate("nodes." + id + ".version"));
            if (versionPredicate.test(version)) {
                return id;
            }
        }
        return null;
    }

    private String getNodeIdByName(String nodeName) {
        try {
            Response response = client().performRequest(new Request("GET", "_nodes"));
            ObjectPath objectPath = ObjectPath.createFromResponse(response);
            Map<String, Object> nodesAsMap = objectPath.evaluate("nodes");
            for (String id : nodesAsMap.keySet()) {
                if (objectPath.<String>evaluate("nodes." + id + ".name").equals(nodeName)) {
                    return id;
                }
            }
            throw new IllegalArgumentException("no node found for node name[" + nodeName + "]");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void testRelocationWithConcurrentIndexing() throws Exception {
        final String index = "relocation_with_concurrent_indexing";
        switch (CLUSTER_TYPE) {
            case OLD:
                Settings.Builder settings = Settings.builder()
                    .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                    .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 2)
                    // if the node with the replica is the first to be restarted, while a replica is still recovering
                    // then delayed allocation will kick in. When the node comes back, the master will search for a copy
                    // but the recovering copy will be seen as invalid and the cluster health won't return to GREEN
                    // before timing out
                    .put(INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "100ms")
                    .put(SETTING_ALLOCATION_MAX_RETRY.getKey(), "0"); // fail faster
                createIndex(index, settings.build());
                indexDocs(index, 0, 10);
                ensureGreen(index);
                // make sure that no shards are allocated, so we can make sure the primary stays on the old node (when one
                // node stops, we lose the master too, so a replica will not be promoted)
                updateIndexSettings(index, Settings.builder().put(INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "none"));
                break;
            case MIXED:
                final String newNode = getNodeId(v -> v.equals(Version.CURRENT));
                final String oldNode = getNodeId(v -> v.before(Version.CURRENT));
                // remove the replica and guaranteed the primary is placed on the old node
                updateIndexSettings(index, Settings.builder()
                    .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                    .put(INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), (String)null)
                    .put("index.routing.allocation.include._id", oldNode)
                );
                ensureGreen(index); // wait for the primary to be assigned
                ensureNoInitializingShards(); // wait for all other shard activity to finish
                updateIndexSettings(index, Settings.builder().put("index.routing.allocation.include._id", newNode));
                asyncIndexDocs(index, 10, 50).get();
                ensureIndexAssignedToNodeIds(index, Collections.singleton(newNode));
                ensureGreen(index);
                client().performRequest(new Request("POST", index + "/_refresh"));
                assertCount(index, "_primary", 60);
                break;
            case UPGRADED:
                updateIndexSettings(index, Settings.builder()
                    .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 2)
                    .put("index.routing.allocation.include._id", (String)null)
                );
                asyncIndexDocs(index, 60, 45).get();
                ensureGreen(index);
                client().performRequest(new Request("POST", index + "/_refresh"));
                Response response = client().performRequest(new Request("GET", "_nodes"));
                ObjectPath objectPath = ObjectPath.createFromResponse(response);
                final Map<String, Object> nodeMap = objectPath.evaluate("nodes");
                List<String> nodes = new ArrayList<>(nodeMap.keySet());

                assertCount(index, "_only_nodes:" + nodes.get(0), 105);
                assertCount(index, "_only_nodes:" + nodes.get(1), 105);
                assertCount(index, "_only_nodes:" + nodes.get(2), 105);
                break;
            default:
                throw new IllegalStateException("unknown type " + CLUSTER_TYPE);
        }
    }

    public void testSearchGeoPoints() throws Exception {
        final String index = "geo_index";
        if (CLUSTER_TYPE == ClusterType.OLD) {
            Settings.Builder settings = Settings.builder()
                .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 2)
                // if the node with the replica is the first to be restarted, while a replica is still recovering
                // then delayed allocation will kick in. When the node comes back, the master will search for a copy
                // but the recovering copy will be seen as invalid and the cluster health won't return to GREEN
                // before timing out
                .put(INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "100ms");
            createIndex(index, settings.build(), "\"doc\": {\"properties\": {\"location\": {\"type\": \"geo_point\"}}}");
            ensureGreen(index);
        } else if (CLUSTER_TYPE == ClusterType.MIXED) {
            ensureGreen(index);
            String requestBody = "{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"should\": [\n" +
                "        {\n" +
                "          \"geo_distance\": {\n" +
                "            \"distance\": \"1000km\",\n" +
                "            \"location\": {\n" +
                "              \"lat\": 40,\n" +
                "              \"lon\": -70\n" +
                "            }\n" +
                "          }\n" +
                "        },\n" +
                "        {\"match_all\": {}}\n" +
                "      ]\n" +
                "    }\n" +
                "  }\n" +
                "}";

            // we need to make sure that requests are routed from a new node to the old node so we are sending the request a few times
            for (int i = 0; i < 10; i++) {
                Request request = new Request("GET", index + "/_search");
                request.setJsonEntity(requestBody);
                // Make sure we only send this request to old nodes
                request.addParameter("preference", "_only_nodes:gen:old");
                client().performRequest(request);
            }
        }
    }

    public void testRecoverSyncedFlushIndex() throws Exception {
        final String index = "recover_synced_flush_index";
        if (CLUSTER_TYPE == ClusterType.OLD) {
            Settings.Builder settings = Settings.builder()
                .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
                // if the node with the replica is the first to be restarted, while a replica is still recovering
                // then delayed allocation will kick in. When the node comes back, the master will search for a copy
                // but the recovering copy will be seen as invalid and the cluster health won't return to GREEN
                // before timing out
                .put(INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "100ms")
                .put(SETTING_ALLOCATION_MAX_RETRY.getKey(), "0"); // fail faster
            createIndex(index, settings.build());
            indexDocs(index, 0, randomInt(5));
            syncedFlush(index);
        }
        ensureGreen(index);
    }

    public void testRecoveryWithSoftDeletes() throws Exception {
        final String index = "recover_with_soft_deletes";
        if (CLUSTER_TYPE == ClusterType.OLD) {
            Settings.Builder settings = Settings.builder()
                .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
                // if the node with the replica is the first to be restarted, while a replica is still recovering
                // then delayed allocation will kick in. When the node comes back, the master will search for a copy
                // but the recovering copy will be seen as invalid and the cluster health won't return to GREEN
                // before timing out
                .put(INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "100ms")
                .put(SETTING_ALLOCATION_MAX_RETRY.getKey(), "0"); // fail faster
            if (getNodeId(v -> v.onOrAfter(Version.V_6_5_0)) != null) {
                settings.put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true);
            }
            createIndex(index, settings.build());
            int numDocs = randomInt(10);
            indexDocs(index, 0, numDocs);
            if (randomBoolean()) {
                client().performRequest(new Request("POST", "/" + index + "/_flush"));
            }
            for (int i = 0; i < numDocs; i++) {
                if (randomBoolean()) {
                    indexDocs(index, i, 1); // update
                } else if (randomBoolean()) {
                    client().performRequest(new Request("DELETE", index + "/test/" + i));
                }
            }
        }
        ensureGreen(index);
    }

    /**
     * This test ensures peer recovery won't get stuck in a situation where the recovery target and recovery source
     * have an identical sync id but different local checkpoint in the commit (in particular the target does not have
     * sequence number yet). This is possible if the primary is on 6.x while the replica was on 5.x and some write
     * operations with sequence numbers have taken place.
     */
    public void testRecoveryWithSyncIdVerifySeqNoStats() throws Exception {
        final String index = "recovery_sync_id_seq_no";
        if (CLUSTER_TYPE == ClusterType.MIXED && firstMixedRound) {
            // allocate the primary on node-1 and replica on node-2
            Settings.Builder settings = Settings.builder()
                .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
                .put("index.routing.allocation.include._name", "node-1");
            createIndex(index, settings.build());
            updateIndexSettings(index, Settings.builder().put("index.routing.allocation.include._name", "node-1,node-2"));
            ensureIndexAssignedToNodeIds(index,
                Sets.newHashSet("node-1", "node-2").stream().map(this::getNodeIdByName).collect(Collectors.toSet()));
            ensureGreen(index);
            // relocate the primary from node-1 to upgraded-node-0 so we can index documents with sequence numbers
            // and prevent allocating the replica on upgraded-node-2 until we trim translog on the primary so recovery won't replay ops
            updateIndexSettings(index, Settings.builder().put("index.routing.allocation.include._name", "upgraded-node-0,node-2"));
            ensureIndexAssignedToNodeIds(index,
                Sets.newHashSet("upgraded-node-0", "node-2").stream().map(this::getNodeIdByName).collect(Collectors.toSet()));
            ensureGreen(index);
            indexDocs(index, 0, 10);
            syncedFlush(index);
        } else if (CLUSTER_TYPE == ClusterType.UPGRADED) {
            assertDocCountOnAllCopies(index, 10);
            final int moreDocs;
            if (randomBoolean()) {
                updateIndexSettings(index, Settings.builder()
                    .put(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey(), "-1")
                    .put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), "-1")
                    .put(IndexSettings.INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE_SETTING.getKey(), "256b"));
                // index more documents to roll translog so we can trim translog
                moreDocs = randomIntBetween(50, 100);
                indexDocs(index, 10, moreDocs);
                assertBusy(() -> {
                    Map<String, ?> stats = entityAsMap(client().performRequest(new Request("GET", index + "/_stats?level=shards")));
                    Integer translogOps = (Integer) XContentMapValues.extractValue("_all.primaries.translog.operations", stats);
                    assertThat(translogOps, equalTo(moreDocs));
                });
            } else {
                moreDocs = 0;
            }
            updateIndexSettings(index, Settings.builder().put("index.routing.allocation.include._name", "upgraded-node-0,upgraded-node-2"));
            ensureIndexAssignedToNodeIds(index,
                Sets.newHashSet("upgraded-node-0", "upgraded-node-2").stream().map(this::getNodeIdByName).collect(Collectors.toSet()));
            ensureGreen(index);
            assertPeerRecoveredFiles("peer recovery must ignore syncId when seq_nos mismatched", index, "upgraded-node-2", greaterThan(0));
            assertDocCountOnAllCopies(index, 10 + moreDocs);
        }
    }

    public void testRollingUpgradeWithSyncedFlushSkipCopyingFiles() throws Exception {
        final String index = "rolling_upgrade_with_synced_flush";
        if (CLUSTER_TYPE == ClusterType.OLD) {
            Settings.Builder settings = Settings.builder()
                .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), randomIntBetween(1, 2));
            if (Version.CURRENT.before(Version.V_6_0_0)) {
                // Disable rebalancing to prevent the primary from relocating to 6.x node while replicas are still on 5.x.
                // The relocating scenario is tested in testRecoveryWithSyncIdVerifySeqNoStats where peer recovery ignores
                // syncId and performs a file-based recovery.
                settings.put("index.routing.rebalance.enable", "none");
            }
            createIndex(index, settings.build());
            ensureGreen(index);
            indexDocs(index, 0, 40);
            syncedFlush(index);
        } else if (CLUSTER_TYPE == ClusterType.MIXED) {
            ensureGreen(index);
            if (firstMixedRound) {
                assertPeerRecoveredFiles("peer recovery with syncId should not copy files", index, "upgraded-node-0", equalTo(0));
                assertDocCountOnAllCopies(index, 40);
                indexDocs(index, 40, 50);
            } else {
                assertPeerRecoveredFiles("peer recovery with syncId should not copy files", index, "upgraded-node-1", equalTo(0));
                assertDocCountOnAllCopies(index, 90);
                indexDocs(index, 90, 60);
            }
            syncedFlush(index);
        } else {
            ensureGreen(index);
            assertPeerRecoveredFiles("peer recovery with syncId should not copy files", index, "upgraded-node-2", equalTo(0));
            assertDocCountOnAllCopies(index, 150);
        }
    }

    private void syncedFlush(String index) throws Exception {
        ensureGlobalCheckpointSynced(index);
        // We have to spin synced-flush requests here because we fire the global checkpoint sync for the last write operation.
        // A synced-flush request considers the global checkpoint sync as an going operation because it acquires a shard permit.
        assertBusy(() -> {
            try {
                Response resp = client().performRequest(new Request("POST", index + "/_flush/synced"));
                Map<String, Object> result = ObjectPath.createFromResponse(resp).evaluate("_shards");
                assertThat(result.get("failed"), equalTo(0));
            } catch (ResponseException ex) {
                throw new AssertionError(ex); // cause assert busy to retry
            }
        });
    }

    @SuppressWarnings("unchecked")
    private void assertPeerRecoveredFiles(String reason, String index, String targetNode, Matcher<Integer> sizeMatcher) throws IOException {
        Response indicesStats = client().performRequest(new Request("GET", index + "/_stats?level=shards"));
        Map<?, ?> recoveryStats = entityAsMap(client().performRequest(new Request("GET", index + "/_recovery")));
        List<Map<?, ?>> shards = (List<Map<?, ?>>) XContentMapValues.extractValue(index + "." + "shards", recoveryStats);
        for (Map<?, ?> shard : shards) {
            if (Objects.equals(XContentMapValues.extractValue("type", shard), "PEER")) {
                if (Objects.equals(XContentMapValues.extractValue("target.name", shard), targetNode)) {
                    Integer recoveredFileSize = (Integer) XContentMapValues.extractValue("index.files.recovered", shard);
                    assertThat(reason + "target node [" + targetNode + "] recovery stats [" + recoveryStats + "]" +
                        " indices stats [" + EntityUtils.toString(indicesStats.getEntity()) + "]", recoveredFileSize, sizeMatcher);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void ensureGlobalCheckpointSynced(String index) throws Exception {
        // we need to send the stats request to a 6.0+ node which has seq_no_stats;
        // otherwise seq_no_stats will be stripped out if it is [de]serialized on a node before 6.0.
        Node[] upgradedNodes = client().getNodes().stream()
            .filter(node -> Version.fromString(node.getVersion()).onOrAfter(Version.V_6_0_0)).toArray(Node[]::new);
        if (upgradedNodes.length == 0) {
            assert CLUSTER_TYPE == ClusterType.OLD : CLUSTER_TYPE;
            return;
        }
        RestClientBuilder clientBuilder = RestClient.builder(upgradedNodes);
        configureClient(clientBuilder, restClientSettings());
        try (RestClient client = clientBuilder.build()) {
            assertBusy(() -> {
                Map<?, ?> stats = entityAsMap(client.performRequest(new Request("GET", index + "/_stats?level=shards")));
                List<Map<?, ?>> shardStats = (List<Map<?, ?>>) XContentMapValues.extractValue("indices." + index + ".shards.0", stats);
                List<? extends Map<?, ?>> seqNoStats = shardStats.stream()
                    .map(shard -> (Map<?, ?>) XContentMapValues.extractValue("seq_no", shard))
                    .filter(Objects::nonNull).collect(Collectors.toList());
                for (Map<?, ?> seqNoStat : seqNoStats) {
                    long globalCheckpoint = ((Number) XContentMapValues.extractValue("global_checkpoint", seqNoStat)).longValue();
                    long localCheckpoint = ((Number) XContentMapValues.extractValue("local_checkpoint", seqNoStat)).longValue();
                    if (globalCheckpoint == SequenceNumbers.UNASSIGNED_SEQ_NO) {
                        assertThat(stats.toString(), localCheckpoint, equalTo(SequenceNumbers.NO_OPS_PERFORMED));
                    } else {
                        assertThat(stats.toString(), globalCheckpoint, equalTo(localCheckpoint));
                    }
                }
            }, 60, TimeUnit.SECONDS);
        }
    }

    /**
     * This method should be called after changing the allocation filter to ensure the relocation has actually occurred;
     * otherwise subsequent calls to ensure green can return true although shards haven't moved around yet due to allocation throttling.
     */
    private void ensureIndexAssignedToNodeIds(String index, Set<String> expectedNodeIds) throws Exception {
        assertBusy(() -> {
            Map<String, ?> state = entityAsMap(client().performRequest(new Request("GET", "/_cluster/state")));
            String xpath = "routing_table.indices." + index + ".shards.0.node";
            @SuppressWarnings("unchecked") List<String> assignedNodes = (List<String>) XContentMapValues.extractValue(xpath, state);
            assertNotNull(state.toString(), assignedNodes);
            assertThat(state.toString(), new HashSet<>(assignedNodes), equalTo(expectedNodeIds));
        }, 60, TimeUnit.SECONDS);
    }
}

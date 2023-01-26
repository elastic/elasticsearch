/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.upgrades;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MetadataIndexStateService;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.rest.ObjectPath;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomAsciiLettersOfLength;
import static org.elasticsearch.cluster.routing.UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY;
import static org.elasticsearch.upgrades.UpgradeWithOldIndexSettingsIT.updateIndexSettingsPermittingSlowlogDeprecationWarning;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;

/**
 * In depth testing of the recovery mechanism during a rolling restart.
 */
public class RecoveryIT extends AbstractRollingTestCase {

    private static String CLUSTER_NAME = System.getProperty("tests.clustername");

    public void testHistoryUUIDIsGenerated() throws Exception {
        final String index = "index_history_uuid";
        if (CLUSTER_TYPE == ClusterType.OLD) {
            Settings.Builder settings = Settings.builder()
                .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
                // if the node with the replica is the first to be restarted, while a replica is still recovering
                // then delayed allocation will kick in. When the node comes back, the master will search for a copy
                // but the recovering copy will be seen as invalid and the cluster health won't return to GREEN
                // before timing out
                .put(INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "100ms");
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
            Request indexDoc = new Request("PUT", index + "/_doc/" + id);
            indexDoc.setJsonEntity("{\"test\": \"test_" + randomAsciiLettersOfLength(2) + "\"}");
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
            case OLD -> {
                Settings.Builder settings = Settings.builder()
                    .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                    .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 2)
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
            }
            case MIXED -> {
                updateIndexSettings(index, Settings.builder().put(INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), (String) null));
                asyncIndexDocs(index, 10, 50).get();
                ensureGreen(index);
                client().performRequest(new Request("POST", index + "/_refresh"));
                assertCount(index, "_only_nodes:" + nodes.get(0), 60);
                assertCount(index, "_only_nodes:" + nodes.get(1), 60);
                assertCount(index, "_only_nodes:" + nodes.get(2), 60);
                // make sure that we can index while the replicas are recovering
                updateIndexSettings(index, Settings.builder().put(INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "primaries"));
            }
            case UPGRADED -> {
                updateIndexSettings(index, Settings.builder().put(INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), (String) null));
                asyncIndexDocs(index, 60, 45).get();
                ensureGreen(index);
                client().performRequest(new Request("POST", index + "/_refresh"));
                assertCount(index, "_only_nodes:" + nodes.get(0), 105);
                assertCount(index, "_only_nodes:" + nodes.get(1), 105);
                assertCount(index, "_only_nodes:" + nodes.get(2), 105);
            }
            default -> throw new IllegalStateException("unknown type " + CLUSTER_TYPE);
        }
    }

    private void assertCount(final String index, final String preference, final int expectedCount) throws IOException {
        final int actualDocs;
        try {
            final Request request = new Request("GET", index + "/_count");
            if (preference != null) {
                request.addParameter("preference", preference);
            }
            final Response response = client().performRequest(request);
            actualDocs = Integer.parseInt(ObjectPath.createFromResponse(response).evaluate("count").toString());
        } catch (ResponseException e) {
            try {
                final Response recoveryStateResponse = client().performRequest(new Request("GET", index + "/_recovery"));
                fail(
                    "failed to get doc count for index ["
                        + index
                        + "] with preference ["
                        + preference
                        + "]"
                        + " response ["
                        + e
                        + "]"
                        + " recovery ["
                        + EntityUtils.toString(recoveryStateResponse.getEntity())
                        + "]"
                );
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw e;
        }
        assertThat("preference [" + preference + "]", actualDocs, equalTo(expectedCount));
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

    public void testRelocationWithConcurrentIndexing() throws Exception {
        final String index = "relocation_with_concurrent_indexing";
        switch (CLUSTER_TYPE) {
            case OLD -> {
                Settings.Builder settings = Settings.builder()
                    .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                    .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 2)
                    // if the node with the replica is the first to be restarted, while a replica is still recovering
                    // then delayed allocation will kick in. When the node comes back, the master will search for a copy
                    // but the recovering copy will be seen as invalid and the cluster health won't return to GREEN
                    // before timing out
                    .put(INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "100ms")
                    .put("index.routing.allocation.include._tier_preference", "")
                    .put(SETTING_ALLOCATION_MAX_RETRY.getKey(), "0"); // fail faster
                createIndex(index, settings.build());
                indexDocs(index, 0, 10);
                ensureGreen(index);
                // make sure that no shards are allocated, so we can make sure the primary stays on the old node (when one
                // node stops, we lose the master too, so a replica will not be promoted)
                updateIndexSettings(index, Settings.builder().put(INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "none"));
            }
            case MIXED -> {
                final String newNode = getNodeId(v -> v.equals(Version.CURRENT));
                final String oldNode = getNodeId(v -> v.before(Version.CURRENT));
                // remove the replica and guaranteed the primary is placed on the old node
                updateIndexSettingsPermittingSlowlogDeprecationWarning(
                    index,
                    Settings.builder()
                        .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                        .put(INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), (String) null)
                        .put("index.routing.allocation.include._id", oldNode)
                        .putNull("index.routing.allocation.include._tier_preference")
                );
                ensureGreen(index); // wait for the primary to be assigned
                ensureNoInitializingShards(); // wait for all other shard activity to finish
                updateIndexSettingsPermittingSlowlogDeprecationWarning(
                    index,
                    Settings.builder().put("index.routing.allocation.include._id", newNode)
                );
                asyncIndexDocs(index, 10, 50).get();
                // ensure the relocation from old node to new node has occurred; otherwise ensureGreen can
                // return true even though shards haven't moved to the new node yet (allocation was throttled).
                assertBusy(() -> {
                    Map<String, ?> state = entityAsMap(client().performRequest(new Request("GET", "/_cluster/state")));
                    String xpath = "routing_table.indices." + index + ".shards.0.node";
                    @SuppressWarnings("unchecked")
                    List<String> assignedNodes = (List<String>) XContentMapValues.extractValue(xpath, state);
                    assertNotNull(state.toString(), assignedNodes);
                    assertThat(state.toString(), newNode, in(assignedNodes));
                }, 60, TimeUnit.SECONDS);
                ensureGreen(index);
                client().performRequest(new Request("POST", index + "/_refresh"));
                assertCount(index, "_only_nodes:" + newNode, 60);
            }
            case UPGRADED -> {
                updateIndexSettings(
                    index,
                    Settings.builder()
                        .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 2)
                        .put("index.routing.allocation.include._id", (String) null)
                        .putNull("index.routing.allocation.include._tier_preference")
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
            }
            default -> throw new IllegalStateException("unknown type " + CLUSTER_TYPE);
        }
        if (randomBoolean()) {
            flush(index, randomBoolean());
        }
    }

    public void testRecovery() throws Exception {
        final String index = "test_recovery";
        if (CLUSTER_TYPE == ClusterType.OLD) {
            Settings.Builder settings = Settings.builder()
                .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
                // if the node with the replica is the first to be restarted, while a replica is still recovering
                // then delayed allocation will kick in. When the node comes back, the master will search for a copy
                // but the recovering copy will be seen as invalid and the cluster health won't return to GREEN
                // before timing out
                .put(INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "100ms")
                .put(SETTING_ALLOCATION_MAX_RETRY.getKey(), "0"); // fail faster
            if (minimumNodeVersion().before(Version.V_8_0_0) && randomBoolean()) {
                settings.put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), randomBoolean());
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
                    if (getNodeId(v -> v.onOrAfter(Version.V_7_0_0)) == null) {
                        client().performRequest(new Request("DELETE", index + "/test/" + i));
                    } else {
                        client().performRequest(new Request("DELETE", index + "/_doc/" + i));
                    }
                }
            }
        }
        if (randomBoolean()) {
            flush(index, randomBoolean());
        }
        ensureGreen(index);
    }

    public void testRetentionLeasesEstablishedWhenPromotingPrimary() throws Exception {
        final String index = "recover_and_create_leases_in_promotion";
        if (CLUSTER_TYPE == ClusterType.OLD) {
            Settings.Builder settings = Settings.builder()
                .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), between(1, 5))
                .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), between(1, 2)) // triggers nontrivial promotion
                .put(INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "100ms")
                .put(SETTING_ALLOCATION_MAX_RETRY.getKey(), "0"); // fail faster
            if (minimumNodeVersion().before(Version.V_8_0_0) && randomBoolean()) {
                settings.put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), randomBoolean());
            }
            createIndex(index, settings.build());
            int numDocs = randomInt(10);
            indexDocs(index, 0, numDocs);
            if (randomBoolean()) {
                client().performRequest(new Request("POST", "/" + index + "/_flush"));
            }
        }
        ensureGreen(index);
        ensurePeerRecoveryRetentionLeasesRenewedAndSynced(index);
    }

    public void testRetentionLeasesEstablishedWhenRelocatingPrimary() throws Exception {
        final String index = "recover_and_create_leases_in_relocation";
        switch (CLUSTER_TYPE) {
            case OLD -> {
                Settings.Builder settings = Settings.builder()
                    .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), between(1, 5))
                    .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), between(0, 1))
                    .put(INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "100ms")
                    .put(SETTING_ALLOCATION_MAX_RETRY.getKey(), "0"); // fail faster
                if (minimumNodeVersion().before(Version.V_8_0_0) && randomBoolean()) {
                    settings.put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), randomBoolean());
                }
                createIndex(index, settings.build());
                int numDocs = randomInt(10);
                indexDocs(index, 0, numDocs);
                if (randomBoolean()) {
                    client().performRequest(new Request("POST", "/" + index + "/_flush"));
                }
                ensureGreen(index);
            }
            case MIXED -> {
                // trigger a primary relocation by excluding the last old node with a shard filter
                final Map<?, ?> nodesMap = ObjectPath.createFromResponse(client().performRequest(new Request("GET", "/_nodes")))
                    .evaluate("nodes");
                final List<String> oldNodeNames = new ArrayList<>();
                for (Object nodeDetails : nodesMap.values()) {
                    final Map<?, ?> nodeDetailsMap = (Map<?, ?>) nodeDetails;
                    final String versionString = (String) nodeDetailsMap.get("version");
                    if (versionString.equals(Version.CURRENT.toString()) == false) {
                        oldNodeNames.add((String) nodeDetailsMap.get("name"));
                    }
                }
                if (oldNodeNames.size() == 1) {
                    final String oldNodeName = oldNodeNames.get(0);
                    logger.info("--> excluding index [{}] from node [{}]", index, oldNodeName);
                    final Request putSettingsRequest = new Request("PUT", "/" + index + "/_settings");
                    putSettingsRequest.setJsonEntity("{\"index.routing.allocation.exclude._name\":\"" + oldNodeName + "\"}");
                    assertOK(client().performRequest(putSettingsRequest));
                }
                ensureGreen(index);
                ensurePeerRecoveryRetentionLeasesRenewedAndSynced(index);
            }
            case UPGRADED -> {
                ensureGreen(index);
                ensurePeerRecoveryRetentionLeasesRenewedAndSynced(index);
            }
        }
    }

    /**
     * This test creates an index in the non upgraded cluster and closes it. It then checks that the index
     * is effectively closed and potentially replicated (if the version the index was created on supports
     * the replication of closed indices) during the rolling upgrade.
     */
    public void testRecoveryClosedIndex() throws Exception {
        final String indexName = "closed_index_created_on_old";
        if (CLUSTER_TYPE == ClusterType.OLD) {
            createIndex(
                indexName,
                Settings.builder()
                    .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                    .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
                    // if the node with the replica is the first to be restarted, while a replica is still recovering
                    // then delayed allocation will kick in. When the node comes back, the master will search for a copy
                    // but the recovering copy will be seen as invalid and the cluster health won't return to GREEN
                    // before timing out
                    .put(INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "100ms")
                    .put(SETTING_ALLOCATION_MAX_RETRY.getKey(), "0") // fail faster
                    .build()
            );
            ensureGreen(indexName);
            closeIndex(indexName);
        }

        final Version indexVersionCreated = indexVersionCreated(indexName);
        if (indexVersionCreated.onOrAfter(Version.V_7_2_0)) {
            // index was created on a version that supports the replication of closed indices,
            // so we expect the index to be closed and replicated
            ensureGreen(indexName);
            assertClosedIndex(indexName, true);
        } else {
            assertClosedIndex(indexName, false);
        }
    }

    /**
     * This test creates and closes a new index at every stage of the rolling upgrade. It then checks that the index
     * is effectively closed and potentially replicated if the cluster supports replication of closed indices at the
     * time the index was closed.
     */
    public void testCloseIndexDuringRollingUpgrade() throws Exception {
        final Version minimumNodeVersion = minimumNodeVersion();
        final String indexName = String.join("_", "index", CLUSTER_TYPE.toString(), Integer.toString(minimumNodeVersion.id))
            .toLowerCase(Locale.ROOT);

        if (indexExists(indexName) == false) {
            createIndex(
                indexName,
                Settings.builder()
                    .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                    .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                    .build()
            );
            ensureGreen(indexName);
            closeIndex(indexName);
        }

        if (minimumNodeVersion.onOrAfter(Version.V_7_2_0)) {
            // index is created on a version that supports the replication of closed indices,
            // so we expect the index to be closed and replicated
            ensureGreen(indexName);
            assertClosedIndex(indexName, true);
        } else {
            assertClosedIndex(indexName, false);
        }
    }

    /**
     * We test that a closed index makes no-op replica allocation/recovery only.
     */
    public void testClosedIndexNoopRecovery() throws Exception {
        final String indexName = "closed_index_replica_allocation";
        if (CLUSTER_TYPE == ClusterType.OLD) {
            createIndex(
                indexName,
                Settings.builder()
                    .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                    .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
                    .put(EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), "none")
                    .put(INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "120s")
                    .put("index.routing.allocation.include._name", CLUSTER_NAME + "-0")
                    .build()
            );
            indexDocs(indexName, 0, randomInt(10));
            // allocate replica to node-2
            updateIndexSettings(
                indexName,
                Settings.builder()
                    .put("index.routing.allocation.include._name", CLUSTER_NAME + "-0," + CLUSTER_NAME + "-2," + CLUSTER_NAME + "-*")
            );
            ensureGreen(indexName);
            closeIndex(indexName);
        }

        final Version indexVersionCreated = indexVersionCreated(indexName);
        if (indexVersionCreated.onOrAfter(Version.V_7_2_0)) {
            // index was created on a version that supports the replication of closed indices,
            // so we expect the index to be closed and replicated
            ensureGreen(indexName);
            assertClosedIndex(indexName, true);
            if (minimumNodeVersion().onOrAfter(Version.V_7_2_0)) {
                switch (CLUSTER_TYPE) {
                    case OLD:
                        break;
                    case MIXED:
                        assertNoopRecoveries(indexName, s -> s.startsWith(CLUSTER_NAME + "-0"));
                        break;
                    case UPGRADED:
                        assertNoopRecoveries(indexName, s -> s.startsWith(CLUSTER_NAME));
                        break;
                }
            }
        } else {
            assertClosedIndex(indexName, false);
        }

    }

    /**
     * Returns the version in which the given index has been created
     */
    private static Version indexVersionCreated(final String indexName) throws IOException {
        final Request request = new Request("GET", "/" + indexName + "/_settings");
        final String versionCreatedSetting = indexName + ".settings.index.version.created";
        request.addParameter("filter_path", versionCreatedSetting);

        final Response response = client().performRequest(request);
        return Version.fromId(Integer.parseInt(ObjectPath.createFromResponse(response).evaluate(versionCreatedSetting)));
    }

    /**
     * Asserts that an index is closed in the cluster state. If `checkRoutingTable` is true, it also asserts
     * that the index has started shards.
     */
    @SuppressWarnings("unchecked")
    private void assertClosedIndex(final String index, final boolean checkRoutingTable) throws IOException {
        final Map<String, ?> state = entityAsMap(client().performRequest(new Request("GET", "/_cluster/state")));

        final Map<String, ?> metadata = (Map<String, Object>) XContentMapValues.extractValue("metadata.indices." + index, state);
        assertThat(metadata, notNullValue());
        assertThat(metadata.get("state"), equalTo("close"));

        final Map<String, ?> blocks = (Map<String, Object>) XContentMapValues.extractValue("blocks.indices." + index, state);
        assertThat(blocks, notNullValue());
        assertThat(blocks.containsKey(String.valueOf(MetadataIndexStateService.INDEX_CLOSED_BLOCK_ID)), is(true));

        final Map<String, ?> settings = (Map<String, Object>) XContentMapValues.extractValue("settings", metadata);
        assertThat(settings, notNullValue());

        final int numberOfShards = Integer.parseInt((String) XContentMapValues.extractValue("index.number_of_shards", settings));
        final int numberOfReplicas = Integer.parseInt((String) XContentMapValues.extractValue("index.number_of_replicas", settings));

        final Map<String, ?> routingTable = (Map<String, Object>) XContentMapValues.extractValue("routing_table.indices." + index, state);
        if (checkRoutingTable) {
            assertThat(routingTable, notNullValue());
            assertThat(Booleans.parseBoolean((String) XContentMapValues.extractValue("index.verified_before_close", settings)), is(true));

            for (int i = 0; i < numberOfShards; i++) {
                final Collection<Map<String, ?>> shards = (Collection<Map<String, ?>>) XContentMapValues.extractValue(
                    "shards." + i,
                    routingTable
                );
                assertThat(shards, notNullValue());
                assertThat(shards.size(), equalTo(numberOfReplicas + 1));
                for (Map<String, ?> shard : shards) {
                    assertThat(XContentMapValues.extractValue("shard", shard), equalTo(i));
                    assertThat((String) XContentMapValues.extractValue("state", shard), oneOf("STARTED", "RELOCATING", "RELOCATED"));
                    assertThat(XContentMapValues.extractValue("index", shard), equalTo(index));
                }
            }
        } else {
            assertThat(routingTable, nullValue());
            assertThat(XContentMapValues.extractValue("index.verified_before_close", settings), nullValue());
        }
    }

    /** Ensure that we can always execute update requests regardless of the version of cluster */
    public void testUpdateDoc() throws Exception {
        final String index = "test_update_doc";
        if (CLUSTER_TYPE == ClusterType.OLD) {
            Settings.Builder settings = Settings.builder()
                .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 2);
            createIndex(index, settings.build());
            indexDocs(index, 0, 100);
        }
        if (randomBoolean()) {
            ensureGreen(index);
        }
        Map<Integer, Long> updates = new HashMap<>();
        for (int docId = 0; docId < 100; docId++) {
            final int times = randomIntBetween(0, 2);
            for (int i = 0; i < times; i++) {
                long value = randomNonNegativeLong();
                Request update = new Request("POST", index + "/_update/" + docId);
                update.setJsonEntity("{\"doc\": {\"updated_field\": " + value + "}}");
                client().performRequest(update);
                updates.put(docId, value);
            }
        }
        client().performRequest(new Request("POST", index + "/_refresh"));
        for (int docId : updates.keySet()) {
            Request get = new Request("GET", index + "/_doc/" + docId);
            Map<String, Object> doc = entityAsMap(client().performRequest(get));
            assertThat(XContentMapValues.extractValue("_source.updated_field", doc), equalTo(updates.get(docId)));
        }
        if (randomBoolean()) {
            flush(index, randomBoolean());
        }
    }

    private void assertNoopRecoveries(String indexName, Predicate<String> targetNode) throws IOException {
        Map<String, Object> recoveries = entityAsMap(client().performRequest(new Request("GET", indexName + "/_recovery?detailed=true")));

        @SuppressWarnings("unchecked")
        List<Map<String, ?>> shards = (List<Map<String, ?>>) XContentMapValues.extractValue(indexName + ".shards", recoveries);
        assertNotNull(shards);
        boolean foundReplica = false;
        for (Map<String, ?> shard : shards) {
            if (shard.get("primary") == Boolean.FALSE && targetNode.test((String) XContentMapValues.extractValue("target.name", shard))) {
                List<?> details = (List<?>) XContentMapValues.extractValue("index.files.details", shard);
                // once detailed recoveries works, remove this if.
                if (details == null) {
                    long totalFiles = ((Number) XContentMapValues.extractValue("index.files.total", shard)).longValue();
                    long reusedFiles = ((Number) XContentMapValues.extractValue("index.files.reused", shard)).longValue();
                    logger.info("total [{}] reused [{}]", totalFiles, reusedFiles);
                    assertEquals("must reuse all files, recoveries [" + recoveries + "]", totalFiles, reusedFiles);
                } else {
                    assertNotNull(details);
                    assertThat(details, Matchers.empty());
                }

                long translogRecovered = ((Number) XContentMapValues.extractValue("translog.recovered", shard)).longValue();
                assertEquals("must be noop, recoveries [" + recoveries + "]", 0, translogRecovered);
                foundReplica = true;
            }
        }

        assertTrue("must find replica", foundReplica);
    }

    /**
     * Tests that with or without soft-deletes, we should perform an operation-based recovery if there were some
     * but not too many uncommitted documents (i.e., less than 10% of committed documents or the extra translog)
     * before we upgrade each node. This is important when we move from translog based to retention leases based
     * peer recoveries.
     */
    public void testOperationBasedRecovery() throws Exception {
        final String index = "test_operation_based_recovery";
        if (CLUSTER_TYPE == ClusterType.OLD) {
            final Settings.Builder settings = Settings.builder()
                .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 2);
            if (minimumNodeVersion().before(Version.V_8_0_0) && randomBoolean()) {
                settings.put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), randomBoolean());
            }
            final String mappings = randomBoolean() ? "\"_source\": { \"enabled\": false}" : null;
            createIndex(index, settings.build(), mappings);
            ensureGreen(index);
            indexDocs(index, 0, randomIntBetween(100, 200));
            flush(index, randomBoolean());
            ensurePeerRecoveryRetentionLeasesRenewedAndSynced(index);
            // uncommitted docs must be less than 10% of committed docs (see IndexSetting#FILE_BASED_RECOVERY_THRESHOLD_SETTING).
            indexDocs(index, randomIntBetween(0, 100), randomIntBetween(0, 3));
        } else {
            ensureGreen(index);
            assertNoFileBasedRecovery(
                index,
                nodeName -> CLUSTER_TYPE == ClusterType.UPGRADED
                    || nodeName.startsWith(CLUSTER_NAME + "-0")
                    || (nodeName.startsWith(CLUSTER_NAME + "-1") && Booleans.parseBoolean(System.getProperty("tests.first_round")) == false)
            );
            indexDocs(index, randomIntBetween(0, 100), randomIntBetween(0, 3));
            ensurePeerRecoveryRetentionLeasesRenewedAndSynced(index);
        }
    }

    /**
     * Verifies that once all shard copies on the new version, we should turn off the translog retention for indices with soft-deletes.
     */
    public void testTurnOffTranslogRetentionAfterUpgraded() throws Exception {
        final String index = "turn_off_translog_retention";
        if (CLUSTER_TYPE == ClusterType.OLD) {
            createIndex(
                index,
                Settings.builder()
                    .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                    .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), randomIntBetween(0, 2))
                    .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                    .build()
            );
            ensureGreen(index);
            indexDocs(index, 0, randomIntBetween(100, 200));
            flush(index, randomBoolean());
            indexDocs(index, randomIntBetween(0, 100), randomIntBetween(0, 100));
        }
        if (CLUSTER_TYPE == ClusterType.UPGRADED) {
            ensureGreen(index);
            flush(index, true);
            assertEmptyTranslog(index);
        }
    }

    public void testAutoExpandIndicesDuringRollingUpgrade() throws Exception {
        final String indexName = "test-auto-expand-filtering";
        final Version minimumNodeVersion = minimumNodeVersion();

        Response response = client().performRequest(new Request("GET", "_nodes"));
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        final Map<String, Object> nodeMap = objectPath.evaluate("nodes");
        List<String> nodes = new ArrayList<>(nodeMap.keySet());

        if (CLUSTER_TYPE == ClusterType.OLD) {
            createIndex(
                indexName,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, randomInt(2))
                    .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-all")
                    .build()
            );
            ensureGreen(indexName);
            updateIndexSettings(
                indexName,
                Settings.builder().put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._id", nodes.get(randomInt(2)))
            );
        }

        ensureGreen(indexName);

        final int numberOfReplicas = Integer.parseInt(
            getIndexSettingsAsMap(indexName).get(IndexMetadata.SETTING_NUMBER_OF_REPLICAS).toString()
        );
        if (minimumNodeVersion.onOrAfter(Version.V_7_6_0)) {
            assertEquals(nodes.size() - 2, numberOfReplicas);
        } else {
            assertEquals(nodes.size() - 1, numberOfReplicas);
        }
    }

    public void testSoftDeletesDisabledWarning() throws Exception {
        final String indexName = "test_soft_deletes_disabled_warning";
        if (CLUSTER_TYPE == ClusterType.OLD) {
            boolean softDeletesEnabled = true;
            Settings.Builder settings = Settings.builder();
            if (minimumNodeVersion().before(Version.V_8_0_0) && randomBoolean()) {
                softDeletesEnabled = randomBoolean();
                settings.put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), softDeletesEnabled);
            }
            Request request = new Request("PUT", "/" + indexName);
            request.setJsonEntity("{\"settings\": " + Strings.toString(settings.build()) + "}");
            if (softDeletesEnabled == false) {
                expectSoftDeletesWarning(request, indexName);
            }
            client().performRequest(request);
        }
        ensureGreen(indexName);
        indexDocs(indexName, randomInt(100), randomInt(100));
    }
}

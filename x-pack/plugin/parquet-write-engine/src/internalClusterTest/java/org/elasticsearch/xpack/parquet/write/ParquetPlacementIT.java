/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplainRequest;
import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplanation;
import org.elasticsearch.action.admin.cluster.allocation.TransportClusterAllocationExplainAction;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * The primary of an index with the parquet format cannot move, and the index cannot have replicas, because the local files with the rows
 * that are not uploaded yet do not move with a shard. {@link ParquetAllocationDecider} says no to every way of moving a shard or adding
 * a copy of it, and these tests show it on a cluster of two data nodes, where a move and a replica are possible for any other index.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 2, numClientNodes = 0)
public class ParquetPlacementIT extends ESIntegTestCase {

    private static final String INDEX = "parquet-placement";
    private static final String REPOSITORY = "lake";
    private static final int ROWS = 1_000;

    private static Path repoRoot;

    @org.junit.BeforeClass
    public static void createRepoRoot() {
        repoRoot = createTempDir();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(Environment.PATH_REPO_SETTING.getKey(), repoRoot)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ParquetWriteEnginePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected void beforeIndexDeletion() {
        // the base class searches every shard, and this engine cannot be searched
    }

    @Override
    protected int numberOfReplicas() {
        // the random index template of the base class would add a replica, which this format does not support
        return 0;
    }

    /** An index with the parquet format and rows that are only in local files, because nothing flushes them. */
    private void createIndexWithLocalRows() throws IOException {
        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, REPOSITORY)
                .setType("fs")
                .setSettings(Settings.builder().put("location", repoRoot.resolve(REPOSITORY).toString()))
        );
        assertAcked(
            indicesAdmin().prepareCreate(INDEX)
                .setSettings(
                    Settings.builder()
                        .put("index.storage_format", "parquet")
                        .put("index.mode", "columnar")
                        .put("index.number_of_shards", 1)
                        .put("index.parquet.repository", REPOSITORY)
                        // no flush by size or by age: the rows stay in the local files
                        .put("index.parquet.flush_threshold_age", "6h")
                        .put("index.parquet.compaction.enabled", false)
                )
                .setMapping("count", "type=long")
        );
        ensureGreen(INDEX);
        final BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
        for (int i = 0; i < ROWS; i++) {
            bulk.add(new IndexRequest(INDEX).source("count", i));
        }
        assertFalse(bulk.get().hasFailures());
        assertThat(docCount(), equalTo((long) ROWS));
    }

    private long docCount() {
        return indicesAdmin().prepareStats(INDEX).get().getTotal().getDocs().getCount();
    }

    private ShardRouting primary() {
        final ClusterState state = clusterService().state();
        return state.routingTable().index(INDEX).shard(0).primaryShard();
    }

    private String nodeName(String nodeId) {
        return clusterService().state().nodes().get(nodeId).getName();
    }

    private String otherNodeName(ShardRouting primary) {
        for (var node : clusterService().state().nodes().getDataNodes().values()) {
            if (node.getId().equals(primary.currentNodeId()) == false) {
                return node.getName();
            }
        }
        throw new AssertionError("the cluster has no second data node");
    }

    private ClusterAllocationExplanation explain(boolean primary) {
        final var request = new ClusterAllocationExplainRequest(TEST_REQUEST_TIMEOUT).setIndex(INDEX).setShard(0).setPrimary(primary);
        return safeGet(client().execute(TransportClusterAllocationExplainAction.TYPE, request)).getExplanation();
    }

    /** The decisions of {@link ParquetAllocationDecider} in a list of decisions of every decider, which an explain output gives. */
    private static List<Decision> parquetDecisions(Decision decision) {
        return decision.getDecisions().stream().filter(d -> ParquetAllocationDecider.NAME.equals(d.label())).toList();
    }

    public void testAMoveOfThePrimaryIsRefusedAndTheRowsStay() throws Exception {
        createIndexWithLocalRows();
        final ShardRouting before = primary();
        final MoveAllocationCommand move = new MoveAllocationCommand(INDEX, 0, nodeName(before.currentNodeId()), otherNodeName(before));

        // without explain the command fails
        final AssertionError failure = expectThrows(
            AssertionError.class,
            () -> safeGet(
                client().execute(
                    TransportClusterRerouteAction.TYPE,
                    new ClusterRerouteRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).add(move)
                )
            )
        );
        assertThat(ExceptionsHelper.stackTrace(failure), containsString("since its not allowed"));

        // with explain the answer names the decider
        final var explanations = safeGet(
            client().execute(
                TransportClusterRerouteAction.TYPE,
                new ClusterRerouteRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).explain(true).add(move)
            )
        ).getExplanations().explanations();
        assertThat(explanations, hasSize(1));
        assertThat(explanations.get(0).decisions().type(), equalTo(Decision.Type.NO));
        final List<Decision> parquet = parquetDecisions(explanations.get(0).decisions());
        assertThat(parquet, hasSize(1));
        assertThat(parquet.get(0).getExplanation(), containsString("cannot move"));

        final ShardRouting after = primary();
        assertThat(after.currentNodeId(), equalTo(before.currentNodeId()));
        assertThat(after.state(), equalTo(ShardRoutingState.STARTED));
        assertThat(docCount(), equalTo((long) ROWS));
    }

    public void testAnAllocationFilterThatExcludesTheNodeOfThePrimaryDoesNotMoveIt() throws Exception {
        createIndexWithLocalRows();
        final ShardRouting before = primary();
        // the filter says that the shard cannot remain on its node, and the decider says that it cannot go anywhere else
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", nodeName(before.currentNodeId())), INDEX);
        ClusterRerouteUtils.reroute(client());
        safeGet(
            clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT)
                .setWaitForNoRelocatingShards(true)
                .setWaitForEvents(org.elasticsearch.common.Priority.LANGUID)
                .execute()
        );

        final ShardRouting after = primary();
        assertThat(after.currentNodeId(), equalTo(before.currentNodeId()));
        assertThat(after.state(), equalTo(ShardRoutingState.STARTED));
        assertThat(docCount(), equalTo((long) ROWS));
        final var move = explain(true).getShardAllocationDecision().getMoveDecision();
        assertTrue("the shard cannot remain: " + move, move.cannotRemainAndCannotMove());
        assertThat(move.getTargetNode(), nullValue());
        final NodeAllocationResult otherNode = move.getNodeDecisions()
            .stream()
            .filter(n -> n.getNode().getId().equals(before.currentNodeId()) == false)
            .findFirst()
            .orElseThrow();
        final List<Decision> parquet = parquetDecisions(otherNode.getCanAllocateDecision());
        assertThat(parquet, hasSize(1));
        assertThat(parquet.get(0).type(), equalTo(Decision.Type.NO));
        assertThat(parquet.get(0).getExplanation(), containsString("cannot move"));

        updateIndexSettings(Settings.builder().putNull("index.routing.allocation.exclude._name"), INDEX);
    }

    public void testAReplicaThatASettingsUpdateAsksForStaysUnassigned() throws Exception {
        createIndexWithLocalRows();

        updateIndexSettings(Settings.builder().put("index.number_of_replicas", 1), INDEX);

        ensureYellow(INDEX);
        final IndexShardRoutingTable table = clusterService().state().routingTable().index(INDEX).shard(0);
        assertThat(table.replicaShards(), hasSize(1));
        assertThat("the replica is not allocated", table.replicaShards().get(0).unassigned(), equalTo(true));
        assertThat(table.activeShards(), hasSize(1));
        final var allocate = explain(false).getShardAllocationDecision().getAllocateDecision();
        assertThat(allocate.getNodeDecisions(), notNullValue());
        assertThat(allocate.getNodeDecisions(), hasSize(2));
        for (NodeAllocationResult node : allocate.getNodeDecisions()) {
            final List<Decision> parquet = parquetDecisions(node.getCanAllocateDecision());
            assertThat(parquet, hasSize(1));
            assertThat(parquet.get(0).type(), equalTo(Decision.Type.NO));
            assertThat(parquet.get(0).getExplanation(), containsString("does not support replicas"));
        }
        assertThat(docCount(), equalTo((long) ROWS));

        updateIndexSettings(Settings.builder().put("index.number_of_replicas", 0), INDEX);
        ensureGreen(INDEX);
    }

    public void testAutoExpandDoesNotAddAReplica() throws Exception {
        createIndexWithLocalRows();

        updateIndexSettings(Settings.builder().put("index.auto_expand_replicas", "0-1"), INDEX);

        ensureGreen(INDEX);
        assertThat(clusterService().state().metadata().getProject().index(INDEX).getNumberOfReplicas(), equalTo(0));
        assertThat(clusterService().state().routingTable().index(INDEX).shard(0).replicaShards(), empty());
        assertThat(docCount(), equalTo((long) ROWS));
        updateIndexSettings(Settings.builder().put("index.auto_expand_replicas", "false"), INDEX);
    }

    public void testOtherIndicesStillGetTheirReplicas() throws Exception {
        createIndexWithLocalRows();

        assertAcked(
            indicesAdmin().prepareCreate("native-index")
                .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 1))
        );

        ensureGreen("native-index");
        assertThat(clusterService().state().routingTable().index("native-index").shard(0).activeShards(), hasSize(2));
    }
}

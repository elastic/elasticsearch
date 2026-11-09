/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.logging.log4j.Level;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.DatasetMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.disruption.BlockClusterStateProcessing;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.dataset.DeleteDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.plugin.TransportEsqlQueryAction;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;

/**
 * A worker that has applied some cluster state but not yet the one the coordinator planned from (a dataset registered or
 * changed in between) cannot bind the plan's dataset. It must not fail the query with the {@code 400} its own state
 * suggests: it waits for the state and then runs its splits, bounded by {@code esql.external.worker_ready_timeout}. The
 * coordinator holds a node back only while it joins ({@code ExternalWorkerReadiness}, {@link ExternalWorkerFirstStateIT});
 * a node that joined long ago and is behind a later state is sent its share and waits.
 *
 * <p>A dedicated master is the only voter, so parking a data-only node's applier cannot steal quorum: the dataset PUT
 * still commits, the coordinator applies it at once, and the parked node simply cannot ack. Publication waits for that
 * node until {@code cluster.publish.timeout}, so the tests shorten it. The node that has not applied the registration
 * refuses in the authorizer (the name is unknown to it); the node that has not applied a definition change refuses in
 * the rebind (the name is known, the definition version is not); both must end the same way.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class ExternalLaggingWorkerIT extends AbstractExternalDataSourceIT {

    /** {@code DataNodeComputeHandler} is package-private in {@code ...esql.plugin}, so its logger is named by string. */
    private static final String DISPATCH_LOGGER = "org.elasticsearch.xpack.esql.plugin.DataNodeComputeHandler";
    /** Above the eligible-worker count of every topology below, so round-robin hands every worker some splits. */
    private static final int FILES = 16;
    private static final int ROWS_PER_FILE = 100;
    private static final long ROWS = (long) FILES * ROWS_PER_FILE;
    /** Sum of ids 0 .. ROWS - 1; forces a real scan rather than an answer folded from file metadata. */
    private static final long ID_SUM = ROWS * (ROWS - 1) / 2;
    /** Short, so the parked node's missing ack is not waited out for the full default. */
    private static final TimeValue SHORT_ACK = TimeValue.timeValueSeconds(1);
    /** Tests that are not about the bound pin a generous one so they carry no timing assumption. */
    private static final TimeValue GENEROUS_WAIT = TimeValue.timeValueMinutes(1);
    private static final String DATA_SOURCE = "lag_ds";

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(2))
            .build();
    }

    @TestLogging(
        value = DISPATCH_LOGGER + ":DEBUG",
        reason = "waits for the lagging worker to park on the cluster state before releasing it"
    )
    public void testWorkerBehindADatasetRegistrationWaitsForTheState() throws Exception {
        String master = internalCluster().startMasterOnlyNode();
        List<String> dataNodes = startDataNodesWithWaitBudget(3, GENEROUS_WAIT);
        ensureStableCluster(4);
        String coordinator = dataNodes.get(0);
        String lagging = dataNodes.get(1);
        registerDataSource(DATA_SOURCE, Map.of());
        Path root = writeFixture("behind_registration");

        BlockClusterStateProcessing blocked = block(lagging);
        String dataset = null;
        try {
            dataset = putDatasetBehindTheBlock(master, coordinator, "behind_registration", root);
            blocked = assertWaitsForTheState(coordinator, lagging, dataset, blocked);
        } finally {
            unblock(blocked);
            deleteDataset(master, dataset);
        }
    }

    @TestLogging(
        value = DISPATCH_LOGGER + ":DEBUG",
        reason = "waits for the lagging worker to park on the cluster state before releasing it"
    )
    public void testWorkerBehindADefinitionChangeWaitsForTheState() throws Exception {
        String master = internalCluster().startMasterOnlyNode();
        List<String> dataNodes = startDataNodesWithWaitBudget(3, GENEROUS_WAIT);
        ensureStableCluster(4);
        String coordinator = dataNodes.get(0);
        String lagging = dataNodes.get(1);
        registerDataSource(DATA_SOURCE, Map.of());
        // Registered and applied everywhere, so the lagging node knows the name; what it will not know is the new definition.
        Path before = writeFixture("behind_change_before");
        String dataset = "behind_change";
        registerDataset(dataset, DATA_SOURCE, StoragePath.fileUri(before) + "/*.csv", Map.of());
        Path after = writeFixture("behind_change_after");

        BlockClusterStateProcessing blocked = block(lagging);
        try {
            putDatasetBehindTheBlock(master, coordinator, dataset, after);
            blocked = assertWaitsForTheState(coordinator, lagging, dataset, blocked);
        } finally {
            unblock(blocked);
        }
    }

    /** Every worker lagging: each waits, no query fails while the state is on its way, and all complete once it lands. */
    @TestLogging(value = DISPATCH_LOGGER + ":DEBUG", reason = "waits for every worker to park on the cluster state before releasing it")
    public void testEveryLaggingWorkerWaits() throws Exception {
        String master = internalCluster().startMasterOnlyNode();
        // A coordinating-only node is not an external worker, so the three data nodes are the whole resolved set.
        String coordinator = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        List<String> dataNodes = startDataNodesWithWaitBudget(3, GENEROUS_WAIT);
        ensureStableCluster(5);
        List<String> workers = eligibleWorkerNames(coordinator);
        assertThat(workers, containsInAnyOrder(dataNodes.toArray()));
        registerDataSource(DATA_SOURCE, Map.of());
        Path root = writeFixture("wait_all");

        List<BlockClusterStateProcessing> blocked = blockAll(workers);
        String dataset = null;
        try {
            dataset = putDatasetBehindTheBlock(master, coordinator, "wait_all", root);
            try (var mockLog = MockLog.capture(DISPATCH_LOGGER)) {
                for (String worker : workers) {
                    mockLog.addExpectation(waiting(worker));
                }
                ActionFuture<EsqlQueryResponse> future = client(coordinator).execute(EsqlQueryAction.INSTANCE, request(dataset, true));
                mockLog.awaitAllExpectationsMatched();
                unblockAll(blocked);
                try (var response = future.actionGet(TIMEOUT)) {
                    assertThat(response.isPartial(), equalTo(false));
                    assertThat(getValuesList(response), equalTo(List.of(List.of(ROWS, ID_SUM))));
                    assertThat(externalScanNodeNames(response), equalTo(Set.copyOf(workers)));
                }
            }
        } finally {
            unblockAll(blocked);
            deleteDataset(master, dataset);
        }
    }

    @TestLogging(value = DISPATCH_LOGGER + ":DEBUG", reason = "waits for the lone worker to park on the cluster state before releasing it")
    public void testLoneWorkerBehindADatasetRegistrationWaitsForTheState() throws Exception {
        String master = internalCluster().startMasterOnlyNode();
        // A coordinating-only node is not an external worker, so the data node below is the coordinator's only choice.
        String coordinator = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        String lagging = startDataNodesWithWaitBudget(1, GENEROUS_WAIT).getFirst();
        ensureStableCluster(3);
        registerDataSource(DATA_SOURCE, Map.of());
        Path root = writeFixture("lone_wait");

        BlockClusterStateProcessing blocked = block(lagging);
        String dataset = null;
        try {
            dataset = putDatasetBehindTheBlock(master, coordinator, "lone_wait", root);
            blocked = assertWaitsForTheState(coordinator, lagging, dataset, blocked);
        } finally {
            unblock(blocked);
            deleteDataset(master, dataset);
        }
    }

    public void testLoneWorkerFailsRetryablyWhenTheWaitRunsOut() throws Exception {
        String master = internalCluster().startMasterOnlyNode();
        String coordinator = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        // The setting is dynamic, but a parked node applies no settings update either; it is given at start.
        String lagging = startDataNodesWithWaitBudget(1, TimeValue.timeValueMillis(100)).getFirst();
        ensureStableCluster(3);
        registerDataSource(DATA_SOURCE, Map.of());
        Path root = writeFixture("lone_timeout");

        BlockClusterStateProcessing blocked = block(lagging);
        String dataset = null;
        try {
            dataset = putDatasetBehindTheBlock(master, coordinator, "lone_timeout", root);
            EsqlQueryRequest request = request(dataset, false);
            Exception failure = expectThrows(
                Exception.class,
                () -> client(coordinator).execute(EsqlQueryAction.INSTANCE, request).actionGet(TIMEOUT).close()
            );
            assertThat(failure.toString(), ExceptionsHelper.status(failure), equalTo(RestStatus.SERVICE_UNAVAILABLE));
            assertTrue(failure.toString(), ExternalException.hasCondition(failure, ExternalException.Condition.WORKER_NOT_READY));
        } finally {
            unblock(blocked);
            deleteDataset(master, dataset);
        }
    }

    @TestLogging(value = DISPATCH_LOGGER + ":DEBUG", reason = "waits for the lone worker to park on the cluster state before cancelling")
    public void testCancellationDuringTheWaitReleasesTheSink() throws Exception {
        String master = internalCluster().startMasterOnlyNode();
        String coordinator = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        String lagging = startDataNodesWithWaitBudget(1, GENEROUS_WAIT).getFirst();
        ensureStableCluster(3);
        registerDataSource(DATA_SOURCE, Map.of());
        Path root = writeFixture("lone_cancel");

        BlockClusterStateProcessing blocked = block(lagging);
        String dataset = null;
        try {
            dataset = putDatasetBehindTheBlock(master, coordinator, "lone_cancel", root);
            try (var mockLog = MockLog.capture(DISPATCH_LOGGER)) {
                mockLog.addExpectation(waiting(lagging));
                ActionFuture<EsqlQueryResponse> future = client(coordinator).execute(EsqlQueryAction.INSTANCE, request(dataset, false));
                mockLog.awaitAllExpectationsMatched();
                client(coordinator).admin()
                    .cluster()
                    .prepareCancelTasks()
                    .setActions(EsqlQueryAction.NAME)
                    .waitForCompletion(true)
                    .get(TIMEOUT);
                Exception failure = expectThrows(Exception.class, () -> future.actionGet(TIMEOUT).close());
                assertThat(failure.toString(), ExceptionsHelper.unwrap(failure, TaskCancelledException.class), notNullValue());
            }
            // The sink the coordinator opened on the parked worker is dropped by the cancellation, not by the reaper.
            var exchangeService = internalCluster().getInstance(TransportEsqlQueryAction.class, lagging).exchangeService();
            assertBusy(() -> assertTrue("leftover sinks " + exchangeService.sinkKeys(), exchangeService.isEmpty()));
        } finally {
            unblock(blocked);
            deleteDataset(master, dataset);
        }
    }

    /**
     * Runs the query while {@code lagging} is parked, waits for it to park on the state it lacks, releases the state and
     * asserts the full result, with the lagging worker having run its share. Returns the released block, {@code null}.
     */
    private BlockClusterStateProcessing assertWaitsForTheState(
        String coordinator,
        String lagging,
        String dataset,
        BlockClusterStateProcessing blocked
    ) throws Exception {
        try (var mockLog = MockLog.capture(DISPATCH_LOGGER)) {
            mockLog.addExpectation(waiting(lagging));
            ActionFuture<EsqlQueryResponse> future = client(coordinator).execute(EsqlQueryAction.INSTANCE, request(dataset, true));
            // The worker is parked on the state it lacks; releasing the applier delivers it and the request resumes.
            mockLog.awaitAllExpectationsMatched();
            blocked = unblock(blocked);
            try (var response = future.actionGet(TIMEOUT)) {
                assertThat(response.isPartial(), equalTo(false));
                assertThat(getValuesList(response), equalTo(List.of(List.of(ROWS, ID_SUM))));
                // The wait line proves the lagging node was assigned splits; this proves it ran them once the state landed.
                assertThat("the lagging worker runs its share after the wait", externalScanNodeNames(response), hasItem(lagging));
            }
            return blocked;
        }
    }

    /** Same order and predicate as {@code NodeEligibilityStrategy.EXTERNAL_WORKER_NODES}. */
    private static List<String> eligibleWorkerNames(String viaNode) {
        List<String> eligible = new ArrayList<>();
        for (DiscoveryNode node : internalCluster().clusterService(viaNode).state().nodes()) {
            if (node.canContainData() && node.hasRole(DiscoveryNodeRole.INDEX_ROLE.roleName()) == false) {
                eligible.add(node.getName());
            }
        }
        return eligible;
    }

    private static MockLog.SeenEventExpectation waiting(String node) {
        return new MockLog.SeenEventExpectation(
            "wait on " + node,
            DISPATCH_LOGGER,
            Level.DEBUG,
            "waiting up to [*] for the cluster state before running [*] external splits on [" + node + "], session [*"
        );
    }

    /**
     * Registers (or replaces) {@code name} on the master while one node cannot apply state, and returns once
     * {@code coordinator} has applied it, so the coordinator plans from a state the parked node does not have.
     */
    private String putDatasetBehindTheBlock(String master, String coordinator, String name, Path root) {
        String resource = StoragePath.fileUri(root) + "/*.csv";
        AcknowledgedResponse put = client(master).execute(
            PutDatasetAction.INSTANCE,
            new PutDatasetAction.Request(TIMEOUT, SHORT_ACK, name, DATA_SOURCE, resource, null, new HashMap<>())
        ).actionGet(TIMEOUT);
        assertThat("the parked node cannot ack", put.isAcknowledged(), equalTo(false));
        awaitClusterState(coordinator, state -> {
            Dataset dataset = DatasetMetadata.get(state.metadata().getProject()).get(name);
            return dataset != null && Objects.equals(dataset.resource(), resource);
        });
        return name;
    }

    /**
     * The wait budget is given at start: the setting is dynamic, but a node that applies no cluster state applies no settings
     * update either.
     */
    private static List<String> startDataNodesWithWaitBudget(int count, TimeValue budget) {
        return internalCluster().startDataOnlyNodes(
            count,
            Settings.builder().put(ExternalSourceSettings.WORKER_READY_TIMEOUT.getKey(), budget).build()
        );
    }

    private BlockClusterStateProcessing block(String node) {
        BlockClusterStateProcessing blocked = new BlockClusterStateProcessing(node, random());
        internalCluster().setDisruptionScheme(blocked);
        blocked.startDisrupting();
        return blocked;
    }

    /** Idempotent, so it can run both at the point a test needs the state delivered and unconditionally in {@code finally}. */
    private static BlockClusterStateProcessing unblock(BlockClusterStateProcessing blocked) {
        if (blocked != null) {
            blocked.stopDisrupting();
            internalCluster().clearDisruptionScheme();
        }
        return null;
    }

    /**
     * Blocks several nodes at once. The test cluster registers a single disruption scheme and the base class clears only
     * that one, so these are applied directly and stopped by {@link #unblockAll}, which the tests run unconditionally.
     */
    private List<BlockClusterStateProcessing> blockAll(List<String> nodes) {
        List<BlockClusterStateProcessing> blocked = new ArrayList<>();
        try {
            for (String node : nodes) {
                BlockClusterStateProcessing block = new BlockClusterStateProcessing(node, random());
                block.applyToCluster(internalCluster());
                block.startDisrupting();
                blocked.add(block);
            }
        } catch (RuntimeException | AssertionError e) {
            // A block that failed to start must not leave the earlier ones parked: the test's finally never sees them.
            unblockAll(blocked);
            throw e;
        }
        return blocked;
    }

    /** Idempotent: stopping a stopped block is a no-op. */
    private static void unblockAll(List<BlockClusterStateProcessing> blocked) {
        for (BlockClusterStateProcessing block : blocked) {
            block.stopDisrupting();
        }
    }

    /** Datasets put behind the block bypass the base class bookkeeping; the base deletes the data source after this. */
    private void deleteDataset(String master, String dataset) {
        if (dataset == null) {
            return;
        }
        try {
            client(master).execute(
                DeleteDatasetAction.INSTANCE,
                new DeleteDatasetAction.Request(TIMEOUT, TIMEOUT, new String[] { dataset })
            ).actionGet(TIMEOUT);
        } catch (Exception e) {
            logger.warn("dataset cleanup [{}] failed", dataset, e);
        }
    }

    private Path writeFixture(String name) throws Exception {
        Path root = createTempDir().resolve(name);
        Files.createDirectories(root);
        for (int f = 0; f < FILES; f++) {
            StringBuilder body = new StringBuilder("id\n");
            for (int i = 0; i < ROWS_PER_FILE; i++) {
                body.append(f * ROWS_PER_FILE + i).append('\n');
            }
            Files.writeString(root.resolve("part_" + f + ".csv"), body.toString(), StandardCharsets.UTF_8);
        }
        return root;
    }

    private static EsqlQueryRequest request(String dataset, boolean profile) {
        var request = syncEsqlQueryRequest("FROM " + dataset + " | STATS c = COUNT(*), s = SUM(id)");
        request.pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "round_robin").build()));
        request.acceptedPragmaRisks(true);
        request.profile(profile);
        return request;
    }
}

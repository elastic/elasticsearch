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
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.disruption.BlockClusterStateProcessing;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.ConnectTransportException;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

/**
 * A worker that has applied some cluster state but not yet the one the coordinator planned from (a dataset registered or
 * changed in between) cannot bind the plan's dataset. It must not fail the query with the {@code 400} its own state
 * suggests: with other workers available it declines and the coordinator hands its splits to the next of them in resolved
 * order, and so on past every worker that declines, the last of them waiting for the state; as the only worker it waits
 * at once. Each wait is bounded by {@code esql.external.worker_ready_timeout}.
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
    private static final String DATA_SOURCE = "lag_ds";

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(2))
            .build();
    }

    /**
     * A walk passes declines, not failures: when the next worker's exchange cannot be opened the query fails, with the
     * open failure, rather than walking on or hanging on slots that are never completed. The exchanges opened on the
     * other workers are released. With three workers the hop is the first of two, so it still asks for a decline.
     */
    @TestLogging(value = DISPATCH_LOGGER + ":DEBUG", reason = "asserts the hop was attempted before it failed")
    public void testFallbackWhoseExchangeCannotBeOpenedFailsTheQuery() throws Exception {
        assertHopFailure(ExchangeService.OPEN_EXCHANGE_ACTION_NAME, "hop_open_fails");
    }

    /** Same as above, one step later: the next worker's exchange is open but the splits cannot be sent to it. */
    @TestLogging(value = DISPATCH_LOGGER + ":DEBUG", reason = "asserts the hop was attempted before it failed")
    public void testFallbackThatCannotBeSentTheSplitsFailsTheQuery() throws Exception {
        assertHopFailure(EsqlQueryAction.NAME + "/data", "hop_send_fails");
    }

    private void assertHopFailure(String failedAction, String datasetName) throws Exception {
        String master = internalCluster().startMasterOnlyNode();
        List<String> dataNodes = internalCluster().startDataOnlyNodes(3);
        ensureStableCluster(4);
        String coordinator = dataNodes.get(0);
        // The first hop goes to the next worker after the decliner in eligible order; the coordinator's own requests take
        // the local connection, which the transport rule below does not see, so the lagging node is chosen such that the
        // hop goes to the third node. The reroute line asserted below names that node, so a wrong choice fails loudly.
        List<String> workers = eligibleWorkerNames(coordinator);
        assertThat(workers, hasSize(3));
        String lagging = workers.get((workers.indexOf(coordinator) + 1) % workers.size());
        String fallback = workers.get((workers.indexOf(coordinator) + 2) % workers.size());
        registerDataSource(DATA_SOURCE, Map.of());
        Path root = writeFixture(datasetName);

        var coordinatorTransport = MockTransportService.getInstance(coordinator);
        Map<String, AtomicInteger> sendsPerNode = new ConcurrentHashMap<>();
        AtomicBoolean failed = new AtomicBoolean();
        // The dispatch sends every worker one request per action; the second one to a node is the hop's.
        coordinatorTransport.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(failedAction)
                && sendsPerNode.computeIfAbsent(connection.getNode().getId(), id -> new AtomicInteger()).incrementAndGet() == 2) {
                failed.set(true);
                throw new ConnectTransportException(connection.getNode(), "simulated: fallback unreachable");
            }
            connection.sendRequest(requestId, action, request, options);
        });
        BlockClusterStateProcessing blocked = block(lagging);
        String dataset = null;
        try {
            dataset = putDatasetBehindTheBlock(master, coordinator, datasetName, root);
            try (var mockLog = MockLog.capture(DISPATCH_LOGGER)) {
                mockLog.addExpectation(rerouted(lagging, fallback, 1, 2));
                EsqlQueryRequest request = request(dataset, false);
                Exception failure = expectThrows(
                    Exception.class,
                    () -> client(coordinator).execute(EsqlQueryAction.INSTANCE, request).actionGet(TIMEOUT).close()
                );
                assertTrue("the hop must have been attempted", failed.get());
                assertThat(failure.toString(), ExceptionsHelper.unwrap(failure, ConnectTransportException.class), notNullValue());
                mockLog.assertAllExpectationsMatched();
            }
        } finally {
            coordinatorTransport.clearAllRules();
            unblock(blocked);
            deleteDataset(master, dataset);
        }
    }

    /** Same order and predicate as {@code NodeEligibilityStrategy.EXTERNAL_WORKER_NODES}, which is also the resolved order. */
    private static List<String> eligibleWorkerNames(String viaNode) {
        List<String> eligible = new ArrayList<>();
        for (DiscoveryNode node : internalCluster().clusterService(viaNode).state().nodes()) {
            if (node.canContainData() && node.hasRole(DiscoveryNodeRole.INDEX_ROLE.roleName()) == false) {
                eligible.add(node.getName());
            }
        }
        return eligible;
    }

    @TestLogging(
        value = DISPATCH_LOGGER + ":DEBUG",
        reason = "asserts the decline on the lagging worker and the reroute on the coordinator"
    )
    public void testWorkerBehindADatasetRegistrationIsRoutedAround() throws Exception {
        String master = internalCluster().startMasterOnlyNode();
        List<String> dataNodes = internalCluster().startDataOnlyNodes(3);
        ensureStableCluster(4);
        String coordinator = dataNodes.get(0);
        String lagging = dataNodes.get(1);
        registerDataSource(DATA_SOURCE, Map.of());
        Path root = writeFixture("behind_registration");

        BlockClusterStateProcessing blocked = block(lagging);
        String dataset = null;
        try {
            dataset = putDatasetBehindTheBlock(master, coordinator, "behind_registration", root);
            assertRoutedAround(coordinator, lagging, dataset);
        } finally {
            unblock(blocked);
            deleteDataset(master, dataset);
        }
    }

    @TestLogging(
        value = DISPATCH_LOGGER + ":DEBUG",
        reason = "asserts the decline on the lagging worker and the reroute on the coordinator"
    )
    public void testWorkerBehindADefinitionChangeIsRoutedAround() throws Exception {
        String master = internalCluster().startMasterOnlyNode();
        List<String> dataNodes = internalCluster().startDataOnlyNodes(3);
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
            assertRoutedAround(coordinator, lagging, dataset);
        } finally {
            unblock(blocked);
        }
    }

    /**
     * Two lagging workers that follow each other in resolved order: the splits of the first walk past the second onto the
     * ready worker after it, the splits of the second take one hop there, and nobody waits.
     */
    @TestLogging(value = DISPATCH_LOGGER + ":DEBUG", reason = "asserts each hop of the walk and that no worker waited")
    public void testSplitsWalkPastTwoLaggingWorkers() throws Exception {
        String master = internalCluster().startMasterOnlyNode();
        List<String> dataNodes = internalCluster().startDataOnlyNodes(4);
        ensureStableCluster(5);
        String coordinator = dataNodes.get(0);
        // The resolved order is the cluster state's node order, not the start order, so the neighbours are read from it.
        List<String> workers = eligibleWorkerNames(coordinator);
        assertThat(workers, hasSize(4));
        int at = workers.indexOf(coordinator);
        String first = workers.get((at + 1) % workers.size());
        String second = workers.get((at + 2) % workers.size());
        String ready = workers.get((at + 3) % workers.size());
        registerDataSource(DATA_SOURCE, Map.of());
        Path root = writeFixture("walk_two");

        List<BlockClusterStateProcessing> blocked = blockAll(List.of(first, second));
        String dataset = null;
        try {
            dataset = putDatasetBehindTheBlock(master, coordinator, "walk_two", root);
            try (var mockLog = MockLog.capture(DISPATCH_LOGGER)) {
                mockLog.addExpectation(rerouted(first, second, 1, 3));
                mockLog.addExpectation(rerouted(second, ready, 2, 3));
                mockLog.addExpectation(rerouted(second, ready, 1, 3));
                mockLog.addExpectation(
                    new MockLog.UnseenEventExpectation(
                        "no wait",
                        DISPATCH_LOGGER,
                        Level.DEBUG,
                        "waiting up to [*] for the cluster state before running [*] external splits on [*"
                    )
                );
                try (var response = client(coordinator).execute(EsqlQueryAction.INSTANCE, request(dataset, true)).actionGet(TIMEOUT)) {
                    assertThat(response.isPartial(), equalTo(false));
                    assertThat(getValuesList(response), equalTo(List.of(List.of(ROWS, ID_SUM))));
                    assertThat(externalScanNodeNames(response), not(hasItem(first)));
                    assertThat(externalScanNodeNames(response), not(hasItem(second)));
                }
                mockLog.assertAllExpectationsMatched();
            }
        } finally {
            unblockAll(blocked);
            deleteDataset(master, dataset);
        }
    }

    /**
     * Every worker lagging: each worker's splits walk the other two, which decline, and wait on the last, so no query
     * fails while the state is on its way, and all of them complete once it lands.
     */
    @TestLogging(value = DISPATCH_LOGGER + ":DEBUG", reason = "waits for every walk to park on its last worker before releasing the state")
    public void testWalkEndsWaitingOnTheLastWorker() throws Exception {
        String master = internalCluster().startMasterOnlyNode();
        // A coordinating-only node is not an external worker, so the three data nodes are the whole resolved set.
        String coordinator = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        TimeValue budget = TimeValue.timeValueMinutes(1);
        List<String> dataNodes = List.of(
            startDataNodeWithWaitBudget(budget),
            startDataNodeWithWaitBudget(budget),
            startDataNodeWithWaitBudget(budget)
        );
        ensureStableCluster(5);
        List<String> workers = eligibleWorkerNames(coordinator);
        assertThat(workers, hasSize(3));
        assertThat(workers, containsInAnyOrder(dataNodes.toArray()));
        registerDataSource(DATA_SOURCE, Map.of());
        Path root = writeFixture("walk_all");

        List<BlockClusterStateProcessing> blocked = blockAll(workers);
        String dataset = null;
        try {
            dataset = putDatasetBehindTheBlock(master, coordinator, "walk_all", root);
            try (var mockLog = MockLog.capture(DISPATCH_LOGGER)) {
                for (int i = 0; i < workers.size(); i++) {
                    String target = workers.get(i);
                    String next = workers.get((i + 1) % workers.size());
                    String last = workers.get((i + 2) % workers.size());
                    mockLog.addExpectation(rerouted(target, next, 1, 2));
                    mockLog.addExpectation(rerouted(next, last, 2, 2));
                    mockLog.addExpectation(waiting(last));
                }
                ActionFuture<EsqlQueryResponse> future = client(coordinator).execute(EsqlQueryAction.INSTANCE, request(dataset, false));
                mockLog.awaitAllExpectationsMatched();
                unblockAll(blocked);
                try (var response = future.actionGet(TIMEOUT)) {
                    assertThat(response.isPartial(), equalTo(false));
                    assertThat(getValuesList(response), equalTo(List.of(List.of(ROWS, ID_SUM))));
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
        String lagging = startDataNodeWithWaitBudget(TimeValue.timeValueMinutes(1));
        ensureStableCluster(3);
        registerDataSource(DATA_SOURCE, Map.of());
        Path root = writeFixture("lone_wait");

        BlockClusterStateProcessing blocked = block(lagging);
        String dataset = null;
        try {
            dataset = putDatasetBehindTheBlock(master, coordinator, "lone_wait", root);
            try (var mockLog = MockLog.capture(DISPATCH_LOGGER)) {
                mockLog.addExpectation(waiting(lagging));
                ActionFuture<EsqlQueryResponse> future = client(coordinator).execute(EsqlQueryAction.INSTANCE, request(dataset, false));
                // The worker is parked on the state it lacks; releasing the applier delivers it and the request resumes.
                mockLog.awaitAllExpectationsMatched();
                blocked = unblock(blocked);
                try (var response = future.actionGet(TIMEOUT)) {
                    assertThat(response.isPartial(), equalTo(false));
                    assertThat(getValuesList(response), equalTo(List.of(List.of(ROWS, ID_SUM))));
                }
            }
        } finally {
            unblock(blocked);
            deleteDataset(master, dataset);
        }
    }

    public void testLoneWorkerFailsRetryablyWhenTheWaitRunsOut() throws Exception {
        String master = internalCluster().startMasterOnlyNode();
        String coordinator = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        // The setting is dynamic, but a parked node applies no settings update either; it is given at start.
        String lagging = startDataNodeWithWaitBudget(TimeValue.timeValueMillis(100));
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
        String lagging = startDataNodeWithWaitBudget(TimeValue.timeValueMinutes(1));
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

    private void assertRoutedAround(String coordinator, String lagging, String dataset) throws Exception {
        try (var mockLog = MockLog.capture(DISPATCH_LOGGER)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "decline on " + lagging,
                    DISPATCH_LOGGER,
                    Level.DEBUG,
                    "declining [*] external splits on [" + lagging + "], session [*"
                )
            );
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "reroute off " + lagging,
                    DISPATCH_LOGGER,
                    Level.DEBUG,
                    "external worker [" + lagging + "] has not applied the cluster state this query was planned from; rerouting [*"
                )
            );
            try (var response = client(coordinator).execute(EsqlQueryAction.INSTANCE, request(dataset, true)).actionGet(TIMEOUT)) {
                assertThat(response.isPartial(), equalTo(false));
                assertThat(getValuesList(response), equalTo(List.of(List.of(ROWS, ID_SUM))));
                assertThat("the lagging worker must not have scanned", externalScanNodeNames(response), not(hasItem(lagging)));
            }
            // The decline line proves the lagging node was assigned splits; without it the answer above is vacuous.
            mockLog.assertAllExpectationsMatched();
        }
    }

    private static MockLog.SeenEventExpectation rerouted(String declined, String next, int hop, int hops) {
        return new MockLog.SeenEventExpectation(
            "reroute " + declined + " -> " + next + " on hop " + hop,
            DISPATCH_LOGGER,
            Level.DEBUG,
            "external worker ["
                + declined
                + "] has not applied the cluster state this query was planned from; rerouting [*] splits to ["
                + next
                + "] on hop ["
                + hop
                + "/"
                + hops
                + "], session [*"
        );
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
     * update either. Tests that are not about the bound pin a generous one so they carry no timing assumption.
     */
    private static String startDataNodeWithWaitBudget(TimeValue budget) {
        return internalCluster().startDataOnlyNode(
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

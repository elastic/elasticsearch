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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

/**
 * A worker that has applied some cluster state but not yet the one the coordinator planned from (a dataset registered or
 * changed in between) cannot bind the plan's dataset. It must not fail the query with the {@code 400} its own state
 * suggests: with another worker available it declines and the coordinator hands its splits to that worker; as the only
 * worker it waits for the state, bounded by {@code esql.external.worker_ready_timeout}.
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
     * The hop is the last attempt: when the fallback's exchange cannot be opened the query fails, with the open failure,
     * rather than hanging on slots that are never completed. The exchanges opened on the other workers are released.
     */
    @TestLogging(value = DISPATCH_LOGGER + ":DEBUG", reason = "asserts the hop was attempted before it failed")
    public void testFallbackWhoseExchangeCannotBeOpenedFailsTheQuery() throws Exception {
        assertHopFailure(ExchangeService.OPEN_EXCHANGE_ACTION_NAME, "hop_open_fails");
    }

    /** Same as above, one step later: the fallback's exchange is open but the splits cannot be sent to it. */
    @TestLogging(value = DISPATCH_LOGGER + ":DEBUG", reason = "asserts the hop was attempted before it failed")
    public void testFallbackThatCannotBeSentTheSplitsFailsTheQuery() throws Exception {
        assertHopFailure(EsqlQueryAction.NAME + "/data", "hop_send_fails");
    }

    private void assertHopFailure(String failedAction, String datasetName) throws Exception {
        String master = internalCluster().startMasterOnlyNode();
        List<String> dataNodes = internalCluster().startDataOnlyNodes(3);
        ensureStableCluster(4);
        String coordinator = dataNodes.get(0);
        // The fallback is the next worker after the decliner in eligible order; the coordinator's own requests take the
        // local connection, which the transport rule below does not see, so the lagging node is chosen such that its
        // fallback is the third node. The reroute line asserted below names the fallback, so a wrong choice fails loudly.
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
                mockLog.addExpectation(
                    new MockLog.SeenEventExpectation(
                        "reroute to " + fallback,
                        DISPATCH_LOGGER,
                        Level.DEBUG,
                        "external worker ["
                            + lagging
                            + "] has not applied the cluster state this query was planned from; rerouting [*] "
                            + "splits to ["
                            + fallback
                            + "], session [*"
                    )
                );
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

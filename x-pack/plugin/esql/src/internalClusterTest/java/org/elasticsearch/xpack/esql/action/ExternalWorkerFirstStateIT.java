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
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.core.async.AsyncStopRequest;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.datasource.TestDataSourceConnectionAction;
import org.elasticsearch.xpack.esql.datasources.datasource.TestDataSourceNodeAction;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.ExternalWorkerReadiness;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlAsyncTestUtils.deleteAsyncId;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

/**
 * The join race: a node accepts transport requests before it has applied any cluster state, and the coordinator sees it
 * as an external worker as soon as it applies the join; every split sent to it until
 * its first apply completes would land on a node with no dataset, no key and default settings. The coordinator therefore
 * holds a node that has just joined back ({@code ExternalWorkerReadiness}) until the node confirms it applied the state
 * that added it, which a node parked in its first apply can only do once that apply is done. Meanwhile the other workers
 * take the splits; when the joining node would have been the only worker, the query waits on the coordinator for it,
 * bounded by {@code esql.external.worker_ready_timeout}, rather than scan on a coordinator that may be thin.
 *
 * <p>The window is produced by parking the new node's cluster applier in its first apply ({@link ParkFirstApplyPlugin}):
 * appliers run before the node's visible state is swapped, so while parked the node still reports its initial,
 * not-recovered state, and the master, which commits on its own vote, has long since let the coordinator see the node.
 * {@code discovery.initial_state_timeout} is zero so starting the node does not wait for that apply, and master nodes
 * are managed by hand because the test cluster's own post-start check would wait for the parked node's view of the
 * cluster. The hold is bounded by the master's lag detector (90 s by default); the tests hold for seconds.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0, autoManageMasterNodes = false)
public class ExternalWorkerFirstStateIT extends AbstractExternalDataSourceIT {

    /** {@code DataNodeComputeHandler} is package-private in {@code ...esql.plugin}, so its logger is named by string. */
    private static final String DISPATCH_LOGGER = "org.elasticsearch.xpack.esql.plugin.DataNodeComputeHandler";
    private static final String READINESS_LOGGER = "org.elasticsearch.xpack.esql.plugin.ExternalWorkerReadiness";
    private static final int FILES = 16;
    private static final int ROWS_PER_FILE = 100;
    private static final long ROWS = (long) FILES * ROWS_PER_FILE;
    private static final long ID_SUM = ROWS * (ROWS - 1) / 2;
    private static final String DATA_SOURCE = "join_ds";

    /**
     * Parks the cluster applier of the node started with {@link #PARK} in its first apply until {@link #releaseApplier}.
     * One node per test is parked, so the latches are static and re-armed by {@link #arm} before that node starts. A
     * state applier must not throw, so a hold that is never released is logged and given up after a minute rather than
     * failing the apply.
     */
    public static class ParkFirstApplyPlugin extends Plugin {
        private static final Logger PARK_LOGGER = LogManager.getLogger(ParkFirstApplyPlugin.class);
        static final Setting<Boolean> PARK = Setting.boolSetting(
            "test.esql.park_first_cluster_state_apply",
            false,
            Setting.Property.NodeScope
        );

        private static volatile CountDownLatch parked = new CountDownLatch(1);
        private static volatile CountDownLatch release = new CountDownLatch(1);

        static void arm() {
            parked = new CountDownLatch(1);
            release = new CountDownLatch(1);
        }

        static void awaitParked() {
            safeAwait(parked);
        }

        static void releaseApplier() {
            release.countDown();
        }

        @Override
        public List<Setting<?>> getSettings() {
            return List.of(PARK);
        }

        @Override
        public Collection<?> createComponents(PluginServices services) {
            if (PARK.get(services.clusterService().getSettings())) {
                AtomicBoolean first = new AtomicBoolean();
                services.clusterService().addStateApplier((ClusterStateApplier) event -> {
                    if (first.compareAndSet(false, true) == false) {
                        return;
                    }
                    parked.countDown();
                    try {
                        if (release.await(1, TimeUnit.MINUTES) == false) {
                            PARK_LOGGER.error("the first cluster state apply was parked but never released; applying it anyway");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            return List.of();
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // The async plugin wires up the async DELETE action the STOP test cleans up with; the mock transport sees what the
        // connection test sends.
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(ParkFirstApplyPlugin.class);
        plugins.add(EsqlAsyncActionIT.LocalStateEsqlAsync.class);
        plugins.add(MockTransportService.TestPlugin.class);
        return plugins;
    }

    /**
     * The master's join publication to the parked node stays in flight until the node applies it or this times out; the
     * default of 30 s is the same as the query timeout, so it is shortened rather than left as a hidden budget.
     */
    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(2))
            .build();
    }

    /**
     * The joining node is held back: it is sent nothing (a request would make it wait, and the wait line is asserted absent),
     * the other workers scan everything, and once its first apply completes it is released and scans like any other.
     */
    @TestLogging(
        value = DISPATCH_LOGGER + ":DEBUG," + READINESS_LOGGER + ":DEBUG",
        reason = "asserts the joining worker is sent nothing and not released before its first apply, and released after"
    )
    public void testWorkerWithoutAnyAppliedStateIsHeldBack() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        internalCluster().startMasterOnlyNode();
        List<String> ready = internalCluster().startDataOnlyNodes(2);
        ensureStableCluster(3);
        String coordinator = ready.get(0);
        String dataset = registerFixture("join_held_back");

        // Started inside the try: a start that fails after the node received its first state would otherwise leave the
        // applier parked for the plugin's own one-minute give-up during teardown.
        try {
            String parked = startParkedDataNode();
            ClusterService parkedClusterService = awaitParkedAndVisible(parked, coordinator);
            assertHeldBack(coordinator, parkedClusterService);
            try (var mockLog = MockLog.capture(READINESS_LOGGER, DISPATCH_LOGGER)) {
                mockLog.addExpectation(notReleased(parked));
                mockLog.addExpectation(noWait(parked));
                try (var response = client(coordinator).execute(EsqlQueryAction.INSTANCE, request(dataset, true)).actionGet(TIMEOUT)) {
                    assertThat(response.isPartial(), equalTo(false));
                    assertThat(getValuesList(response), equalTo(List.of(List.of(ROWS, ID_SUM))));
                    assertThat("the joining worker must not have scanned", externalScanNodeNames(response), not(hasItem(parked)));
                    assertThat(externalScanNodeNames(response), equalTo(Set.copyOf(ready)));
                }
                mockLog.assertAllExpectationsMatched();
            }
            assertFalse("the window must still be open after the query", parkedClusterService.state().clusterRecovered());
            // The connection test leaves the joining node out as well: it has no credentials to probe with yet.
            Set<String> probed = nodesProbedFrom(coordinator);
            assertThat(probed, not(hasItem(parked)));
            assertThat(probed, hasItem(ready.get(1)));

            try (var mockLog = MockLog.capture(READINESS_LOGGER)) {
                mockLog.addExpectation(released(parked));
                ParkFirstApplyPlugin.releaseApplier();
                // Every node that held the worker logs this line, and the appender cannot tell which. The first
                // line can be another node's while this coordinator still has the worker pending, so the ready set
                // is what the following query and probe actually use.
                assertBusy(() -> assertReleased(coordinator, parkedClusterService));
                mockLog.awaitAllExpectationsMatched();
            }
            ensureStableCluster(4);
            try (var response = client(coordinator).execute(EsqlQueryAction.INSTANCE, request(dataset, true)).actionGet(TIMEOUT)) {
                assertThat(getValuesList(response), equalTo(List.of(List.of(ROWS, ID_SUM))));
                assertThat("released, the worker takes its share", externalScanNodeNames(response), hasItem(parked));
            }
            assertThat("released, the worker is probed", nodesProbedFrom(coordinator), hasItem(parked));
        } finally {
            ParkFirstApplyPlugin.releaseApplier();
        }
        ensureStableCluster(4);
    }

    /**
     * With the joining node the only worker there is, the query waits on the coordinator for it: nothing is sent to the
     * node (no dispatch wait line), and once its first apply completes the query runs, on that node.
     */
    @TestLogging(
        value = DISPATCH_LOGGER + ":DEBUG," + READINESS_LOGGER + ":DEBUG",
        reason = "waits for the coordinator to park the query on the joining worker before releasing it"
    )
    public void testLoneWorkerWithoutAnyAppliedStateIsWaitedFor() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        internalCluster().startMasterOnlyNode();
        // A coordinating-only node is not an external worker, so the parked node below is the coordinator's only choice.
        String coordinator = internalCluster().startCoordinatingOnlyNode(withWaitBudget(TimeValue.timeValueMinutes(1)));
        ensureStableCluster(2);
        String dataset = registerFixture("join_lone");

        try {
            String parked = startParkedDataNode();
            ClusterService parkedClusterService = awaitParkedAndVisible(parked, coordinator);
            assertHeldBack(coordinator, parkedClusterService);
            try (var mockLog = MockLog.capture(READINESS_LOGGER, DISPATCH_LOGGER)) {
                mockLog.addExpectation(coordinatorWaiting());
                mockLog.addExpectation(noWait(parked));
                ActionFuture<EsqlQueryResponse> future = client(coordinator).execute(EsqlQueryAction.INSTANCE, request(dataset, true));
                mockLog.awaitAllExpectationsMatched();
                assertFalse("the worker must still be parked while the coordinator waits", parkedClusterService.state().clusterRecovered());
                mockLog.addExpectation(released(parked));
                ParkFirstApplyPlugin.releaseApplier();
                try (var response = future.actionGet(TIMEOUT)) {
                    assertThat(response.isPartial(), equalTo(false));
                    assertThat(getValuesList(response), equalTo(List.of(List.of(ROWS, ID_SUM))));
                    assertThat("the released worker scanned everything", externalScanNodeNames(response), equalTo(Set.of(parked)));
                }
                mockLog.awaitAllExpectationsMatched();
            }
        } finally {
            ParkFirstApplyPlugin.releaseApplier();
        }
        ensureStableCluster(3);
    }

    /**
     * A query pinned to the coordinator ({@code external_distribution=coordinator_only}) reads its splits there whatever
     * workers there are, so it does not wait for the joining one: it completes while the node is still parked.
     */
    @TestLogging(
        value = DISPATCH_LOGGER + ":DEBUG," + READINESS_LOGGER + ":DEBUG",
        reason = "asserts the coordinator neither waited for nor sent anything to the joining worker"
    )
    public void testQueryPinnedToTheCoordinatorDoesNotWaitForTheLoneJoiningWorker() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        internalCluster().startMasterOnlyNode();
        String coordinator = internalCluster().startCoordinatingOnlyNode(withWaitBudget(TimeValue.timeValueMinutes(1)));
        ensureStableCluster(2);
        String dataset = registerFixture("join_lone_pinned");

        try {
            String parked = startParkedDataNode();
            ClusterService parkedClusterService = awaitParkedAndVisible(parked, coordinator);
            assertHeldBack(coordinator, parkedClusterService);
            try (var mockLog = MockLog.capture(DISPATCH_LOGGER, READINESS_LOGGER)) {
                mockLog.addExpectation(noCoordinatorWait());
                mockLog.addExpectation(noWait(parked));
                EsqlQueryRequest request = request(dataset, true, "coordinator_only");
                try (var response = client(coordinator).execute(EsqlQueryAction.INSTANCE, request).actionGet(TIMEOUT)) {
                    assertThat(response.isPartial(), equalTo(false));
                    assertThat(getValuesList(response), equalTo(List.of(List.of(ROWS, ID_SUM))));
                    assertThat("read on the coordinator", externalScanNodeNames(response), equalTo(Set.of(coordinator)));
                }
                mockLog.assertAllExpectationsMatched();
                assertFalse("the window must still be open after the query", parkedClusterService.state().clusterRecovered());
            }
        } finally {
            ParkFirstApplyPlugin.releaseApplier();
        }
        ensureStableCluster(3);
    }

    /** The coordinator's wait is bounded by its own {@code esql.external.worker_ready_timeout}; past it, the retryable 503. */
    @TestLogging(
        value = DISPATCH_LOGGER + ":DEBUG," + READINESS_LOGGER + ":DEBUG",
        reason = "asserts the coordinator waited and nothing was sent to the joining worker"
    )
    public void testLoneWorkerWithoutAnyAppliedStateFailsRetryablyWhenTheCoordinatorsWaitRunsOut() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        internalCluster().startMasterOnlyNode();
        String coordinator = internalCluster().startCoordinatingOnlyNode(withWaitBudget(TimeValue.timeValueMillis(100)));
        ensureStableCluster(2);
        String dataset = registerFixture("join_lone_timeout");

        try {
            String parked = startParkedDataNode();
            ClusterService parkedClusterService = awaitParkedAndVisible(parked, coordinator);
            assertHeldBack(coordinator, parkedClusterService);
            try (var mockLog = MockLog.capture(DISPATCH_LOGGER, READINESS_LOGGER)) {
                mockLog.addExpectation(coordinatorWaiting());
                mockLog.addExpectation(noWait(parked));
                EsqlQueryRequest request = request(dataset, false);
                Exception failure = expectThrows(
                    Exception.class,
                    () -> client(coordinator).execute(EsqlQueryAction.INSTANCE, request).actionGet(TIMEOUT).close()
                );
                assertThat(failure.toString(), ExceptionsHelper.status(failure), equalTo(RestStatus.SERVICE_UNAVAILABLE));
                assertTrue(failure.toString(), ExternalException.hasCondition(failure, ExternalException.Condition.WORKER_NOT_READY));
                assertThat(failure.toString(), containsString("on the coordinator"));
                mockLog.assertAllExpectationsMatched();
                assertFalse("the window must still be open after the query", parkedClusterService.state().clusterRecovered());
            }
        } finally {
            ParkFirstApplyPlugin.releaseApplier();
        }
        ensureStableCluster(3);
    }

    /**
     * An async STOP while the coordinator waits for the lone joining worker: there is no exchange and no driver to cut
     * yet, so STOP has nothing to cut and waits with the query; once the worker is released the query runs to completion
     * and STOP returns the full result, not a partial one.
     */
    @TestLogging(
        value = DISPATCH_LOGGER + ":DEBUG," + READINESS_LOGGER + ":DEBUG",
        reason = "waits for the coordinator to park the query on the joining worker before stopping it"
    )
    public void testAsyncStopDuringTheCoordinatorsWaitReturnsTheResultOnceTheWorkerIsReady() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        internalCluster().startMasterOnlyNode();
        String coordinator = internalCluster().startCoordinatingOnlyNode(withWaitBudget(TimeValue.timeValueMinutes(1)));
        ensureStableCluster(2);
        String dataset = registerFixture("join_lone_stop");

        try {
            String parked = startParkedDataNode();
            ClusterService parkedClusterService = awaitParkedAndVisible(parked, coordinator);
            assertHeldBack(coordinator, parkedClusterService);
            String asyncId = null;
            try (var mockLog = MockLog.capture(READINESS_LOGGER, DISPATCH_LOGGER)) {
                mockLog.addExpectation(coordinatorWaiting());
                mockLog.addExpectation(noWait(parked));
                EsqlQueryRequest request = EsqlQueryRequest.asyncEsqlQueryRequest("FROM " + dataset + " | STATS c = COUNT(*), s = SUM(id)");
                request.pragmas(
                    new QueryPragmas(Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "round_robin").build())
                );
                request.acceptedPragmaRisks(true);
                request.waitForCompletionTimeout(TimeValue.timeValueMillis(100));
                request.keepOnCompletion(true);
                try (var submitted = client(coordinator).execute(EsqlQueryAction.INSTANCE, request).actionGet(TIMEOUT)) {
                    assertTrue("still parked on the coordinator", submitted.isRunning());
                    asyncId = submitted.asyncExecutionId().orElseThrow();
                }
                mockLog.awaitAllExpectationsMatched();

                ActionFuture<EsqlQueryResponse> stopped = client(coordinator).execute(
                    EsqlAsyncStopAction.INSTANCE,
                    new AsyncStopRequest(asyncId)
                );
                // Nothing to cut: STOP cannot complete before the query does, and the query cannot before the release.
                assertFalse("STOP waits with the query", stopped.isDone());
                assertFalse("the worker must still be parked", parkedClusterService.state().clusterRecovered());
                ParkFirstApplyPlugin.releaseApplier();
                try (var response = stopped.actionGet(TIMEOUT)) {
                    assertThat(response.isRunning(), equalTo(false));
                    assertThat(getValuesList(response), equalTo(List.of(List.of(ROWS, ID_SUM))));
                    assertThat("nothing was cut, so nothing is partial", response.isPartial(), equalTo(false));
                }
            } finally {
                if (asyncId != null) {
                    deleteAsyncId(client(coordinator), asyncId);
                }
            }
        } finally {
            ParkFirstApplyPlugin.releaseApplier();
        }
        ensureStableCluster(3);
    }

    /**
     * Runs the data source connection test on {@code coordinator} and returns the names of the other nodes it probed. The
     * coordinator probes itself over its local connection, which the mock transport does not see.
     */
    private static Set<String> nodesProbedFrom(String coordinator) {
        Set<String> probed = ConcurrentHashMap.newKeySet();
        var transport = MockTransportService.getInstance(coordinator);
        transport.addSendBehavior((connection, requestId, action, request, options) -> {
            if (TestDataSourceNodeAction.TYPE.name().equals(action)) {
                probed.add(connection.getNode().getName());
            }
            connection.sendRequest(requestId, action, request, options);
        });
        try {
            client(coordinator).execute(
                TestDataSourceConnectionAction.INSTANCE,
                new TestDataSourceConnectionAction.Request("test", Map.of())
            ).actionGet(TIMEOUT);
        } finally {
            transport.clearAllRules();
        }
        return probed;
    }

    private static Settings withWaitBudget(TimeValue budget) {
        return Settings.builder().put(ExternalSourceSettings.WORKER_READY_TIMEOUT.getKey(), budget).build();
    }

    private static final String COORDINATOR_WAIT_LINE =
        "waiting up to [*] for an external worker to apply its cluster state before distributing [*] external splits, session [*";

    private static MockLog.SeenEventExpectation coordinatorWaiting() {
        return new MockLog.SeenEventExpectation("coordinator wait", READINESS_LOGGER, Level.DEBUG, COORDINATOR_WAIT_LINE);
    }

    private static MockLog.UnseenEventExpectation noCoordinatorWait() {
        return new MockLog.UnseenEventExpectation("no coordinator wait", READINESS_LOGGER, Level.DEBUG, COORDINATOR_WAIT_LINE);
    }

    /**
     * The coordinator sees the parked node in its cluster state and leaves it out of its external workers. The hold is
     * installed by an applier, before the state that adds the node is visible, so it cannot trail the state: no waiting.
     */
    private static void assertHeldBack(String coordinator, ClusterService parkedClusterService) {
        String parkedId = parkedClusterService.localNode().getId();
        DiscoveryNodes nodes = internalCluster().clusterService(coordinator).state().nodes();
        assertNotNull("the coordinator must see the joining node", nodes.get(parkedId));
        var readiness = internalCluster().getInstance(ExternalWorkerReadiness.class, coordinator);
        assertNull("held back", readiness.readyNodes(nodes).get(parkedId));
    }

    private static void assertReleased(String coordinator, ClusterService parkedClusterService) {
        String parkedId = parkedClusterService.localNode().getId();
        DiscoveryNodes nodes = internalCluster().clusterService(coordinator).state().nodes();
        assertNotNull(
            "released",
            internalCluster().getInstance(ExternalWorkerReadiness.class, coordinator).readyNodes(nodes).get(parkedId)
        );
    }

    private static MockLog.SeenEventExpectation released(String node) {
        return new MockLog.SeenEventExpectation(
            "release " + node,
            READINESS_LOGGER,
            Level.DEBUG,
            "external worker [" + node + "] is eligible for external splits: it has applied cluster state version [*"
        );
    }

    private static MockLog.UnseenEventExpectation notReleased(String node) {
        return new MockLog.UnseenEventExpectation(
            "no release of " + node,
            READINESS_LOGGER,
            Level.DEBUG,
            "external worker [" + node + "] is eligible for external splits*"
        );
    }

    /** A request to the parked node would park on its first state and say so; none must. */
    private static MockLog.UnseenEventExpectation noWait(String node) {
        return new MockLog.UnseenEventExpectation(
            "no wait on " + node,
            DISPATCH_LOGGER,
            Level.DEBUG,
            "waiting up to [*] for the cluster state before running [*] external splits on [" + node + "], session [*"
        );
    }

    private String startParkedDataNode() {
        ParkFirstApplyPlugin.arm();
        return internalCluster().startDataOnlyNode(
            Settings.builder()
                .put(ParkFirstApplyPlugin.PARK.getKey(), true)
                .put(Node.INITIAL_STATE_TIMEOUT_SETTING.getKey(), TimeValue.ZERO)
                // Nothing here should wait on the parked node; were something to, a generous bound keeps the failure a clear one.
                .put(ExternalSourceSettings.WORKER_READY_TIMEOUT.getKey(), TimeValue.timeValueMinutes(1))
                .build()
        );
    }

    /** The parked node is in its first apply, still reporting its initial state, and the coordinator already sees it. */
    private static ClusterService awaitParkedAndVisible(String parked, String coordinator) {
        ParkFirstApplyPlugin.awaitParked();
        ClusterService parkedClusterService = internalCluster().getInstance(ClusterService.class, parked);
        assertFalse(parkedClusterService.state().clusterRecovered());
        String parkedId = parkedClusterService.localNode().getId();
        awaitClusterState(coordinator, state -> state.nodes().get(parkedId) != null);
        return parkedClusterService;
    }

    private String registerFixture(String name) throws Exception {
        Path root = createTempDir().resolve(name);
        Files.createDirectories(root);
        for (int f = 0; f < FILES; f++) {
            StringBuilder body = new StringBuilder("id\n");
            for (int i = 0; i < ROWS_PER_FILE; i++) {
                body.append(f * ROWS_PER_FILE + i).append('\n');
            }
            Files.writeString(root.resolve("part_" + f + ".csv"), body.toString(), StandardCharsets.UTF_8);
        }
        registerDataSource(DATA_SOURCE, Map.of());
        registerDataset(name, DATA_SOURCE, StoragePath.fileUri(root) + "/*.csv", Map.of());
        return name;
    }

    private static EsqlQueryRequest request(String dataset, boolean profile) {
        return request(dataset, profile, "round_robin");
    }

    private static EsqlQueryRequest request(String dataset, boolean profile, String distribution) {
        var request = syncEsqlQueryRequest("FROM " + dataset + " | STATS c = COUNT(*), s = SUM(id)");
        request.pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), distribution).build()));
        request.acceptedPragmaRisks(true);
        request.profile(profile);
        return request;
    }
}

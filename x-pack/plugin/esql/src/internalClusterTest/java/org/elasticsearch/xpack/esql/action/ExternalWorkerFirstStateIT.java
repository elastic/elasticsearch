/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

/**
 * The join race behind elastic/esql-planning#2319: a node accepts transport requests before it has applied any cluster
 * state, the coordinator sees it as an external worker as soon as it applies the join, and every split sent to it until
 * its first apply completes lands on a node with no dataset, no key and default settings. With another worker available
 * the node declines and the coordinator hands its splits to that worker; as the only worker it waits for its first state.
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
        return CollectionUtils.appendToCopy(super.nodePlugins(), ParkFirstApplyPlugin.class);
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

    @TestLogging(
        value = DISPATCH_LOGGER + ":DEBUG",
        reason = "asserts the decline on the joining worker and the reroute on the coordinator"
    )
    public void testWorkerWithoutAnyAppliedStateIsRoutedAround() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        internalCluster().startMasterOnlyNode();
        List<String> ready = internalCluster().startDataOnlyNodes(2);
        ensureStableCluster(3);
        String coordinator = ready.get(0);
        String dataset = registerFixture("join_reroute");

        // Started inside the try: a start that fails after the node received its first state would otherwise leave the
        // applier parked for the plugin's own one-minute give-up during teardown.
        try {
            String parked = startParkedDataNode();
            ClusterService parkedClusterService = awaitParkedAndVisible(parked, coordinator);
            try (var mockLog = MockLog.capture(DISPATCH_LOGGER)) {
                mockLog.addExpectation(
                    new MockLog.SeenEventExpectation(
                        "decline on " + parked,
                        DISPATCH_LOGGER,
                        Level.DEBUG,
                        "declining [*] external splits on [" + parked + "], session [*"
                    )
                );
                mockLog.addExpectation(
                    new MockLog.SeenEventExpectation(
                        "reroute off " + parked,
                        DISPATCH_LOGGER,
                        Level.DEBUG,
                        "external worker [" + parked + "] has not applied the cluster state this query was planned from; rerouting [*"
                    )
                );
                try (var response = client(coordinator).execute(EsqlQueryAction.INSTANCE, request(dataset, true)).actionGet(TIMEOUT)) {
                    assertThat(response.isPartial(), equalTo(false));
                    assertThat(getValuesList(response), equalTo(List.of(List.of(ROWS, ID_SUM))));
                    assertThat("the joining worker must not have scanned", externalScanNodeNames(response), not(hasItem(parked)));
                }
                // The decline line proves the joining node was assigned splits; without it the answer above is vacuous.
                mockLog.assertAllExpectationsMatched();
            }
            assertFalse("the window must still be open after the query", parkedClusterService.state().clusterRecovered());
        } finally {
            ParkFirstApplyPlugin.releaseApplier();
        }
        ensureStableCluster(4);
    }

    @TestLogging(
        value = DISPATCH_LOGGER + ":DEBUG",
        reason = "waits for the lone worker to park on its first cluster state before releasing it"
    )
    public void testLoneWorkerWithoutAnyAppliedStateWaitsForIt() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        internalCluster().startMasterOnlyNode();
        // A coordinating-only node is not an external worker, so the parked node below is the coordinator's only choice.
        String coordinator = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        ensureStableCluster(2);
        String dataset = registerFixture("join_wait");

        try {
            String parked = startParkedDataNode();
            ClusterService parkedClusterService = awaitParkedAndVisible(parked, coordinator);
            try (var mockLog = MockLog.capture(DISPATCH_LOGGER)) {
                mockLog.addExpectation(
                    new MockLog.SeenEventExpectation(
                        "wait on " + parked,
                        DISPATCH_LOGGER,
                        Level.DEBUG,
                        "waiting up to [*] for the cluster state before running [*] external splits on [" + parked + "], session [*"
                    )
                );
                ActionFuture<EsqlQueryResponse> future = client(coordinator).execute(EsqlQueryAction.INSTANCE, request(dataset, false));
                mockLog.awaitAllExpectationsMatched();
                assertFalse("the worker must be parked while it waits", parkedClusterService.state().clusterRecovered());
                ParkFirstApplyPlugin.releaseApplier();
                try (var response = future.actionGet(TIMEOUT)) {
                    assertThat(response.isPartial(), equalTo(false));
                    assertThat(getValuesList(response), equalTo(List.of(List.of(ROWS, ID_SUM))));
                }
            }
        } finally {
            ParkFirstApplyPlugin.releaseApplier();
        }
        ensureStableCluster(3);
    }

    private String startParkedDataNode() {
        ParkFirstApplyPlugin.arm();
        return internalCluster().startDataOnlyNode(
            Settings.builder()
                .put(ParkFirstApplyPlugin.PARK.getKey(), true)
                .put(Node.INITIAL_STATE_TIMEOUT_SETTING.getKey(), TimeValue.ZERO)
                // Neither test is about the bound; a generous one keeps the lone-worker wait free of timing assumptions.
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
        var request = syncEsqlQueryRequest("FROM " + dataset + " | STATS c = COUNT(*), s = SUM(id)");
        request.pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "round_robin").build()));
        request.acceptedPragmaRisks(true);
        request.profile(profile);
        return request;
    }
}

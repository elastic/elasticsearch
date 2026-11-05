/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.downsample.DownsampleAction;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata.DownsampleTaskStatus;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.downsample.DownsampleShardPersistentTaskExecutor.DelegatingAction;
import org.elasticsearch.xpack.downsample.orchestration.DownsampleOrchestrator;
import org.elasticsearch.xpack.downsample.orchestration.DownsampleStateIndex;
import org.elasticsearch.xpack.downsample.orchestration.DownsampleStateStore;
import org.elasticsearch.xpack.downsample.orchestration.DownsampleStateStore.Versioned;
import org.elasticsearch.xpack.downsample.orchestration.IndexTaskDoc;
import org.elasticsearch.xpack.downsample.orchestration.IndexTaskPhase;
import org.elasticsearch.xpack.downsample.orchestration.ShardTaskDoc;
import org.elasticsearch.xpack.downsample.orchestration.ShardTaskPoller;
import org.elasticsearch.xpack.downsample.orchestration.ShardTaskStatus;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

/**
 * Downsampling through the system index survives the loss of the master and of the node running a shard task, without anyone
 * calling the downsample API again, and produces a complete target index.
 * <p>
 * To interrupt a shard task deterministically, {@link ParkingPlugin} parks the shard work ({@link DelegatingAction}) on chosen
 * nodes: the shard task is then {@code RUNNING} on that node but its indexer has not started.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class DownsampleSystemIndexDisruptionIT extends DownsamplingIntegTestCase {

    private static final Logger logger = LogManager.getLogger(DownsampleSystemIndexDisruptionIT.class);

    private static final String SOURCE = "source";
    private static final String TARGET = "target";
    private static final DateHistogramInterval INTERVAL = DateHistogramInterval.HOUR;
    private static final int DOC_COUNT = 300;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(ParkingPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(ShardTaskPoller.POLL_INTERVAL_SETTING.getKey(), "1s")
            .put(DownsampleOrchestrator.RECONCILE_INTERVAL_SETTING.getKey(), "100ms")
            .build();
    }

    @Before
    public void checkFeatureFlag() {
        assumeTrue("the system index path needs the feature flag", DownsampleStateIndex.FEATURE_FLAG.isEnabled());
    }

    @After
    public void releaseParked() {
        ParkingPlugin.parkingNodes.clear();
        ParkingPlugin.parked.clear();
        ParkingPlugin.stateWriteParkingNodes.clear();
        ParkingPlugin.parkedStateWrites.clear();
    }

    /**
     * The master stops while every shard task is running: the new master continues the operation, and the request, retried on the
     * new master, joins it and is acknowledged once it is done.
     */
    public void testMasterFailoverWhileShardTasksRun() throws Exception {
        internalCluster().startMasterOnlyNodes(3);
        List<String> dataNodes = internalCluster().startDataOnlyNodes(2);
        int shards = randomIntBetween(1, 2);
        createSource(shards);
        ParkingPlugin.parkingNodes.addAll(dataNodes);

        ActionFuture<AcknowledgedResponse> downsample = requestDownsample(randomFrom(dataNodes));
        assertBusy(() -> assertThat(ParkingPlugin.parked, hasSize(shards)));
        assertThat(indexTask().phase(), equalTo(IndexTaskPhase.RUN_SHARDS));
        assertThat(shardTasks(shards).stream().map(t -> t.doc().status()).toList(), everyItem(equalTo(ShardTaskStatus.RUNNING)));

        String oldMaster = internalCluster().getMasterName();
        internalCluster().stopCurrentMasterNode();
        ensureStableCluster(4);
        assertThat(internalCluster().getMasterName(), not(equalTo(oldMaster)));

        ParkingPlugin.release();
        assertTrue(downsample.actionGet(TimeValue.timeValueMinutes(2)).isAcknowledged());
        assertCompleteTarget(shards);
    }

    /**
     * The node running a shard task leaves: the master gives the task to the other node holding a copy of the source shard, at a
     * new epoch and without counting an attempt, and the operation completes.
     */
    public void testDataNodeLossWhileShardTaskRuns() throws Exception {
        String master = internalCluster().startMasterOnlyNode();
        List<String> dataNodes = internalCluster().startDataOnlyNodes(2);
        createSource(1);
        ParkingPlugin.parkingNodes.addAll(dataNodes);

        ActionFuture<AcknowledgedResponse> downsample = requestDownsample(master);
        assertBusy(() -> assertThat(ParkingPlugin.parked, hasSize(1)));
        ShardTaskDoc running = shardTasks(1).get(0).doc();
        assertThat(running.status(), equalTo(ShardTaskStatus.RUNNING));
        String taskNode = nodeName(running.assignedNode());
        String otherNode = dataNodes.stream().filter(n -> n.equals(taskNode) == false).findFirst().orElseThrow();

        // The target index is built without replicas (as on the persistent task path), so losing the node that holds a target
        // shard fails the operation. Keep the target away from the node that is about to leave: this test is about the shard task.
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", taskNode), TARGET);
        assertBusy(() -> assertThat(targetShardNodes(), everyItem(equalTo(otherNode))));

        ParkingPlugin.parkingNodes.remove(otherNode);
        internalCluster().stopNode(taskNode);

        assertTrue(downsample.actionGet(TimeValue.timeValueMinutes(2)).isAcknowledged());
        ShardTaskDoc completed = shardTasks(1).get(0).doc();
        assertThat(completed.status(), equalTo(ShardTaskStatus.COMPLETED));
        assertThat(nodeName(completed.assignedNode()), equalTo(otherNode));
        assertThat(completed.epoch(), greaterThan(running.epoch()));
        assertThat("leaving the cluster is not a failed attempt", completed.attempt(), equalTo(0));
        assertCompleteTarget(1);
    }

    /**
     * The node running a shard task leaves while the master fails over, and only the new master can notice: the old master's
     * reassignment never lands (its writes to the state index are parked), so the new master's first reconciliation must see that
     * the task is on a node that is gone, and reassign it.
     */
    public void testDataNodeLossDuringMasterFailover() throws Exception {
        List<String> masters = internalCluster().startMasterOnlyNodes(3);
        List<String> dataNodes = internalCluster().startDataOnlyNodes(2);
        createSource(1, 1);
        ParkingPlugin.parkingNodes.addAll(dataNodes);

        // via a master-eligible node that is not the master: it stays up for the whole test
        String oldMaster = internalCluster().getMasterName();
        ActionFuture<AcknowledgedResponse> downsample = requestDownsample(
            masters.stream().filter(n -> n.equals(oldMaster) == false).findFirst().orElseThrow()
        );
        assertBusy(() -> assertThat(ParkingPlugin.parked, hasSize(1)));
        ShardTaskDoc running = shardTasks(1).get(0).doc();
        String taskNode = nodeName(running.assignedNode());
        String otherNode = dataNodes.stream().filter(n -> n.equals(taskNode) == false).findFirst().orElseThrow();
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", taskNode), TARGET);
        assertBusy(() -> assertThat(targetShardNodes(), everyItem(equalTo(otherNode))));
        ParkingPlugin.parkingNodes.remove(otherNode);

        // the old master sees the node leave and tries to reassign the task, but that write is parked and never lands
        ParkingPlugin.stateWriteParkingNodes.add(oldMaster);
        internalCluster().stopNode(taskNode);
        assertBusy(() -> assertThat(ParkingPlugin.parkedStateWrites, not(hasSize(0))));
        internalCluster().stopCurrentMasterNode();

        assertTrue(downsample.actionGet(TimeValue.timeValueMinutes(2)).isAcknowledged());
        ShardTaskDoc completed = shardTasks(1).get(0).doc();
        assertThat(completed.status(), equalTo(ShardTaskStatus.COMPLETED));
        assertThat(nodeName(completed.assignedNode()), equalTo(otherNode));
        assertThat("only the new master reassigned the task", completed.epoch(), equalTo(running.epoch() + 1));
        assertThat(completed.attempt(), equalTo(0));
        assertCompleteTarget(1);
    }

    /**
     * The only data node, which holds the source, the target and the state index, restarts while its shard task runs. While it is
     * down the master cannot move the task (no other copy of the source) nor write the state index (its shard is on that node),
     * so after the restart the task is still {@code RUNNING} on the node, which has no local run any more: the poller resumes it with a
     * new run, and the operation completes.
     */
    public void testDataNodeRestartWhileShardTaskRuns() throws Exception {
        String master = internalCluster().startMasterOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        createSource(1, 0);
        ParkingPlugin.parkingNodes.add(dataNode);

        ActionFuture<AcknowledgedResponse> downsample = requestDownsample(master);
        assertBusy(() -> assertThat(ParkingPlugin.parked, hasSize(1)));
        ShardTaskDoc running = shardTasks(1).get(0).doc();
        assertThat(running.status(), equalTo(ShardTaskStatus.RUNNING));

        internalCluster().restartNode(dataNode, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                // the parked work died with the node; the restarted node runs its shard tasks normally
                ParkingPlugin.parkingNodes.remove(nodeName);
                ParkingPlugin.parked.clear();
                return Settings.EMPTY;
            }
        });

        assertTrue(downsample.actionGet(TimeValue.timeValueMinutes(2)).isAcknowledged());
        ShardTaskDoc completed = shardTasks(1).get(0).doc();
        assertThat(completed.status(), equalTo(ShardTaskStatus.COMPLETED));
        assertThat(nodeName(completed.assignedNode()), equalTo(dataNode));
        logger.info(
            "restart: epoch [{}] -> [{}], run [{}] -> [{}]",
            running.epoch(),
            completed.epoch(),
            running.runId(),
            completed.runId()
        );
        assertThat("a new run completed the task", completed.runId(), not(equalTo(running.runId())));
        assertThat("a restart is not a failed attempt", completed.attempt(), equalTo(0));
        assertCompleteTarget(1);
    }

    // ---- helpers ----

    private void createSource(int shards) throws Exception {
        createSource(shards, 1);
    }

    private void createSource(int shards, int replicas) throws Exception {
        assertAcked(
            indicesAdmin().prepareCreate(SOURCE)
                .setSettings(
                    indexSettings(shards, replicas).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                        .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of(FIELD_DIMENSION_KEYWORD))
                        .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2022-01-01T00:00:00Z")
                        .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2022-01-02T00:00:00Z")
                )
                .setMapping(
                    FIELD_TIMESTAMP,
                    "type=date",
                    FIELD_DIMENSION_KEYWORD,
                    "type=keyword,time_series_dimension=true",
                    FIELD_DIMENSION_LONG,
                    "type=long,time_series_dimension=true",
                    FIELD_METRIC_COUNTER_DOUBLE,
                    "type=double,time_series_metric=counter"
                )
        );
        ensureGreen(SOURCE);
        indexDocuments(SOURCE, DOC_COUNT, "2022-01-01T00:00:00");
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_BLOCKS_WRITE, true), SOURCE);
    }

    private ActionFuture<AcknowledgedResponse> requestDownsample(String viaNode) {
        return client(viaNode).execute(
            DownsampleAction.INSTANCE,
            new DownsampleAction.Request(
                TEST_REQUEST_TIMEOUT,
                SOURCE,
                TARGET,
                TimeValue.timeValueMinutes(5),
                new DownsampleConfig(INTERVAL, randomSamplingMethod())
            )
        );
    }

    /**
     * The target index is complete and successful: one document per (dimensions, interval) bucket of the source, computed
     * independently with a composite aggregation. The counter only grows, so there are no extra reset documents.
     */
    private void assertCompleteTarget(int shards) {
        IndexMetadata target = internalCluster().clusterService().state().metadata().getProject().index(TARGET);
        assertThat(IndexMetadata.INDEX_DOWNSAMPLE_STATUS.get(target.getSettings()), equalTo(DownsampleTaskStatus.SUCCESS));
        assertThat(indexTask().phase(), equalTo(IndexTaskPhase.DONE));
        assertThat(shardTasks(shards).stream().map(t -> t.doc().status()).toList(), everyItem(equalTo(ShardTaskStatus.COMPLETED)));
        assertHitCount(prepareSearch(TARGET).setSize(0).setTrackTotalHits(true), expectedBuckets());
    }

    private long expectedBuckets() {
        CompositeAggregationBuilder buckets = new CompositeAggregationBuilder(
            "buckets",
            List.of(
                new TermsValuesSourceBuilder("kw").field(FIELD_DIMENSION_KEYWORD),
                new TermsValuesSourceBuilder("long").field(FIELD_DIMENSION_LONG),
                new DateHistogramValuesSourceBuilder("ts").field(FIELD_TIMESTAMP).fixedInterval(INTERVAL)
            )
        ).size(10_000);
        SearchResponse response = prepareSearch(SOURCE).setSize(0).addAggregation(buckets).get();
        try {
            CompositeAggregation composite = response.getAggregations().get("buckets");
            assertThat("the buckets fit in one page", composite.getBuckets().size(), lessThan(10_000));
            return composite.getBuckets().size();
        } finally {
            response.decRef();
        }
    }

    private IndexTaskDoc indexTask() {
        PlainActionFuture<Optional<Versioned<IndexTaskDoc>>> future = new PlainActionFuture<>();
        store().forMaster().getIndexTask(ProjectId.DEFAULT, TARGET, future);
        return future.actionGet(TEST_REQUEST_TIMEOUT).orElseThrow().doc();
    }

    private List<Versioned<ShardTaskDoc>> shardTasks(int shards) {
        IndexTaskDoc indexTask = indexTask();
        PlainActionFuture<List<Optional<Versioned<ShardTaskDoc>>>> future = new PlainActionFuture<>();
        store().forMaster().getShardTasks(ProjectId.DEFAULT, TARGET, indexTask.targetIndexUuid(), shards, future);
        return future.actionGet(TEST_REQUEST_TIMEOUT).stream().map(Optional::orElseThrow).toList();
    }

    /**
     * Reads only, so any node's store does (the master view only checks mastership on writes).
     */
    private DownsampleStateStore store() {
        return internalCluster().getInstance(DownsampleStateStore.class);
    }

    private String nodeName(String nodeId) {
        return internalCluster().clusterService().state().nodes().get(nodeId).getName();
    }

    private List<String> targetShardNodes() {
        ClusterState state = internalCluster().clusterService().state();
        return state.routingTable(ProjectId.DEFAULT)
            .index(TARGET)
            .allShards()
            .flatMap(shard -> shard.activeShards().stream())
            .map(ShardRouting::currentNodeId)
            .map(id -> state.nodes().get(id).getName())
            .toList();
    }

    /**
     * Parks the shard work of downsampling on the nodes in {@link #parkingNodes}, until {@link #release()}. Parking does not block
     * a thread, so a node with parked work stops cleanly (its parked work is simply never run).
     */
    public static class ParkingPlugin extends Plugin implements ActionPlugin {

        static final Set<String> parkingNodes = ConcurrentHashMap.newKeySet();
        static final List<Runnable> parked = new CopyOnWriteArrayList<>();
        /** nodes whose writes to the state index are parked, and never resumed: e.g. a master whose decisions must not land */
        static final Set<String> stateWriteParkingNodes = ConcurrentHashMap.newKeySet();
        static final List<Object> parkedStateWrites = new CopyOnWriteArrayList<>();

        private volatile String nodeName;
        private volatile ThreadPool threadPool;

        @Override
        public Collection<?> createComponents(PluginServices services) {
            nodeName = services.clusterService().getNodeName();
            threadPool = services.threadPool();
            return List.of();
        }

        static void release() {
            parkingNodes.clear();
            List<Runnable> toRun = new ArrayList<>(parked);
            parked.clear();
            toRun.forEach(Runnable::run);
        }

        @Override
        public List<ActionFilter> getActionFilters() {
            return List.of(new ActionFilter() {
                @Override
                public int order() {
                    return Integer.MIN_VALUE;
                }

                @Override
                public <Request extends ActionRequest, Response extends ActionResponse> void apply(
                    Task task,
                    String action,
                    Request request,
                    ActionListener<Response> listener,
                    ActionFilterChain<Request, Response> chain
                ) {
                    if (TransportIndexAction.NAME.equals(action)
                        && request instanceof IndexRequest indexRequest
                        && DownsampleStateIndex.INDEX_NAME.equals(indexRequest.index())
                        && stateWriteParkingNodes.contains(nodeName)) {
                        parkedStateWrites.add(indexRequest);
                    } else if (DelegatingAction.NAME.equals(action) && parkingNodes.contains(nodeName)) {
                        // resume in the context of the shard work (a system context with the project id), not the releaser's
                        Supplier<ThreadContext.StoredContext> context = threadPool.getThreadContext().newRestorableContext(false);
                        parked.add(() -> {
                            try (ThreadContext.StoredContext ignored = context.get()) {
                                chain.proceed(task, action, request, listener);
                            }
                        });
                    } else {
                        chain.proceed(task, action, request, listener);
                    }
                }
            });
        }
    }
}

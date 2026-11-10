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
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.downsample.DownsampleAction;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.persistent.StartPersistentTaskAction;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.downsample.DownsampleShardTask;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

/**
 * Tests the resilience of downsampling regarding both multiple requests and disruptions. These are tests together because the
 * {@link DownsampleDisruptionPlugin} allows us to better different interleaving scenarios when we have multiple requests in flight.
 * <p>
 * The tests control the operation with {@link DownsampleDisruptionPlugin}, which parks or fails the actions it is made of, and
 * then assert that the downsample index is complete and identical to the result of a downsample of the same source that ran
 * without disruption.
 * <p>
 * Some disruptions make the downsample request fail. The API is designed to be retried in that case: a retry picks up the
 * downsample index left behind by the failed attempt. The tests that expect a failure retry the request, the others require
 * the original request to succeed.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class DownsampleResilienceIT extends DownsamplingIntegTestCase {

    private static final Logger logger = LogManager.getLogger(DownsampleResilienceIT.class);

    private static final String SOURCE = "source";
    private static final String TARGET = "target";
    private static final int DATA_NODES = 3;
    private static final int DOC_COUNT = 10_000;
    // Use this timeout for the downsample request to ensure it will not time out while the disruptions are in progress
    private static final TimeValue EXTENDED_WAIT_TIMEOUT = TimeValue.timeValueMinutes(2);

    private DownsampleConfig config;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(DownsampleDisruptionPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // A shard-level downsample task is not reassigned when its source shard relocates, so the task fails. Rebalancing can
            // relocate source shards at any time, for example when the downsample index is created, so it is disabled to have each
            // test exercise only the disruption it applies.
            .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), "none")
            .build();
    }

    @After
    public void resetDisruptionPlugin() {
        DownsampleDisruptionPlugin.reset();
    }

    /**
     * Sends a second request for the same downsample index while the first one is in progress.
     * Expected: both succeed but only one persistent task per shard is created.
     */
    public void testSecondRequestWhileFirstInProgress() throws Exception {
        final String coordinator = startCluster(1);
        final int shards = between(1, 3);
        createReadOnlyTimeSeriesIndex(SOURCE, shards, between(0, 1), DOC_COUNT);

        DownsampleDisruptionPlugin.parkAll(DownsampleDisruptionPlugin.downsamplingShardTask(SOURCE));
        final ActionFuture<AcknowledgedResponse> first = startDownsample(coordinator, TARGET);
        awaitParked(shards);

        // For the second request we only want to capture the start task request because it will
        // attach to the existing downsampling shard tasks
        DownsampleDisruptionPlugin.parkAll(DownsampleDisruptionPlugin.persistentTaskStart());
        final ActionFuture<AcknowledgedResponse> second = startDownsample(
            randomFrom(internalCluster().getNodeNames()),
            TARGET,
            EXTENDED_WAIT_TIMEOUT
        );
        awaitParked(2 * shards);
        DownsampleDisruptionPlugin.releaseParked(DownsampleDisruptionPlugin.persistentTaskStart());
        // Wait for the cluster state updates of the released start requests to be processed, so a duplicate task would be visible
        assertFalse(clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT).setWaitForEvents(Priority.LANGUID).get().isTimedOut());
        // Both requests share the persistent task of each shard, so there is exactly one per shard
        assertThat(shardTasks(), hasSize(shards));
        DownsampleDisruptionPlugin.release();
        assertDownsampleSucceeds(first);
        assertDownsampleSucceeds(second);
        assertDownsampleCompleted(coordinator, TARGET);
    }

    /**
     * Sends a second request while the first one is in progress, after every shard but one has completed. The shard-level task of the
     * remaining shard is parked.
     * Expected: both requests succeed, and the second request only attaches to the task of the unfinished shard, so no persistent task
     * is started again for the shards that already completed.
     */
    public void testSecondRequestDoesNotRetryCompletedShards() throws Exception {
        final String coordinator = startCluster(1);
        final int shards = between(2, 3);
        createReadOnlyTimeSeriesIndex(SOURCE, shards, between(0, 1), DOC_COUNT);

        // Park only one shard-level task, the others run to completion
        int parkedShardId = randomIntBetween(0, shards - 1);
        BiPredicate<String, ActionRequest> incompleteShard = DownsampleDisruptionPlugin.downsamplingShardTask(SOURCE, parkedShardId);
        DownsampleDisruptionPlugin.parkAll(incompleteShard);
        final ActionFuture<AcknowledgedResponse> first = startDownsample(coordinator, TARGET);
        awaitParked(1);
        // A persistent task is removed when it completes, and so is its running task on the node, so only the parked one is left
        assertBusy(() -> {
            assertThat(shardTasks(), hasSize(1));
            assertThat(clusterAdmin().prepareListTasks().setActions(DownsampleShardTask.TASK_NAME + "[c]").get().getTasks(), hasSize(1));
        }, 60, TimeUnit.SECONDS);
        assertThat(DownsampleDisruptionPlugin.parkedRequests(), hasSize(1));

        // We park the start of persistent tasks for the shards that have already completed, the second request should not issue any.
        // This has to be in place before the second request starts, and a single rule has to match all the completed shards.
        DownsampleDisruptionPlugin.parkAll((action, request) -> {
            if (StartPersistentTaskAction.INSTANCE.name().equals(action)
                && request instanceof StartPersistentTaskAction.Request start
                && DownsampleShardTask.TASK_NAME.equals(start.getTaskName())
                && start.getParams() instanceof DownsampleShardTaskParams params) {
                return params.shardId().getIndexName().equals(SOURCE) && params.shardId().getId() != parkedShardId;
            }
            return false;
        });

        // The second request should attach to the existing unfinished task
        final ActionFuture<AcknowledgedResponse> second = startDownsample(
            randomFrom(internalCluster().getNodeNames()),
            TARGET,
            EXTENDED_WAIT_TIMEOUT
        );
        DownsampleDisruptionPlugin.releaseParked(incompleteShard);
        assertDownsampleSucceeds(first);
        // The second request should be able to finish without creating any new persistent tasks that would be parked.
        assertDownsampleSucceeds(second);
        assertDownsampleCompleted(coordinator, TARGET);
    }

    /**
     * Downsamples the same source into several downsample indices concurrently.
     * Expected: every request succeeds and every downsample index is complete.
     */
    public void testConcurrentRequestsForDifferentTargets() throws Exception {
        final String coordinator = startCluster(1);
        createReadOnlyTimeSeriesIndex(SOURCE, between(1, 3), between(0, 1), DOC_COUNT);

        final List<String> targets = new ArrayList<>();
        final List<ActionFuture<AcknowledgedResponse>> requests = new ArrayList<>();
        for (int i = between(3, 6); i > 0; i--) {
            final String target = TARGET + "-" + i;
            targets.add(target);
            requests.add(startDownsample(randomFrom(internalCluster().getNodeNames()), target, EXTENDED_WAIT_TIMEOUT));
        }
        for (ActionFuture<AcknowledgedResponse> downsampleRequest : requests) {
            assertDownsampleSucceeds(downsampleRequest);
        }
        assertDownsampleCompleted(coordinator, targets.get(0));
        for (String target : targets.subList(1, targets.size())) {
            assertThat(
                IndexMetadata.INDEX_DOWNSAMPLE_STATUS.get(indexMetadata(target).getSettings()),
                equalTo(IndexMetadata.DownsampleTaskStatus.SUCCESS)
            );
            assertSameDocuments(targets.get(0), target);
        }
    }

    /**
     * Removes the shard-level persistent task after its first bulk request landed, and retries the request.
     * Expected: the original request fails, and the retry completes the operation, skipping the already indexed buckets as version
     * conflicts.
     */
    public void testInterruptedRunIsRetried() throws Exception {
        final String coordinator = startCluster(1);
        // One bucket per document, so the shard-level task sends several bulk requests. Either every bulk request holds other time
        // series, or all of them hold the same one.
        final boolean singleSeries = randomBoolean();
        createReadOnlyTimeSeriesIndex(
            SOURCE,
            1,
            between(0, 1),
            singleSeries ? 15_000 : 20_000,
            singleSeries ? TimeSeriesLayout.ONE_SERIES_ONE_DOCUMENT_PER_HOUR : TimeSeriesLayout.ONE_SERIES_PER_DOCUMENT
        );

        final long indexedByInterruptedRun = interruptAfterFirstBulk(coordinator);

        assertDownsampleSucceeds(startDownsample(coordinator, TARGET, EXTENDED_WAIT_TIMEOUT));
        final Map<String, DocSnapshot> docs = assertDownsampleCompleted(coordinator, TARGET);
        assertThat((long) docs.size(), greaterThan(indexedByInterruptedRun));
        assertNoDocumentRewritten(docs);
        assertThat(versionConflicts(TARGET), equalTo(indexedByInterruptedRun));
    }

    /**
     * Like {@link #testInterruptedRunIsRetried()}, muted until resuming from the last time series is reinstated, see
     * https://github.com/elastic/elasticsearch/issues/164609.
     * Expected: the retried task only recomputes the buckets of the last time series the interrupted run indexed.
     */
    public void testInterruptedRunResumesFromLastTimeSeries() throws Exception {
        final String coordinator = startCluster(1);
        createReadOnlyTimeSeriesIndex(SOURCE, 1, between(0, 1), 20_000, TimeSeriesLayout.ONE_SERIES_PER_DOCUMENT);

        final long indexedByInterruptedRun = interruptAfterFirstBulk(coordinator);

        assertDownsampleSucceeds(startDownsample(coordinator, TARGET, EXTENDED_WAIT_TIMEOUT));
        final Map<String, DocSnapshot> docs = assertDownsampleCompleted(coordinator, TARGET);
        assertThat((long) docs.size(), greaterThan(indexedByInterruptedRun));
        assertNoDocumentRewritten(docs);
        assertThat(versionConflicts(TARGET), allOf(greaterThan(0L), lessThan(indexedByInterruptedRun)));
    }

    /**
     * Fails the second bulk request of a shard-level task after every later one landed, leaving a gap in the downsample index.
     * Expected: the original request fails, and the retry fills the gap without indexing any document twice.
     */
    public void testFailedNonLastBulkIsRetried() throws Exception {
        final String coordinator = startCluster(1);
        // A time series per document and a bucket per time series, so consecutive bulk requests hold different time series and the
        // shard-level task has to send at least three of them
        createReadOnlyTimeSeriesIndex(SOURCE, 1, between(0, 1), 30_000, TimeSeriesLayout.ONE_SERIES_PER_DOCUMENT);
        final long buckets = countDocuments(SOURCE);

        final DownsampleDisruptionPlugin.OrdinalMatcher bulks = DownsampleDisruptionPlugin.ordinal(
            DownsampleDisruptionPlugin.actionOnIndex(TransportBulkAction.NAME, TARGET),
            ordinal -> ordinal == 1
        );
        DownsampleDisruptionPlugin.parkAll(bulks);
        final ActionFuture<AcknowledgedResponse> downsample = startDownsample(coordinator, TARGET);

        // Wait until every bulk request but the parked one landed, which leaves the shard-level task waiting for the parked one
        assertBusy(() -> {
            final List<ActionRequest> parked = DownsampleDisruptionPlugin.parkedRequests();
            assertThat(parked, hasSize(1));
            assertThat(countDocuments(TARGET), equalTo(buckets - ((BulkRequest) parked.get(0)).numberOfActions()));
        }, 60, TimeUnit.SECONDS);
        // At least one bulk request was sent after the parked one, so the parked one is not the last one
        assertThat(bulks.matched(), greaterThanOrEqualTo(3));

        DownsampleDisruptionPlugin.failParked();
        final Exception failure = expectThrows(Exception.class, () -> downsample.actionGet(TimeValue.THIRTY_SECONDS));
        assertThat(failure.getMessage(), containsString("] failed"));
        awaitNoShardTasks();
        assertThat(
            IndexMetadata.INDEX_DOWNSAMPLE_STATUS.get(indexMetadata(TARGET).getSettings()),
            equalTo(IndexMetadata.DownsampleTaskStatus.STARTED)
        );

        assertDownsampleSucceeds(startDownsample(coordinator, TARGET, EXTENDED_WAIT_TIMEOUT));
        final Map<String, DocSnapshot> docs = assertDownsampleCompleted(coordinator, TARGET);
        assertThat((long) docs.size(), equalTo(buckets));
        assertNoDocumentRewritten(docs);
    }

    /**
     * Starts downsampling {@link #SOURCE}, which must have a single shard, lets only the first bulk request of its shard-level task
     * through and removes the persistent task while the next bulk request is parked. This leaves a partially populated downsample
     * index in the {@code STARTED} status, as if the task had been interrupted.
     *
     * @return the number of documents the interrupted run indexed
     */
    private long interruptAfterFirstBulk(String coordinator) throws Exception {
        DownsampleDisruptionPlugin.parkAll(
            DownsampleDisruptionPlugin.ordinal(
                DownsampleDisruptionPlugin.actionOnIndex(TransportBulkAction.NAME, TARGET),
                ordinal -> ordinal > 0
            )
        );
        // The tiny wait timeout makes the request fail right after it created the downsample index and the shard-level task, so it
        // does not complete the downsample index once the task is removed
        expectThrows(
            Exception.class,
            () -> startDownsample(coordinator, TARGET, TimeValue.timeValueMillis(1)).actionGet(TimeValue.THIRTY_SECONDS)
        );

        // Wait until the first bulk request landed and the next one is parked
        assertBusy(() -> {
            assertThat(DownsampleDisruptionPlugin.parkedRequests(), not(empty()));
            assertThat(countDocuments(TARGET), greaterThan(0L));
        }, 60, TimeUnit.SECONDS);

        final PersistentTasksService persistentTasksService = internalCluster().getInstance(PersistentTasksService.class);
        final List<String> taskIds = shardTasks().stream().map(PersistentTasksCustomMetadata.PersistentTask::getId).toList();
        assertThat(taskIds, hasSize(1));
        logger.info("-> removing the persistent task {} while its next bulk request is in flight", taskIds);
        for (String taskId : taskIds) {
            safeAwait(
                (ActionListener<PersistentTasksCustomMetadata.PersistentTask<?>> listener) -> persistentTasksService
                    .sendProjectRemoveRequest(ProjectId.DEFAULT, taskId, TEST_REQUEST_TIMEOUT, listener)
            );
        }

        // Fail the parked bulk requests so the task that lost its persistent task stops without indexing anything else
        DownsampleDisruptionPlugin.failParked();
        awaitNoShardTasks();
        assertThat(
            IndexMetadata.INDEX_DOWNSAMPLE_STATUS.get(indexMetadata(TARGET).getSettings()),
            equalTo(IndexMetadata.DownsampleTaskStatus.STARTED)
        );
        final long indexed = countDocuments(TARGET);
        assertThat(indexed, greaterThan(0L));
        logger.info("-> the interrupted run indexed [{}] documents", indexed);
        return indexed;
    }

    private static List<PersistentTasksCustomMetadata.PersistentTask<?>> shardTasks() {
        final PersistentTasksCustomMetadata tasks = PersistentTasksCustomMetadata.get(
            clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState().metadata().getProject()
        );
        return tasks == null ? List.of() : List.copyOf(tasks.findTasks(DownsampleShardTask.TASK_NAME, task -> true));
    }

    /**
     * Waits until no downsample persistent task exists and no shard-level task runs on any node.
     */
    private static void awaitNoShardTasks() throws Exception {
        assertBusy(() -> {
            assertThat(shardTasks(), empty());
            assertThat(clusterAdmin().prepareListTasks().setActions(DownsampleShardTask.TASK_NAME + "[c]").get().getTasks(), empty());
        });
    }

    /**
     * Starts a master-only node per {@code masterNodes}, {@link #DATA_NODES} data nodes and a coordinating-only node, which
     * sends the downsample requests so that it is never the node a test disrupts.
     *
     * @return the name of the coordinating-only node
     */
    private String startCluster(int masterNodes) {
        internalCluster().startMasterOnlyNodes(masterNodes);
        internalCluster().startDataOnlyNodes(DATA_NODES);
        final String coordinator = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        ensureStableCluster(masterNodes + DATA_NODES + 1);
        config = new DownsampleConfig(new DateHistogramInterval("1h"), randomSamplingMethod());
        return coordinator;
    }

    private ActionFuture<AcknowledgedResponse> startDownsample(String coordinator, String target) {
        return startDownsample(coordinator, target, EXTENDED_WAIT_TIMEOUT);
    }

    /**
     * Sends a downsample request through {@code node}. The request waits up to {@code waitTimeout} for the shard-level tasks.
     */
    private ActionFuture<AcknowledgedResponse> startDownsample(String node, String target, TimeValue waitTimeout) {
        logger.info("-> downsampling [{}] to [{}] with [{}] through node [{}]", SOURCE, target, config, node);
        return client(node).execute(
            DownsampleAction.INSTANCE,
            new DownsampleAction.Request(TEST_REQUEST_TIMEOUT, SOURCE, target, waitTimeout, config)
        );
    }

    private static void assertDownsampleSucceeds(ActionFuture<AcknowledgedResponse> downsample) {
        assertTrue(safeGet(downsample).isAcknowledged());
    }

    /**
     * Asserts that the downsample index is complete, that no shard-level task is left behind and that the downsample index is
     * identical to the one a downsample of the same source produces without disruption.
     *
     * @return every document of the downsample index keyed by {@code _id}
     */
    private Map<String, DocSnapshot> assertDownsampleCompleted(String coordinator, String target) throws Exception {
        final IndexMetadata sourceMetadata = indexMetadata(SOURCE);
        final IndexMetadata targetMetadata = indexMetadata(target);
        assertThat(
            IndexMetadata.INDEX_DOWNSAMPLE_STATUS.get(targetMetadata.getSettings()),
            equalTo(IndexMetadata.DownsampleTaskStatus.SUCCESS)
        );
        assertThat(targetMetadata.getSettings().getAsBoolean(IndexMetadata.SETTING_BLOCKS_WRITE, false), equalTo(true));
        assertThat(targetMetadata.getNumberOfReplicas(), equalTo(sourceMetadata.getNumberOfReplicas()));
        assertThat(targetMetadata.getNumberOfShards(), equalTo(sourceMetadata.getNumberOfShards()));
        ensureGreen(target);
        assertBusy(() -> assertThat(shardTasks(), empty()));

        DownsampleDisruptionPlugin.reset();
        final String reference = "reference-" + target;
        assertTrue(safeGet(startDownsample(coordinator, reference)).isAcknowledged());
        return assertSameDocuments(reference, target);
    }

    private static IndexMetadata indexMetadata(String index) {
        return clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState().metadata().getProject().index(index);
    }
}

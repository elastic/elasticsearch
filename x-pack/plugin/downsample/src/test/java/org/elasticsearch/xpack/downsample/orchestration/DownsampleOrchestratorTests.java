/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample.orchestration;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata.DownsampleTaskStatus;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.downsample.Downsample;
import org.elasticsearch.xpack.downsample.DownsampleShardIndexerException;
import org.elasticsearch.xpack.downsample.orchestration.DownsampleStateStore.Versioned;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * Drives downsample operations through {@link DownsampleOrchestrator} on a single node with a real time series source index.
 * Most tests play the node through the store's node view, to control the shard tasks. The last ones run the real shard work: the
 * poller only polls when the test calls it, so each step is deterministic.
 */
public class DownsampleOrchestratorTests extends ESSingleNodeTestCase {

    private static final String SOURCE = "source";
    private static final String TARGET = "target";
    private static final int SOURCE_DOCS = 10;

    private DownsampleStateStore store;
    private DownsampleOrchestrator orchestrator;
    private ShardTaskPoller poller;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LocalStateCompositeXPackPlugin.class, Downsample.class, AggregateMetricMapperPlugin.class, DataStreamsPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .put(DownsampleOrchestrator.RECONCILE_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(200))
            // phase retries back off from 25ms, doubling: 10 attempts take about 13s, the 7th is due about 3s after the first failure
            .put(DownsampleOrchestrator.RETRY_DELAY_SETTING.getKey(), TimeValue.timeValueMillis(25))
            // the tests play the node, keep the real poller out of the way
            .put(ShardTaskPoller.POLL_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1))
            .build();
    }

    @Before
    public void setUpOrchestrator() {
        assumeTrue("the system index is only registered with the feature flag", DownsampleStateIndex.FEATURE_FLAG.isEnabled());
        store = getInstanceFromNode(DownsampleStateStore.class);
        orchestrator = getInstanceFromNode(DownsampleOrchestrator.class);
        poller = getInstanceFromNode(ShardTaskPoller.class);
    }

    public void testDrivesAnOperationToCompletion() throws Exception {
        int shards = between(1, 3);
        IndexMetadata source = createSource(shards);
        requestDownsample(source);

        // the master leaves the tasks to the nodes to claim
        List<Versioned<ShardTaskDoc>> unassigned = awaitShardTasks(shards, ShardTaskStatus.UNASSIGNED);
        assertThat(unassigned.stream().map(t -> t.doc().assignedNode()).toList(), everyItem(nullValue()));
        IndexMetadata target = index(TARGET);
        assertThat(IndexMetadata.INDEX_DOWNSAMPLE_STATUS.get(target.getSettings()), equalTo(DownsampleTaskStatus.STARTED));
        assertTrue(target.isHidden());
        assertBusy(() -> assertThat(indexTask().doc().phase(), equalTo(IndexTaskPhase.RUN_SHARDS)));
        assertThat(indexTask().doc().targetIndexUuid(), equalTo(target.getIndexUUID()));

        // play the node: claim, run and complete every shard task
        for (Versioned<ShardTaskDoc> task : unassigned) {
            Versioned<ShardTaskDoc> running = claim(task);
            nodeWrite(running, running.doc().completed(Map.of("in_num_docs_received", 1)), running.doc().epoch());
        }

        assertBusy(() -> assertThat(indexTask().doc().phase(), equalTo(IndexTaskPhase.DONE)));
        target = index(TARGET);
        assertThat(IndexMetadata.INDEX_DOWNSAMPLE_STATUS.get(target.getSettings()), equalTo(DownsampleTaskStatus.SUCCESS));
        assertThat(target.getSettings().get(IndexMetadata.SETTING_BLOCKS_WRITE), equalTo("true"));
        assertFalse(target.isHidden());
        assertThat(indexTask().doc().errorHistory(), hasSize(0));
    }

    public void testRetriesAFailedShardTask() throws Exception {
        IndexMetadata source = createSource(1);
        requestDownsample(source);
        Versioned<ShardTaskDoc> running = claim(awaitShardTasks(1, ShardTaskStatus.UNASSIGNED).get(0));
        long epoch = running.doc().epoch();
        nodeWrite(running, running.doc().failedOnNode("bulk failure", true, Map.of()), epoch);

        // the master makes the task claimable again, for another attempt at a new epoch
        assertBusy(() -> {
            ShardTaskDoc retried = shardTasks(1).get(0).doc();
            assertThat(retried.status(), equalTo(ShardTaskStatus.UNASSIGNED));
            assertThat(retried.assignedNode(), nullValue());
            assertThat(retried.attempt(), equalTo(1));
            assertThat(retried.epoch(), equalTo(epoch + 1));
            assertThat(retried.lastError(), equalTo("bulk failure"));
        });
        assertThat(indexTask().doc().phase(), equalTo(IndexTaskPhase.RUN_SHARDS));
    }

    public void testNonRetriableShardFailureFailsTheOperation() throws Exception {
        IndexMetadata source = createSource(1);
        requestDownsample(source);
        Versioned<ShardTaskDoc> running = claim(awaitShardTasks(1, ShardTaskStatus.UNASSIGNED).get(0));
        nodeWrite(running, running.doc().failedOnNode("cannot parse", false, Map.of()), running.doc().epoch());

        assertBusy(() -> assertThat(indexTask().doc().phase(), equalTo(IndexTaskPhase.FAILED)));
        assertThat(indexTask().doc().lastError(), containsString("cannot parse"));
    }

    public void testSourceDeletedFailsTheOperation() throws Exception {
        IndexMetadata source = createSource(1);
        assertAcked(client().admin().indices().prepareDelete(SOURCE));
        requestDownsample(source);
        assertBusy(() -> assertThat(indexTask().doc().phase(), equalTo(IndexTaskPhase.FAILED)));
        assertThat(indexTask().doc().lastError(), containsString("source index [" + SOURCE + "]"));
    }

    public void testExistingNonDownsampleTargetFailsTheOperation() throws Exception {
        IndexMetadata source = createSource(1);
        createIndex(TARGET);
        requestDownsample(source);
        assertBusy(() -> assertThat(indexTask().doc().phase(), equalTo(IndexTaskPhase.FAILED)));
        assertThat(indexTask().doc().lastError(), containsString("is not a downsample index"));
    }

    public void testTargetDeletedWhileRunningFailsTheOperation() throws Exception {
        IndexMetadata source = createSource(1);
        requestDownsample(source);
        awaitShardTasks(1, ShardTaskStatus.UNASSIGNED);
        assertBusy(() -> assertThat(indexTask().doc().phase(), equalTo(IndexTaskPhase.RUN_SHARDS)));
        assertAcked(client().admin().indices().prepareDelete(TARGET));
        assertBusy(() -> assertThat(indexTask().doc().phase(), equalTo(IndexTaskPhase.FAILED)));
        assertThat(indexTask().doc().lastError(), containsString("target index [" + TARGET + "]"));
    }

    // ---- with the real shard work ----

    public void testDownsamplesThroughThePoller() throws Exception {
        int shards = between(1, 3);
        IndexMetadata source = createSource(shards);
        requestDownsample(source);
        awaitShardTasks(shards, ShardTaskStatus.UNASSIGNED);

        poll();

        assertBusy(() -> assertThat(indexTask().doc().phase(), equalTo(IndexTaskPhase.DONE)));
        // every source document is in its own (dimension, hour) bucket
        assertHitCount(client().prepareSearch(TARGET).setSize(0), SOURCE_DOCS);
        long received = 0;
        for (Versioned<ShardTaskDoc> task : shardTasks(shards)) {
            assertThat(task.doc().status(), equalTo(ShardTaskStatus.COMPLETED));
            assertNotNull(task.doc().runId());
            received += ((Number) task.doc().progress().get("in_num_docs_received")).longValue();
        }
        assertThat(received, equalTo((long) SOURCE_DOCS));
        assertBusy(() -> assertThat(poller.activeRuns(), equalTo(0)));
    }

    /**
     * After a node restart, its tasks are still {@code RUNNING} on it but it has no local run: the poller starts a new run.
     */
    public void testResumesARunningTaskWithoutALocalRun() throws Exception {
        IndexMetadata source = createSource(1);
        requestDownsample(source);
        Versioned<ShardTaskDoc> task = awaitShardTasks(1, ShardTaskStatus.UNASSIGNED).get(0);
        PlainActionFuture<Versioned<ShardTaskDoc>> claimed = new PlainActionFuture<>();
        store.forLocalNode().claimShardTask(task, "before-restart", Map.of(), claimed);
        claimed.actionGet(TimeValue.timeValueSeconds(10));

        poll();

        assertBusy(() -> assertThat(indexTask().doc().phase(), equalTo(IndexTaskPhase.DONE)));
        ShardTaskDoc completed = shardTasks(1).get(0).doc();
        assertThat(completed.status(), equalTo(ShardTaskStatus.COMPLETED));
        assertNotEquals("before-restart", completed.runId());
        assertHitCount(client().prepareSearch(TARGET).setSize(0), SOURCE_DOCS);
    }

    // ---- retries and their limits ----

    /**
     * The shard work fails with a retriable error a few times (as when a bulk fails while the shard is scanned): every failure
     * reported by the node gives the task another attempt, and the operation completes.
     */
    public void testShardTaskIsRetriedUntilItSucceeds() throws Exception {
        int failures = between(1, DownsampleOrchestrator.MAX_SHARD_TASK_ATTEMPTS - 1);
        AtomicInteger runs = new AtomicInteger();
        ShardTaskPoller node = nodeWithIndexer(
            run -> runs.getAndIncrement() < failures ? new DownsampleShardIndexerException("bulk failure", true) : null
        );
        requestDownsample(createSource(1));

        assertBusy(() -> {
            poll(node);
            assertThat(indexTask().doc().phase(), equalTo(IndexTaskPhase.DONE));
        });
        ShardTaskDoc completed = shardTasks(1).get(0).doc();
        assertThat(completed.status(), equalTo(ShardTaskStatus.COMPLETED));
        assertThat(completed.attempt(), equalTo(failures));
        assertThat(runs.get(), equalTo(failures + 1));
    }

    /**
     * The shard work keeps failing with a retriable error: after {@link DownsampleOrchestrator#MAX_SHARD_TASK_ATTEMPTS} attempts the
     * master fails the task for good, and with it the operation, recording why.
     */
    public void testShardTaskFailsForGoodAfterMaxAttempts() throws Exception {
        AtomicInteger runs = new AtomicInteger();
        ShardTaskPoller node = nodeWithIndexer(run -> {
            runs.incrementAndGet();
            return new DownsampleShardIndexerException("bulk failure", true);
        });
        requestDownsample(createSource(1));

        assertBusy(() -> {
            poll(node);
            assertThat(indexTask().doc().phase(), equalTo(IndexTaskPhase.FAILED));
        });
        int maxAttempts = DownsampleOrchestrator.MAX_SHARD_TASK_ATTEMPTS;
        assertThat(runs.get(), equalTo(maxAttempts));
        ShardTaskDoc failed = shardTasks(1).get(0).doc();
        assertThat(failed.status(), equalTo(ShardTaskStatus.FAILED));
        assertFalse(failed.retriable());
        assertThat(failed.attempt(), equalTo(maxAttempts - 1));
        String error = indexTask().doc().lastError();
        assertThat(error, containsString("failed [" + maxAttempts + "] times"));
        assertThat(error, containsString("bulk failure"));
    }

    /**
     * A phase of the master fails with a retriable error (here, the target index is read only, so its settings cannot be finalized):
     * the failure is recorded and the phase retried after a delay, until it succeeds.
     */
    public void testFailedPhaseIsRetriedWithBackoff() throws Exception {
        requestDownsample(createSource(1));
        Versioned<ShardTaskDoc> running = claim(awaitShardTasks(1, ShardTaskStatus.UNASSIGNED).get(0));
        setTargetReadOnly(true);
        nodeWrite(running, running.doc().completed(Map.of()), running.doc().epoch());

        assertBusy(() -> {
            IndexTaskDoc retrying = indexTask().doc();
            assertThat(retrying.phase(), equalTo(IndexTaskPhase.FINALIZE_SETTINGS));
            assertThat(retrying.attempt(), greaterThan(0));
            assertThat(retrying.nextRetryAt(), greaterThan(0L));
            assertThat(retrying.lastError(), containsString("read-only"));
        });
        setTargetReadOnly(false);

        assertBusy(() -> assertThat(indexTask().doc().phase(), equalTo(IndexTaskPhase.DONE)));
        IndexTaskDoc done = indexTask().doc();
        assertThat("the next phases start without attempts", done.attempt(), equalTo(0));
        assertThat(done.errorHistory(), not(empty()));
        assertThat(done.errorHistory().get(0), containsString("read-only"));
    }

    /**
     * A phase of the master keeps failing: after {@link DownsampleOrchestrator#MAX_INDEX_TASK_ATTEMPTS} attempts the operation fails,
     * with the most recent errors kept.
     */
    public void testOperationFailsAfterMaxPhaseAttempts() throws Exception {
        requestDownsample(createSource(1));
        Versioned<ShardTaskDoc> running = claim(awaitShardTasks(1, ShardTaskStatus.UNASSIGNED).get(0));
        setTargetReadOnly(true);
        nodeWrite(running, running.doc().completed(Map.of()), running.doc().epoch());

        assertBusy(() -> assertThat(indexTask().doc().phase(), equalTo(IndexTaskPhase.FAILED)), 60, TimeUnit.SECONDS);
        IndexTaskDoc failed = indexTask().doc();
        assertThat(failed.attempt(), equalTo(DownsampleOrchestrator.MAX_INDEX_TASK_ATTEMPTS - 1));
        assertThat(failed.lastError(), containsString("read-only"));
        assertThat(failed.errorHistory(), hasSize(IndexTaskDoc.MAX_ERROR_HISTORY));
        setTargetReadOnly(false);
    }

    /**
     * Nobody runs the shard task: once the operation passes its deadline (the request's wait timeout) it fails, instead of waiting
     * forever.
     */
    public void testOperationFailsAfterItsDeadline() throws Exception {
        requestDownsample(createSource(1), TimeValue.timeValueSeconds(2));
        awaitShardTasks(1, ShardTaskStatus.UNASSIGNED);

        assertBusy(() -> assertThat(indexTask().doc().phase(), equalTo(IndexTaskPhase.FAILED)));
        assertThat(indexTask().doc().lastError(), containsString("deadline"));
    }

    // ---- helpers ----

    /**
     * One round of the poller, after a refresh: its search is near real time.
     */
    private void poll() {
        poll(poller);
    }

    /**
     * A poller like the node's, but its indexer is a fake: {@code outcome} returns the failure of a run, or {@code null} for a
     * successful one. The real node side code (runs, writes, fencing) runs around it.
     */
    private ShardTaskPoller nodeWithIndexer(Function<ShardTaskRun, Exception> outcome) {
        return new ShardTaskPoller(
            client(),
            store,
            getInstanceFromNode(ClusterService.class),
            getInstanceFromNode(ThreadPool.class),
            getInstanceFromNode(ProjectResolver.class),
            false,
            (run, completion) -> {
                Exception failure = outcome.apply(run);
                if (failure == null) {
                    completion.onResponse(null);
                } else {
                    completion.onFailure(failure);
                }
            },
            0
        );
    }

    private void setTargetReadOnly(boolean readOnly) {
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(TARGET)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_READ_ONLY, readOnly))
        );
    }

    private void poll(ShardTaskPoller nodePoller) {
        client().admin().indices().prepareRefresh(DownsampleStateIndex.INDEX_NAME).get();
        PlainActionFuture<Void> polled = new PlainActionFuture<>();
        nodePoller.poll(polled);
        polled.actionGet(TimeValue.timeValueSeconds(10));
    }

    private IndexMetadata createSource(int shards) {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of("dim"))
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2024-01-01T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2024-01-02T00:00:00Z")
            .build();
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(SOURCE)
                .setSettings(settings)
                .setMapping(
                    "@timestamp",
                    "type=date",
                    "dim",
                    "type=keyword,time_series_dimension=true",
                    "value",
                    "type=double,time_series_metric=gauge"
                )
        );
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < SOURCE_DOCS; i++) {
            bulk.add(
                client().prepareIndex(SOURCE)
                    .setSource(
                        "{\"@timestamp\":\"2024-01-01T0" + (i % 10) + ":00:00Z\",\"dim\":\"d" + (i % 3) + "\",\"value\":" + i + "}",
                        XContentType.JSON
                    )
            );
        }
        assertFalse(bulk.get().hasFailures());
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(SOURCE)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_BLOCKS_WRITE, true))
        );
        return index(SOURCE);
    }

    private void requestDownsample(IndexMetadata source) {
        requestDownsample(source, TimeValue.timeValueHours(1));
    }

    private void requestDownsample(IndexMetadata source, TimeValue deadlineIn) {
        long now = System.currentTimeMillis();
        IndexTaskDoc doc = IndexTaskDoc.newTask(
            ProjectId.DEFAULT,
            TARGET,
            SOURCE,
            source.getIndexUUID(),
            new DownsampleConfig(DateHistogramInterval.HOUR, null),
            now,
            now + deadlineIn.millis()
        );
        Versioned<IndexTaskDoc> created = call(l -> store.forMaster().createIndexTask(doc, l));
        assertThat(created.doc(), equalTo(doc));
        orchestrator.reconcile(ProjectId.DEFAULT, TARGET);
    }

    private List<Versioned<ShardTaskDoc>> awaitShardTasks(int shards, ShardTaskStatus status) throws Exception {
        List<Versioned<ShardTaskDoc>> tasks = new ArrayList<>();
        assertBusy(() -> {
            tasks.clear();
            tasks.addAll(shardTasks(shards));
            assertThat(tasks.stream().map(t -> t.doc().status()).toList(), everyItem(equalTo(status)));
        });
        return tasks;
    }

    private List<Versioned<ShardTaskDoc>> shardTasks(int shards) {
        Versioned<IndexTaskDoc> indexTask = indexTask();
        assertNotNull("the target index is not created yet", indexTask.doc().targetIndexUuid());
        List<Optional<Versioned<ShardTaskDoc>>> found = call(
            l -> store.forMaster().getShardTasks(ProjectId.DEFAULT, TARGET, indexTask.doc().targetIndexUuid(), shards, l)
        );
        return found.stream().map(Optional::orElseThrow).toList();
    }

    private Versioned<IndexTaskDoc> indexTask() {
        Optional<Versioned<IndexTaskDoc>> task = call(l -> store.forMaster().getIndexTask(ProjectId.DEFAULT, TARGET, l));
        return task.orElseThrow();
    }

    private Versioned<ShardTaskDoc> claim(Versioned<ShardTaskDoc> unassigned) {
        return call(l -> store.forLocalNode().claimShardTask(unassigned, randomUUID(), Map.of(), l));
    }

    private Versioned<ShardTaskDoc> nodeWrite(Versioned<ShardTaskDoc> current, ShardTaskDoc next, long heldEpoch) {
        return call(l -> store.forLocalNode().updateShardTask(current, next, heldEpoch, l));
    }

    private IndexMetadata index(String name) {
        return getInstanceFromNode(ClusterService.class).state().metadata().getProject().index(name);
    }

    private static <T> T call(Consumer<ActionListener<T>> operation) {
        PlainActionFuture<T> future = new PlainActionFuture<>();
        operation.accept(future);
        return future.actionGet(TimeValue.timeValueSeconds(10));
    }
}

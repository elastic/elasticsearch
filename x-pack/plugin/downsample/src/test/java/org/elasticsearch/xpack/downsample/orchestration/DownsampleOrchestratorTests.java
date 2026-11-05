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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.downsample.Downsample;
import org.elasticsearch.xpack.downsample.orchestration.DownsampleStateStore.Versioned;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;

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

        List<Versioned<ShardTaskDoc>> assigned = awaitShardTasks(shards, ShardTaskStatus.ASSIGNED);
        String localNode = store.forLocalNode().nodeId();
        assertThat(assigned.stream().map(t -> t.doc().assignedNode()).toList(), everyItem(equalTo(localNode)));
        IndexMetadata target = index(TARGET);
        assertThat(IndexMetadata.INDEX_DOWNSAMPLE_STATUS.get(target.getSettings()), equalTo(DownsampleTaskStatus.STARTED));
        assertTrue(target.isHidden());
        assertThat(indexTask().doc().phase(), equalTo(IndexTaskPhase.RUN_SHARDS));
        assertThat(indexTask().doc().targetIndexUuid(), equalTo(target.getIndexUUID()));

        // play the node: run and complete every shard task
        for (Versioned<ShardTaskDoc> task : assigned) {
            long epoch = task.doc().epoch();
            Versioned<ShardTaskDoc> running = nodeWrite(task, task.doc().running(randomUUID(), Map.of()), epoch);
            nodeWrite(running, running.doc().completed(Map.of("in_num_docs_received", 1)), epoch);
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
        Versioned<ShardTaskDoc> task = awaitShardTasks(1, ShardTaskStatus.ASSIGNED).get(0);
        long epoch = task.doc().epoch();
        Versioned<ShardTaskDoc> running = nodeWrite(task, task.doc().running(randomUUID(), Map.of()), epoch);
        nodeWrite(running, running.doc().failedOnNode("bulk failure", true, Map.of()), epoch);

        // the master gives the task another attempt, here on the same (only) node at a new epoch
        assertBusy(() -> {
            ShardTaskDoc retried = shardTasks(1).get(0).doc();
            assertThat(retried.status(), equalTo(ShardTaskStatus.ASSIGNED));
            assertThat(retried.attempt(), equalTo(1));
            assertThat(retried.epoch(), equalTo(epoch + 1));
            assertThat(retried.lastError(), equalTo("bulk failure"));
        });
        assertThat(indexTask().doc().phase(), equalTo(IndexTaskPhase.RUN_SHARDS));
    }

    public void testNonRetriableShardFailureFailsTheOperation() throws Exception {
        IndexMetadata source = createSource(1);
        requestDownsample(source);
        Versioned<ShardTaskDoc> task = awaitShardTasks(1, ShardTaskStatus.ASSIGNED).get(0);
        long epoch = task.doc().epoch();
        Versioned<ShardTaskDoc> running = nodeWrite(task, task.doc().running(randomUUID(), Map.of()), epoch);
        nodeWrite(running, running.doc().failedOnNode("cannot parse", false, Map.of()), epoch);

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
        awaitShardTasks(1, ShardTaskStatus.ASSIGNED);
        assertAcked(client().admin().indices().prepareDelete(TARGET));
        assertBusy(() -> assertThat(indexTask().doc().phase(), equalTo(IndexTaskPhase.FAILED)));
        assertThat(indexTask().doc().lastError(), containsString("target index [" + TARGET + "]"));
    }

    // ---- with the real shard work ----

    public void testDownsamplesThroughThePoller() throws Exception {
        int shards = between(1, 3);
        IndexMetadata source = createSource(shards);
        requestDownsample(source);
        awaitShardTasks(shards, ShardTaskStatus.ASSIGNED);

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
        Versioned<ShardTaskDoc> task = awaitShardTasks(1, ShardTaskStatus.ASSIGNED).get(0);
        nodeWrite(task, task.doc().running("before-restart", Map.of()), task.doc().epoch());

        poll();

        assertBusy(() -> assertThat(indexTask().doc().phase(), equalTo(IndexTaskPhase.DONE)));
        ShardTaskDoc completed = shardTasks(1).get(0).doc();
        assertThat(completed.status(), equalTo(ShardTaskStatus.COMPLETED));
        assertNotEquals("before-restart", completed.runId());
        assertHitCount(client().prepareSearch(TARGET).setSize(0), SOURCE_DOCS);
    }

    // ---- helpers ----

    /**
     * One round of the poller, after a refresh: its search is near real time.
     */
    private void poll() {
        client().admin().indices().prepareRefresh(DownsampleStateIndex.INDEX_NAME).get();
        PlainActionFuture<Void> polled = new PlainActionFuture<>();
        poller.poll(polled);
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
        long now = System.currentTimeMillis();
        IndexTaskDoc doc = IndexTaskDoc.newTask(
            ProjectId.DEFAULT,
            TARGET,
            SOURCE,
            source.getIndexUUID(),
            new DownsampleConfig(DateHistogramInterval.HOUR, null),
            now,
            now + TimeValue.timeValueHours(1).millis()
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

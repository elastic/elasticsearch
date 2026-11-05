/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample.orchestration;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.downsample.Downsample;
import org.elasticsearch.xpack.downsample.DownsampleShardIndexerException;
import org.elasticsearch.xpack.downsample.orchestration.DownsampleStateStore.Versioned;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * The node side: how {@link ShardTaskPoller} and {@link ShardTaskRun} react to what they find in the state index, in particular when
 * a task is taken away from the node while it runs (fencing) and when the poller's near real time search is stale.
 * <p>
 * The poller under test is not the node's (which polls every hour here, so it stays out of the way): it is driven by the tests, and
 * its indexer is a fake that records each run, so the tests decide when and how a run completes.
 */
public class ShardTaskPollerTests extends ESSingleNodeTestCase {

    private static final ProjectId PROJECT = ProjectId.DEFAULT;

    private DownsampleStateStore store;
    private ShardTaskPoller poller;
    private final List<Tuple<ShardTaskRun, ActionListener<Void>>> started = new CopyOnWriteArrayList<>();
    private String targetUuid;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LocalStateCompositeXPackPlugin.class, Downsample.class, AggregateMetricMapperPlugin.class, DataStreamsPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .put(ShardTaskPoller.POLL_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1))
            .build();
    }

    @Before
    public void setUpPoller() {
        assumeTrue("the system index path needs the feature flag", DownsampleStateIndex.FEATURE_FLAG.isEnabled());
        store = getInstanceFromNode(DownsampleStateStore.class);
        poller = new ShardTaskPoller(
            client(),
            store,
            getInstanceFromNode(ClusterService.class),
            getInstanceFromNode(ThreadPool.class),
            getInstanceFromNode(ProjectResolver.class),
            false,
            (run, completion) -> started.add(new Tuple<>(run, completion))
        );
        createIndex("target");
        targetUuid = getInstanceFromNode(ClusterService.class).state().metadata().getProject().index("target").getIndexUUID();
    }

    public void testRunsAndCompletesItsTask() throws Exception {
        Versioned<ShardTaskDoc> assigned = assignedToThisNode();
        poll();
        ShardTaskRun run = awaitStarted(1);
        ShardTaskDoc running = read(assigned).doc();
        assertThat(running.status(), equalTo(ShardTaskStatus.RUNNING));
        assertThat(running.runId(), equalTo(run.runId()));

        completeRun(0, l -> l.onResponse(null));
        assertBusy(() -> assertThat(read(assigned).doc().status(), equalTo(ShardTaskStatus.COMPLETED)));
        assertBusy(() -> assertThat(poller.activeRuns(), equalTo(0)));
    }

    public void testReportsWhetherAFailureIsRetriable() throws Exception {
        Versioned<ShardTaskDoc> assigned = assignedToThisNode();
        poll();
        awaitStarted(1);
        boolean retriable = randomBoolean();
        completeRun(0, l -> l.onFailure(new DownsampleShardIndexerException("bulk failure", retriable)));
        assertBusy(() -> {
            ShardTaskDoc failed = read(assigned).doc();
            assertThat(failed.status(), equalTo(ShardTaskStatus.FAILED));
            assertThat(failed.retriable(), equalTo(retriable));
        });
    }

    /**
     * The search is near real time (and may hit a lagging copy): a task missing from it is not a reason to stop its run. A real
     * time read shows it is still ours, so the run goes on.
     */
    public void testMissingSearchHitDoesNotCancelTheRun() throws Exception {
        Versioned<ShardTaskDoc> assigned = assignedToThisNode();
        poll();
        ShardTaskRun run = awaitStarted(1);

        reconcile(List.of());
        assertFalse(run.isCancelled());
        assertThat(poller.activeRuns(), equalTo(1));

        completeRun(0, l -> l.onResponse(null));
        assertBusy(() -> assertThat(read(assigned).doc().status(), equalTo(ShardTaskStatus.COMPLETED)));
    }

    /**
     * A stale hit with an older epoch of the task (here: from before the task was taken away and given back) must not replace
     * the current run with one that is fenced from the start.
     */
    public void testStaleHitDoesNotReplaceTheRun() throws Exception {
        Versioned<ShardTaskDoc> firstAssignment = assignedToThisNode();
        Versioned<ShardTaskDoc> unassigned = masterWrite(firstAssignment, firstAssignment.doc().unassign());
        Versioned<ShardTaskDoc> reassigned = masterWrite(unassigned, unassigned.doc().assignTo(nodeId(), true));
        poll();
        ShardTaskRun run = awaitStarted(1);
        assertThat(run.epoch(), equalTo(reassigned.doc().epoch()));

        reconcile(List.of(firstAssignment));
        assertFalse(run.isCancelled());
        assertThat("no run was started for the stale hit", started, hasSize(1));
        assertThat(poller.activeRuns(), equalTo(1));
    }

    /**
     * The master took the task away: once a real time read confirms it, the run is cancelled, and it writes nothing when its
     * indexer completes.
     */
    public void testReassignedTaskIsCancelledAfterConfirmation() throws Exception {
        Versioned<ShardTaskDoc> assigned = assignedToThisNode();
        poll();
        ShardTaskRun run = awaitStarted(1);
        Versioned<ShardTaskDoc> reassigned = masterWrite(read(assigned), read(assigned).doc().assignTo("another-node", false));

        reconcile(List.of());
        assertTrue(run.isCancelled());
        assertThat(poller.activeRuns(), equalTo(0));

        completeRun(0, l -> l.onResponse(null));
        assertThat("the cancelled run did not write", read(assigned), equalTo(reassigned));
    }

    /**
     * The master took the task away, and the run finds out by itself on its next write (a progress write after a bulk): the version
     * conflict, confirmed by a real time read, fences it.
     */
    public void testRunIsFencedByItsNextWrite() throws Exception {
        Versioned<ShardTaskDoc> assigned = assignedToThisNode();
        poll();
        ShardTaskRun run = awaitStarted(1);
        Versioned<ShardTaskDoc> reassigned = masterWrite(read(assigned), read(assigned).doc().assignTo("another-node", false));

        run.onBulkCompleted();
        assertBusy(() -> assertTrue(run.isCancelled()));

        completeRun(0, l -> l.onResponse(null));
        assertThat("the fenced run did not write", read(assigned), equalTo(reassigned));
    }

    /**
     * A run started from an older version of a task that is still ours at the same epoch (e.g. from a stale search hit, after an
     * earlier run of this node marked it running): its first write conflicts, the real time read shows the task is still ours, so
     * it retries and goes on.
     */
    public void testBenignConflictIsRetried() throws Exception {
        Versioned<ShardTaskDoc> assigned = assignedToThisNode();
        PlainActionFuture<Versioned<ShardTaskDoc>> earlierRun = new PlainActionFuture<>();
        store.forLocalNode().updateShardTask(assigned, assigned.doc().running("earlier-run", Map.of()), assigned.doc().epoch(), earlierRun);
        earlierRun.actionGet(TimeValue.timeValueSeconds(10));

        List<ActionListener<Void>> indexer = new CopyOnWriteArrayList<>();
        ShardTaskRun run = new ShardTaskRun(store.forLocalNode(), assigned, finished -> {});
        run.start(indexer::add);
        assertBusy(() -> assertThat(indexer, hasSize(1)));
        assertFalse(run.isCancelled());
        assertThat(read(assigned).doc().runId(), equalTo(run.runId()));

        indexer.get(0).onResponse(null);
        assertBusy(() -> assertThat(read(assigned).doc().status(), equalTo(ShardTaskStatus.COMPLETED)));
    }

    // ---- helpers ----

    private Versioned<ShardTaskDoc> assignedToThisNode() {
        ShardTaskDoc doc = ShardTaskDoc.newTask(
            PROJECT,
            "target",
            targetUuid,
            OrchestrationTestUtils.randomParams("target", new ShardId(new Index("source", randomUUID()), 0))
        );
        PlainActionFuture<Versioned<ShardTaskDoc>> created = new PlainActionFuture<>();
        store.forMaster().createShardTask(doc, created);
        Versioned<ShardTaskDoc> unassigned = created.actionGet(TimeValue.timeValueSeconds(10));
        return masterWrite(unassigned, unassigned.doc().assignTo(nodeId(), false));
    }

    private Versioned<ShardTaskDoc> masterWrite(Versioned<ShardTaskDoc> current, ShardTaskDoc next) {
        PlainActionFuture<Versioned<ShardTaskDoc>> written = new PlainActionFuture<>();
        store.forMaster().updateShardTask(current, next, written);
        return written.actionGet(TimeValue.timeValueSeconds(10));
    }

    private Versioned<ShardTaskDoc> read(Versioned<ShardTaskDoc> task) {
        PlainActionFuture<Optional<Versioned<ShardTaskDoc>>> found = new PlainActionFuture<>();
        store.forLocalNode().getShardTask(PROJECT, task.doc().id(), found);
        return found.actionGet(TimeValue.timeValueSeconds(10)).orElseThrow();
    }

    private void poll() {
        client().admin().indices().prepareRefresh(DownsampleStateIndex.INDEX_NAME).get();
        PlainActionFuture<Void> polled = new PlainActionFuture<>();
        poller.poll(polled);
        polled.actionGet(TimeValue.timeValueSeconds(10));
    }

    /**
     * Reconciles the local runs with the given search result, as if the poller's search had returned it.
     */
    private void reconcile(List<Versioned<ShardTaskDoc>> found) {
        PlainActionFuture<Void> reconciled = new PlainActionFuture<>();
        poller.reconcile(store.forLocalNode(), PROJECT, found, reconciled);
        reconciled.actionGet(TimeValue.timeValueSeconds(10));
    }

    private ShardTaskRun awaitStarted(int runs) throws Exception {
        assertBusy(() -> assertThat(started, hasSize(runs)));
        return started.get(runs - 1).v1();
    }

    private void completeRun(int index, Consumer<ActionListener<Void>> outcome) {
        outcome.accept(started.get(index).v2());
    }

    private String nodeId() {
        return store.forLocalNode().nodeId();
    }
}

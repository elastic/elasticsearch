/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample.orchestration;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * The node side: how {@link ShardTaskPoller} and {@link ShardTaskRun} claim and run tasks, in particular when a claim loses a race,
 * when a task is taken away from the node while it runs (fencing) and when the poller's near real time search is stale.
 * <p>
 * The poller under test is not the node's (which polls every hour here, so it stays out of the way): it is driven by the tests, and
 * its indexer is a fake that records each run, so the tests decide when and how a run completes.
 */
public class ShardTaskPollerTests extends ESSingleNodeTestCase {

    private static final ProjectId PROJECT = ProjectId.DEFAULT;

    private DownsampleStateStore store;
    private ShardTaskPoller poller;
    private final List<Tuple<ShardTaskRun, ActionListener<Void>>> started = new CopyOnWriteArrayList<>();
    private Index source;
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
        poller = newPoller(0);
        // every shard of the source index is on this node, so this node can claim all of their tasks
        source = createIndex("source", Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3).build()).index();
        targetUuid = createIndex("target").index().getUUID();
    }

    public void testClaimsRunsAndCompletesATask() throws Exception {
        Versioned<ShardTaskDoc> unassigned = unassignedTask(0);
        poll();
        ShardTaskRun run = awaitStarted(1);
        ShardTaskDoc running = read(unassigned).doc();
        assertThat(running.status(), equalTo(ShardTaskStatus.RUNNING));
        assertThat(running.assignedNode(), equalTo(nodeId()));
        assertThat(running.epoch(), equalTo(unassigned.doc().epoch() + 1));
        assertThat(running.runId(), equalTo(run.runId()));
        assertThat(run.epoch(), equalTo(running.epoch()));

        completeRun(0, l -> l.onResponse(null));
        assertBusy(() -> assertThat(read(unassigned).doc().status(), equalTo(ShardTaskStatus.COMPLETED)));
        assertBusy(() -> assertThat(poller.activeRuns(), equalTo(0)));
    }

    public void testReportsWhetherAFailureIsRetriable() throws Exception {
        Versioned<ShardTaskDoc> unassigned = unassignedTask(0);
        poll();
        awaitStarted(1);
        boolean retriable = randomBoolean();
        completeRun(0, l -> l.onFailure(new DownsampleShardIndexerException("bulk failure", retriable)));
        assertBusy(() -> {
            ShardTaskDoc failed = read(unassigned).doc();
            assertThat(failed.status(), equalTo(ShardTaskStatus.FAILED));
            assertThat(failed.retriable(), equalTo(retriable));
        });
    }

    /**
     * The task was claimed (here by another run, as it could be by another node) between the search and the claim: the claim loses
     * the compare-and-set, nothing runs.
     */
    public void testLostClaimRunsNothing() throws Exception {
        Versioned<ShardTaskDoc> unassigned = unassignedTask(0);
        PlainActionFuture<Versioned<ShardTaskDoc>> otherClaim = new PlainActionFuture<>();
        store.forLocalNode().claimShardTask(unassigned, "other-run", Map.of(), otherClaim);
        Versioned<ShardTaskDoc> claimedElsewhere = otherClaim.actionGet(TimeValue.timeValueSeconds(10));

        // the claim of this run is based on the version the search returned
        AtomicBoolean done = new AtomicBoolean();
        List<ActionListener<Void>> indexer = new CopyOnWriteArrayList<>();
        ShardTaskRun run = new ShardTaskRun(store.forLocalNode(), unassigned, finished -> done.set(true));
        run.start(indexer::add);
        assertBusy(() -> assertTrue(done.get()));
        assertThat(indexer, empty());
        assertFalse(run.hasStarted());
        assertThat(read(unassigned), equalTo(claimedElsewhere));
    }

    /**
     * The search is near real time (and may hit a lagging copy): a task missing from it is not a reason to stop its run. A real
     * time read shows it is still ours, so the run goes on.
     */
    public void testMissingSearchHitDoesNotCancelTheRun() throws Exception {
        Versioned<ShardTaskDoc> unassigned = unassignedTask(0);
        poll();
        ShardTaskRun run = awaitStarted(1);

        reconcile(List.of());
        assertFalse(run.isCancelled());
        assertThat(poller.activeRuns(), equalTo(1));

        completeRun(0, l -> l.onResponse(null));
        assertBusy(() -> assertThat(read(unassigned).doc().status(), equalTo(ShardTaskStatus.COMPLETED)));
    }

    /**
     * A stale hit with an older epoch of the task (here: from before the task was released and claimed again by this node) must
     * not replace the current run with one that is fenced from the start.
     */
    public void testStaleHitDoesNotReplaceTheRun() throws Exception {
        Versioned<ShardTaskDoc> unassigned = unassignedTask(0);
        poll();
        awaitStarted(1);
        Versioned<ShardTaskDoc> firstClaim = read(unassigned);
        masterWrite(firstClaim, firstClaim.doc().unassign());
        reconcile(List.of());
        assertThat(poller.activeRuns(), equalTo(0));

        poll();
        ShardTaskRun run = awaitStarted(2);
        assertThat(run.epoch(), equalTo(firstClaim.doc().epoch() + 2));

        reconcile(List.of(firstClaim));
        assertFalse(run.isCancelled());
        assertThat("no run was started for the stale hit", started, hasSize(2));
        assertThat(poller.activeRuns(), equalTo(1));
    }

    /**
     * The master released the task: once a real time read confirms it, the run is cancelled, and it writes nothing when its
     * indexer completes.
     */
    public void testReleasedTaskIsCancelledAfterConfirmation() throws Exception {
        Versioned<ShardTaskDoc> unassigned = unassignedTask(0);
        poll();
        ShardTaskRun run = awaitStarted(1);
        Versioned<ShardTaskDoc> released = masterWrite(read(unassigned), read(unassigned).doc().unassign());

        reconcile(List.of());
        assertTrue(run.isCancelled());
        assertThat(poller.activeRuns(), equalTo(0));

        completeRun(0, l -> l.onResponse(null));
        assertThat("the cancelled run did not write", read(unassigned), equalTo(released));
    }

    /**
     * The master released the task, and the run finds out by itself on its next write (a progress write after a bulk): the version
     * conflict, confirmed by a real time read, fences it.
     */
    public void testRunIsFencedByItsNextWrite() throws Exception {
        Versioned<ShardTaskDoc> unassigned = unassignedTask(0);
        poll();
        ShardTaskRun run = awaitStarted(1);
        Versioned<ShardTaskDoc> released = masterWrite(read(unassigned), read(unassigned).doc().unassign());

        run.onBulkCompleted();
        assertBusy(() -> assertTrue(run.isCancelled()));

        completeRun(0, l -> l.onResponse(null));
        assertThat("the fenced run did not write", read(unassigned), equalTo(released));
    }

    /**
     * A run resumed from an older version of a task that is still ours at the same epoch (e.g. from a stale search hit, after an
     * earlier run of this node wrote progress): its first write conflicts, the real time read shows the task is still ours, so
     * it retries and goes on.
     */
    public void testBenignConflictIsRetried() throws Exception {
        Versioned<ShardTaskDoc> claimed = claimedByThisNode(0, "earlier-run");
        PlainActionFuture<Versioned<ShardTaskDoc>> earlierRun = new PlainActionFuture<>();
        store.forLocalNode()
            .updateShardTask(claimed, claimed.doc().withProgress(Map.of("in_num_docs_received", 1)), claimed.doc().epoch(), earlierRun);
        earlierRun.actionGet(TimeValue.timeValueSeconds(10));

        List<ActionListener<Void>> indexer = new CopyOnWriteArrayList<>();
        ShardTaskRun run = new ShardTaskRun(store.forLocalNode(), claimed, finished -> {});
        run.start(indexer::add);
        assertBusy(() -> assertThat(indexer, hasSize(1)));
        assertFalse(run.isCancelled());
        assertThat(read(claimed).doc().runId(), equalTo(run.runId()));

        indexer.get(0).onResponse(null);
        assertBusy(() -> assertThat(read(claimed).doc().status(), equalTo(ShardTaskStatus.COMPLETED)));
    }

    /**
     * A node claims no more shard tasks than it can run at the same time (by default the size of the downsample thread pool): the
     * others stay unassigned, claimable by any node, and this node claims one as soon as a run finished.
     */
    public void testClaimsAtMostItsLimit() throws Exception {
        poller = newPoller(2);
        List<Versioned<ShardTaskDoc>> tasks = List.of(unassignedTask(0), unassignedTask(1), unassignedTask(2));
        poll();
        awaitStarted(2);
        assertThat(poller.activeRuns(), equalTo(2));
        assertThat("one task waits for a free slot", count(tasks, ShardTaskStatus.UNASSIGNED), equalTo(1L));

        completeRun(0, l -> l.onResponse(null));
        // the finished run frees a slot and makes the poller look again right away
        awaitStarted(3);
        assertBusy(() -> assertThat(poller.activeRuns(), equalTo(2)));
        assertThat(count(tasks, ShardTaskStatus.UNASSIGNED), equalTo(0L));
    }

    /**
     * Tasks are claimed oldest operation first, so the operations requested first finish first.
     */
    public void testClaimsOldestFirst() throws Exception {
        poller = newPoller(1);
        Versioned<ShardTaskDoc> newer = unassignedTask(0, 2_000);
        Versioned<ShardTaskDoc> older = unassignedTask(1, 1_000);
        poll();
        ShardTaskRun run = awaitStarted(1);
        assertThat(run.docId(), equalTo(older.doc().id()));
        assertThat(read(newer).doc().status(), equalTo(ShardTaskStatus.UNASSIGNED));
    }

    /**
     * The search returned a task that was taken in the meantime (here the master failed it, as another node could have claimed it):
     * the claim loses the compare-and-set, and the free slot goes to the next candidate right away, not at the next poll.
     */
    public void testLostClaimMovesOnToTheNextCandidate() throws Exception {
        poller = newPoller(1);
        Versioned<ShardTaskDoc> older = unassignedTask(0, 1_000);
        Versioned<ShardTaskDoc> newer = unassignedTask(1, 2_000);
        masterWrite(older, older.doc().failedByMaster("taken in the meantime"));

        // the candidates as a stale search would return them
        poller.claimInOrder(store.forLocalNode(), new ArrayList<>(List.of(newer, older)));

        ShardTaskRun run = awaitStarted(1);
        assertThat(run.docId(), equalTo(newer.doc().id()));
        assertThat(read(older).doc().status(), equalTo(ShardTaskStatus.FAILED));
        assertThat(poller.activeRuns(), equalTo(1));
    }

    /**
     * With a single free slot, a task this node already runs (e.g. after a restart) is resumed before new ones are claimed, even
     * older ones.
     */
    public void testResumesOwnTasksBeforeClaiming() throws Exception {
        poller = newPoller(1);
        Versioned<ShardTaskDoc> unassigned = unassignedTask(0, 1_000);
        Versioned<ShardTaskDoc> wasRunning = claimedByThisNode(1, "before-restart");

        poll();
        ShardTaskRun run = awaitStarted(1);
        assertThat(run.docId(), equalTo(wasRunning.doc().id()));
        assertThat(run.epoch(), equalTo(wasRunning.doc().epoch()));
        assertThat(read(unassigned).doc().status(), equalTo(ShardTaskStatus.UNASSIGNED));
    }

    /**
     * A task of a source shard this node holds no copy of is left for the nodes that do.
     */
    public void testDoesNotClaimTasksOfShardsItDoesNotHold() throws Exception {
        Versioned<ShardTaskDoc> elsewhere = createUnassigned(new ShardId(new Index("source-elsewhere", randomUUID()), 0), 1);
        poll();
        assertThat(started, empty());
        assertThat(read(elsewhere).doc().status(), equalTo(ShardTaskStatus.UNASSIGNED));
    }

    // ---- helpers ----

    private ShardTaskPoller newPoller(int maxConcurrentRuns) {
        return new ShardTaskPoller(
            client(),
            store,
            getInstanceFromNode(ClusterService.class),
            getInstanceFromNode(ThreadPool.class),
            getInstanceFromNode(ProjectResolver.class),
            false,
            (run, completion) -> started.add(new Tuple<>(run, completion)),
            maxConcurrentRuns
        );
    }

    private Versioned<ShardTaskDoc> unassignedTask(int shard) {
        return unassignedTask(shard, 1);
    }

    private Versioned<ShardTaskDoc> unassignedTask(int shard, long createdAt) {
        return createUnassigned(new ShardId(source, shard), createdAt);
    }

    private Versioned<ShardTaskDoc> createUnassigned(ShardId sourceShard, long createdAt) {
        ShardTaskDoc doc = ShardTaskDoc.newTask(
            PROJECT,
            "target",
            targetUuid,
            OrchestrationTestUtils.randomParams("target", sourceShard),
            createdAt
        );
        PlainActionFuture<Versioned<ShardTaskDoc>> created = new PlainActionFuture<>();
        store.forMaster().createShardTask(doc, created);
        return created.actionGet(TimeValue.timeValueSeconds(10));
    }

    /**
     * A task an earlier run of this node claimed, as found after this node restarted.
     */
    private Versioned<ShardTaskDoc> claimedByThisNode(int shard, String earlierRunId) {
        PlainActionFuture<Versioned<ShardTaskDoc>> claimed = new PlainActionFuture<>();
        store.forLocalNode().claimShardTask(unassignedTask(shard), earlierRunId, Map.of(), claimed);
        return claimed.actionGet(TimeValue.timeValueSeconds(10));
    }

    private long count(List<Versioned<ShardTaskDoc>> tasks, ShardTaskStatus status) {
        return tasks.stream().filter(t -> read(t).doc().status() == status).count();
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

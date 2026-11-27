/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.cluster.state.AwaitClusterStateVersionAppliedRequest;
import org.elasticsearch.action.admin.cluster.state.AwaitClusterStateVersionAppliedResponse;
import org.elasticsearch.action.admin.cluster.state.TransportAwaitClusterStateVersionAppliedAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ActionNotFoundTransportException;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalException;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * A node that joins is asked, once, to confirm it applied the state that added it, and is kept out of the external-worker
 * pool until it answers; nodes that cannot be external workers are not asked; a node that leaves, or restarts under the
 * same id, is forgotten with its question. A real {@link ClusterService} drives the listener; the client records each
 * await and lets the test answer it.
 */
public class ExternalWorkerReadinessTests extends ESTestCase {

    /**
     * One await the readiness sent: the version asked for and the listener to answer it on. The request keeps the node it
     * targets to itself; which node was asked shows in what an answer releases.
     */
    private record Await(long version, ActionListener<AwaitClusterStateVersionAppliedResponse> listener) {}

    private ThreadPool threadPool;
    private ClusterSettings clusterSettings;
    private ClusterService clusterService;
    private DiscoveryNode local;
    private final List<Await> awaits = new CopyOnWriteArrayList<>();
    private ExternalWorkerReadiness readiness;

    @Before
    public void setUpReadiness() {
        threadPool = new TestThreadPool(getTestName());
        local = DiscoveryNodeUtils.builder("local").roles(Set.of(DiscoveryNodeRole.DATA_ROLE)).build();
        clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Sets.addToCopy(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, ExternalSourceSettings.WORKER_READY_TIMEOUT)
        );
        clusterService = ClusterServiceUtils.createClusterService(threadPool, local, clusterSettings);
        var client = new NoOpNodeClient(threadPool) {
            @Override
            @SuppressWarnings("unchecked")
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                assertThat(action, sameInstance(TransportAwaitClusterStateVersionAppliedAction.TYPE));
                assertTrue("sent as the node itself", threadPool.getThreadContext().isSystemContext());
                var await = (AwaitClusterStateVersionAppliedRequest) request;
                assertThat("no timeout: a node that never applies stays out", await.nodeTimeout(), equalTo(TimeValue.MINUS_ONE));
                awaits.add(new Await(await.clusterStateVersion(), (ActionListener<AwaitClusterStateVersionAppliedResponse>) listener));
            }
        };
        readiness = new ExternalWorkerReadiness(clusterService, client, threadPool, true);
    }

    @After
    public void tearDownReadiness() {
        clusterService.close();
        terminate(threadPool);
    }

    public void testJoinedWorkerIsHeldBackUntilItConfirmsTheState() throws Exception {
        DiscoveryNode worker = dataNode("worker", "e1");
        ClusterState joined = apply(state(10, worker));

        assertThat(readiness.pendingEphemeralIds(), contains("e1"));
        awaitAwaits(1);
        assertThat("the version of the state that added the node", awaits.getFirst().version(), equalTo(joined.version()));
        DiscoveryNodes ready = readiness.readyNodes(joined.nodes());
        assertThat(ready, not(sameInstance(joined.nodes())));
        assertNull("held back", ready.get(worker.getId()));
        assertNotNull("the coordinator itself is never held back", ready.get(local.getId()));
        assertThat(ready.getLocalNodeId(), equalTo(local.getId()));

        awaits.getFirst().listener().onResponse(applied());

        assertThat(readiness.pendingEphemeralIds(), empty());
        assertThat(readiness.readyNodes(joined.nodes()), sameInstance(joined.nodes()));
    }

    public void testNodesThatCannotBeExternalWorkersAreNotAsked() {
        DiscoveryNode master = DiscoveryNodeUtils.builder("master").roles(Set.of(DiscoveryNodeRole.MASTER_ROLE)).build();
        DiscoveryNode index = DiscoveryNodeUtils.builder("index").roles(Set.of(DiscoveryNodeRole.INDEX_ROLE)).build();
        DiscoveryNode coordinatingOnly = DiscoveryNodeUtils.builder("coordinating").roles(Set.of()).build();
        ClusterState joined = apply(state(3, master, index, coordinatingOnly));

        assertThat(readiness.pendingEphemeralIds(), empty());
        assertThat(readiness.readyNodes(joined.nodes()), sameInstance(joined.nodes()));
        assertThat("nothing pending, so nothing was forked", awaits, empty());
    }

    public void testWorkerThatLeavesIsForgottenAndALateAnswerIsIgnored() throws Exception {
        DiscoveryNode worker = dataNode("worker", "e1");
        apply(state(10, worker));
        assertThat(readiness.pendingEphemeralIds(), contains("e1"));
        awaitAwaits(1);

        apply(state(11));
        assertThat(readiness.pendingEphemeralIds(), empty());

        // Back under the same node id but a new process: a new join, a new question, and the old answer counts for nothing.
        DiscoveryNode restarted = dataNode("worker", "e2");
        ClusterState rejoined = apply(state(12, restarted));
        assertThat(readiness.pendingEphemeralIds(), contains("e2"));
        awaitAwaits(2);
        assertThat(awaits.get(1).version(), equalTo(rejoined.version()));

        awaits.getFirst().listener().onResponse(applied());
        assertThat("the answer to the first join does not release the second", readiness.pendingEphemeralIds(), contains("e2"));
        assertNull(readiness.readyNodes(rejoined.nodes()).get(restarted.getId()));

        awaits.get(1).listener().onResponse(applied());
        assertThat(readiness.pendingEphemeralIds(), empty());
    }

    public void testFailedAwaitIsAskedAgain() throws Exception {
        DiscoveryNode worker = dataNode("worker", "e1");
        ClusterState joined = apply(state(10, worker));
        awaitAwaits(1);

        if (randomBoolean()) {
            awaits.getFirst().listener().onFailure(new NodeNotConnectedException(worker, "not yet"));
        } else {
            awaits.getFirst().listener().onResponse(failed(worker, new NodeNotConnectedException(worker, "not yet")));
        }
        assertThat("still held back", readiness.pendingEphemeralIds(), contains("e1"));

        awaitAwaits(2);
        assertThat("the same question again", awaits.get(1).version(), equalTo(joined.version()));
        awaits.get(1).listener().onResponse(applied());
        assertThat(readiness.pendingEphemeralIds(), empty());
    }

    /**
     * A node older than the action has no handler to ask, and asking it would be fatal to it with its assertions enabled:
     * it is neither asked nor held back, as it never was before the action existed.
     */
    public void testWorkerOlderThanTheActionIsNeitherAskedNorHeldBack() throws Exception {
        DiscoveryNode tooOld = dataNode("old", "e1", VersionUtils.getPreviousVersion(ExternalWorkerReadiness.AWAIT_ACTION_SINCE));
        DiscoveryNode newEnough = dataNode("new", "e2", ExternalWorkerReadiness.AWAIT_ACTION_SINCE);
        assertFalse(ExternalWorkerReadiness.canBeAsked(tooOld));
        assertTrue(ExternalWorkerReadiness.canBeAsked(newEnough));

        ClusterState joined = apply(state(10, tooOld, newEnough));

        assertThat(readiness.pendingEphemeralIds(), contains("e2"));
        awaitAwaits(1);
        DiscoveryNodes ready = readiness.readyNodes(joined.nodes());
        assertNotNull("not held back", ready.get(tooOld.getId()));
        assertNull("held back", ready.get(newEnough.getId()));
    }

    /** Should a node answer that it has no such action, which the version bound is meant to prevent, it is released. */
    public void testWorkerThatCannotBeAskedIsNotHeldBack() throws Exception {
        DiscoveryNode worker = dataNode("worker", "e1");
        apply(state(10, worker));
        awaitAwaits(1);

        var notFound = new ActionNotFoundTransportException(TransportAwaitClusterStateVersionAppliedAction.TYPE.name() + "[n]");
        if (randomBoolean()) {
            awaits.getFirst().listener().onFailure(notFound);
        } else {
            awaits.getFirst().listener().onResponse(failed(worker, notFound));
        }

        assertThat(readiness.pendingEphemeralIds(), empty());
    }

    /**
     * A rejoin without a restart: same node, same ephemeral id, a new join. The answer to the first join must not release
     * the second, which is what the entry identity, not the key, is for.
     */
    public void testAnswerToAnEarlierJoinOfTheSameNodeIsIgnored() throws Exception {
        DiscoveryNode worker = dataNode("worker", "e1");
        apply(state(10, worker));
        awaitAwaits(1);
        apply(state(11));
        ClusterState rejoined = apply(state(12, worker));
        assertThat(readiness.pendingEphemeralIds(), contains("e1"));
        awaitAwaits(2);
        assertThat(awaits.get(1).version(), equalTo(rejoined.version()));

        awaits.getFirst().listener().onResponse(applied());
        assertThat("the earlier join's answer counts for nothing", readiness.pendingEphemeralIds(), contains("e1"));
        assertNull(readiness.readyNodes(rejoined.nodes()).get(worker.getId()));

        awaits.get(1).listener().onResponse(applied());
        assertThat(readiness.pendingEphemeralIds(), empty());
    }

    /** The joining node is the elected master, as a default-role node can be: the copy handed out names no master. */
    public void testHeldBackMasterLeavesTheCopyWithoutAMaster() throws Exception {
        DiscoveryNode master = DiscoveryNodeUtils.builder("master")
            .name("master")
            .ephemeralId("em")
            .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE))
            .build();
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder()
            .add(local)
            .add(master)
            .localNodeId(local.getId())
            .masterNodeId(master.getId());
        ClusterState joined = apply(ClusterState.builder(clusterService.state()).version(5).nodes(nodes).build());
        assertThat(readiness.pendingEphemeralIds(), contains("em"));
        awaitAwaits(1);

        DiscoveryNodes ready = readiness.readyNodes(joined.nodes());
        assertNull(ready.get(master.getId()));
        assertNull(ready.getMasterNodeId());
        assertNull(ready.getMasterNode());
        assertNotNull(ready.get(local.getId()));
        assertThat(ready.getLocalNodeId(), equalTo(local.getId()));

        awaits.getFirst().listener().onResponse(applied());
        assertThat(readiness.readyNodes(joined.nodes()), sameInstance(joined.nodes()));
    }

    public void testRetriesBackOffUpToACap() {
        assertThat(ExternalWorkerReadiness.retryDelay(0), equalTo(ExternalWorkerReadiness.RETRY_DELAY));
        assertThat(ExternalWorkerReadiness.retryDelay(1), equalTo(TimeValue.timeValueSeconds(2)));
        assertThat(ExternalWorkerReadiness.retryDelay(2), equalTo(TimeValue.timeValueSeconds(4)));
        assertThat(ExternalWorkerReadiness.retryDelay(3), equalTo(TimeValue.timeValueSeconds(8)));
        assertThat(ExternalWorkerReadiness.retryDelay(4), equalTo(ExternalWorkerReadiness.MAX_RETRY_DELAY));
        assertThat(ExternalWorkerReadiness.retryDelay(63), equalTo(ExternalWorkerReadiness.MAX_RETRY_DELAY));
        assertThat(ExternalWorkerReadiness.retryDelay(Integer.MAX_VALUE), equalTo(ExternalWorkerReadiness.MAX_RETRY_DELAY));
    }

    public void testEveryNodeOfTheFirstStateIsAskedButTheCoordinator() throws Exception {
        DiscoveryNode a = dataNode("a", "ea");
        DiscoveryNode b = dataNode("b", "eb");
        ClusterState first = apply(state(1, a, b));

        awaitAwaits(2);
        assertThat(readiness.pendingEphemeralIds(), equalTo(Set.of("ea", "eb")));
        DiscoveryNodes ready = readiness.readyNodes(first.nodes());
        assertThat(ready.size(), equalTo(1));
        assertNotNull(ready.get(local.getId()));
    }

    /**
     * The hold precedes the state: it is installed by an applier, before the state that adds the node becomes the one the
     * cluster service hands out, so a query that reads the node from that state finds it held back already. Observed from
     * an applier registered after the readiness, which runs after its applier and before the state is swapped.
     */
    public void testAJoinedWorkerIsHeldBackBeforeTheStateThatAddsItIsVisible() throws Exception {
        DiscoveryNode worker = dataNode("worker", "e1");
        AtomicReference<Set<String>> pendingDuringApply = new AtomicReference<>();
        clusterService.addStateApplier(event -> {
            if (event.nodesDelta().added()) {
                pendingDuringApply.set(readiness.pendingEphemeralIds());
            }
        });

        apply(state(10, worker));

        assertThat("held back before the state was swapped", pendingDuringApply.get(), contains("e1"));
        awaitAwaits(1);
    }

    /**
     * The reverse order for a node that leaves: still held back while the state without it is applied, forgotten once that
     * state is visible, so a wait woken by the departure cannot find the node in the state and no longer held back.
     */
    public void testALeavingWorkerIsForgottenOnlyOnceTheStateWithoutItIsVisible() throws Exception {
        DiscoveryNode worker = dataNode("worker", "e1");
        apply(state(10, worker));
        awaitAwaits(1);
        AtomicReference<Set<String>> pendingDuringApply = new AtomicReference<>();
        clusterService.addStateApplier(event -> {
            if (event.nodesDelta().removed()) {
                pendingDuringApply.set(readiness.pendingEphemeralIds());
            }
        });

        apply(state(11));

        assertThat("still held back while the state without it was applied", pendingDuringApply.get(), contains("e1"));
        assertThat("forgotten once that state is visible", readiness.pendingEphemeralIds(), empty());
    }

    // -- the coordinator-side wait --

    private static final String WHAT = "distributing [1] external splits, session [test]";

    public void testWaitEndsAtOnceWhenSatisfied() {
        PlainActionFuture<Void> waited = new PlainActionFuture<>();
        readiness.awaitEligibleWorker(() -> true, task(), WHAT, waited);
        assertTrue(waited.isDone());
        assertNull(waited.actionGet());
    }

    public void testWaitEndsWhenTheHeldBackWorkerIsReleased() throws Exception {
        DiscoveryNode worker = dataNode("worker", "e1");
        apply(state(10, worker));
        awaitAwaits(1);
        PlainActionFuture<Void> waited = new PlainActionFuture<>();
        readiness.awaitEligibleWorker(() -> isReady(worker), task(), WHAT, waited);
        assertFalse(waited.isDone());

        awaits.getFirst().listener().onResponse(applied());

        assertNull("woken by the release", waited.actionGet(TimeValue.timeValueSeconds(30)));
        assertNoWaiters();
    }

    /** The only worker leaves: nothing is eligible any more and the query proceeds, to its local fallback. */
    public void testWaitEndsWhenTheHeldBackWorkerLeaves() throws Exception {
        DiscoveryNode worker = dataNode("worker", "e1");
        apply(state(10, worker));
        awaitAwaits(1);
        PlainActionFuture<Void> waited = new PlainActionFuture<>();
        readiness.awaitEligibleWorker(this::aWorkerIsReadyOrNoneExists, task(), WHAT, waited);
        assertFalse(waited.isDone());

        apply(state(11));

        assertNull("woken by the removal", waited.actionGet(TimeValue.timeValueSeconds(30)));
    }

    /** The first worker never answers; a second joins during the wait and answers: the wait must see it. */
    public void testWaitSeesAWorkerThatJoinsDuringIt() throws Exception {
        DiscoveryNode first = dataNode("first", "e1");
        apply(state(10, first));
        awaitAwaits(1);
        PlainActionFuture<Void> waited = new PlainActionFuture<>();
        readiness.awaitEligibleWorker(this::aWorkerIsReadyOrNoneExists, task(), WHAT, waited);
        assertFalse(waited.isDone());

        DiscoveryNode second = dataNode("second", "e2");
        apply(state(11, first, second));
        awaitAwaits(2);
        assertFalse("both still held back", waited.isDone());

        awaits.get(1).listener().onResponse(applied());

        assertNull("woken by the second worker's release", waited.actionGet(TimeValue.timeValueSeconds(30)));
        assertThat("the first is still held back", readiness.pendingEphemeralIds(), contains("e1"));
    }

    /**
     * The second worker answers while the state that adds it is still being applied, from an applier registered after the
     * readiness: the release wakes the wait against the old state, which has no second worker. The applier lets that
     * woken check run, and find nothing, before it returns, so that only a wake after the state is visible can end the
     * wait; the wait must not sit out the whole budget.
     */
    public void testWaitSeesAWorkerReleasedBeforeTheStateThatAddsItIsVisible() throws Exception {
        setWorkerReadyTimeout(TimeValue.timeValueMinutes(1));
        DiscoveryNode first = dataNode("first", "e1");
        apply(state(10, first));
        awaitAwaits(1);
        PlainActionFuture<Void> waited = new PlainActionFuture<>();
        AtomicInteger evaluations = new AtomicInteger();
        readiness.awaitEligibleWorker(() -> {
            // Counted after the read: a count seen by the applier then means the check read the state before the swap.
            boolean ready = aWorkerIsReadyOrNoneExists();
            evaluations.incrementAndGet();
            return ready;
        }, task(), WHAT, waited);
        assertFalse(waited.isDone());
        DiscoveryNode second = dataNode("second", "e2");
        // Recorded rather than asserted in the applier: an error thrown there would hang the apply, not fail the test.
        AtomicReference<String> failedInTheApply = new AtomicReference<>();
        AtomicReference<Set<String>> pendingAfterTheAnswer = new AtomicReference<>();
        AtomicBoolean doneDuringTheApply = new AtomicBoolean(true);
        clusterService.addStateApplier(event -> {
            if (event.nodesDelta().addedNodes().contains(second) == false) {
                return;
            }
            if (waitUntil(() -> awaits.size() == 2, 30, TimeUnit.SECONDS) == false) {
                failedInTheApply.set("the second worker was not asked during the apply");
                return;
            }
            final int before = evaluations.get();
            awaits.get(1).listener().onResponse(applied());
            pendingAfterTheAnswer.set(readiness.pendingEphemeralIds());
            if (waitUntil(() -> evaluations.get() > before, 10, TimeUnit.SECONDS) == false) {
                failedInTheApply.set("the release did not wake the wait");
                return;
            }
            doneDuringTheApply.set(waited.isDone());
        });

        apply(state(11, first, second));

        assertNull(failedInTheApply.get(), failedInTheApply.get());
        assertThat("released during the apply", pendingAfterTheAnswer.get(), contains("e1"));
        assertFalse("the woken check ran against the old state and found no worker", doneDuringTheApply.get());
        assertNull("woken once the state is visible", waited.actionGet(TimeValue.timeValueSeconds(10)));
    }

    /** A worker too old to be asked is not held back, so its join alone makes a worker available: the wait must see it. */
    public void testWaitSeesAnUnaskedWorkerThatJoinsDuringIt() throws Exception {
        setWorkerReadyTimeout(TimeValue.timeValueMinutes(1));
        DiscoveryNode first = dataNode("first", "e1");
        apply(state(10, first));
        awaitAwaits(1);
        PlainActionFuture<Void> waited = new PlainActionFuture<>();
        readiness.awaitEligibleWorker(this::aWorkerIsReadyOrNoneExists, task(), WHAT, waited);
        assertFalse(waited.isDone());

        apply(state(11, first, dataNode("old", "e2", VersionUtils.getPreviousVersion(ExternalWorkerReadiness.AWAIT_ACTION_SINCE))));

        assertNull("woken by the join", waited.actionGet(TimeValue.timeValueSeconds(10)));
        assertThat("not asked", awaits, hasSize(1));
    }

    /** The path of every cluster without federation: nothing is held back, nobody is asked, the nodes are handed back as is. */
    public void testDisabledReadinessHoldsNothingBack() throws Exception {
        ClusterService other = ClusterServiceUtils.createClusterService(threadPool, local, clusterSettings);
        try {
            var disabled = new ExternalWorkerReadiness(other, null, threadPool, false);
            DiscoveryNode worker = dataNode("worker", "e1");
            ClusterServiceUtils.setState(other, state(10, worker));
            DiscoveryNodes nodes = other.state().nodes();

            assertNotNull(nodes.get(worker.getId()));
            assertThat(disabled.pendingEphemeralIds(), empty());
            assertThat(disabled.readyNodes(nodes), sameInstance(nodes));
            assertThat("the enabled readiness is on another cluster service", awaits, empty());
        } finally {
            other.close();
        }
    }

    public void testWaitTimesOutWithTheRetryableCondition() throws Exception {
        setWorkerReadyTimeout(TimeValue.timeValueMillis(100));
        PlainActionFuture<Void> waited = new PlainActionFuture<>();
        readiness.awaitEligibleWorker(() -> false, task(), WHAT, waited);

        assertBusy(() -> assertTrue(waited.isDone()));
        Exception failure = expectThrows(Exception.class, waited::actionGet);
        assertThat(ExceptionsHelper.status(failure), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        assertTrue(failure.toString(), ExternalException.hasCondition(failure, ExternalException.Condition.WORKER_NOT_READY));
        assertThat(failure.getMessage(), containsString("waited [100ms] on the coordinator"));
        assertNoWaiters();
    }

    /** The shortest budget there is: the timer can fire before the wait is even registered, and must still unregister it. */
    public void testAWaitThatTimesOutAtOnceIsNotLeftRegistered() throws Exception {
        setWorkerReadyTimeout(TimeValue.timeValueMillis(1));
        PlainActionFuture<Void> waited = new PlainActionFuture<>();
        readiness.awaitEligibleWorker(() -> false, task(), WHAT, waited);

        assertBusy(() -> assertTrue(waited.isDone()));
        assertTrue(
            ExternalException.hasCondition(expectThrows(Exception.class, waited::actionGet), ExternalException.Condition.WORKER_NOT_READY)
        );
        assertNoWaiters();
    }

    /** Satisfied without a change notification, between the last check and the timer: the timer checks once more. */
    public void testAWorkerReadyJustBeforeTheTimerWins() {
        setWorkerReadyTimeout(TimeValue.timeValueSeconds(1));
        AtomicBoolean ready = new AtomicBoolean();
        PlainActionFuture<Void> waited = new PlainActionFuture<>();
        readiness.awaitEligibleWorker(ready::get, task(), WHAT, waited);
        ready.set(true);

        assertNull(waited.actionGet(TimeValue.timeValueSeconds(30)));
    }

    /** A condition that throws decides the wait, rather than leaving the query to the timer or to nothing at all. */
    public void testAConditionThatThrowsFailsTheWait() throws Exception {
        DiscoveryNode worker = dataNode("worker", "e1");
        apply(state(10, worker));
        awaitAwaits(1);
        AtomicBoolean armed = new AtomicBoolean();
        PlainActionFuture<Void> waited = new PlainActionFuture<>();
        readiness.awaitEligibleWorker(() -> {
            if (armed.get()) {
                throw new IllegalStateException("boom");
            }
            return false;
        }, task(), WHAT, waited);
        assertFalse(waited.isDone());
        armed.set(true);

        awaits.getFirst().listener().onResponse(applied());

        Exception failure = expectThrows(Exception.class, () -> waited.actionGet(TimeValue.timeValueSeconds(30)));
        assertThat(ExceptionsHelper.unwrapCause(failure), instanceOf(IllegalStateException.class));
    }

    public void testAZeroBudgetFailsAtOnce() {
        setWorkerReadyTimeout(TimeValue.ZERO);
        PlainActionFuture<Void> waited = new PlainActionFuture<>();
        readiness.awaitEligibleWorker(() -> false, task(), WHAT, waited);
        assertTrue(waited.isDone());
        Exception failure = expectThrows(Exception.class, waited::actionGet);
        assertTrue(failure.toString(), ExternalException.hasCondition(failure, ExternalException.Condition.WORKER_NOT_READY));
    }

    public void testCancellationEndsTheWait() {
        CancellableTask task = task();
        PlainActionFuture<Void> waited = new PlainActionFuture<>();
        readiness.awaitEligibleWorker(() -> false, task, WHAT, waited);
        assertFalse(waited.isDone());

        TaskCancelHelper.cancel(task, "test");

        assertTrue(waited.isDone());
        Exception failure = expectThrows(Exception.class, waited::actionGet);
        assertThat(failure, instanceOf(TaskCancelledException.class));
        assertNoWaiters();
    }

    /** A task cancelled before the wait starts: the cancellation ends it at once, and nothing of it is left registered. */
    public void testAWaitOnATaskAlreadyCancelledIsNotLeftRegistered() {
        CancellableTask task = task();
        TaskCancelHelper.cancel(task, "test");
        PlainActionFuture<Void> waited = new PlainActionFuture<>();

        readiness.awaitEligibleWorker(() -> false, task, WHAT, waited);

        assertTrue(waited.isDone());
        assertThat(expectThrows(Exception.class, waited::actionGet), instanceOf(TaskCancelledException.class));
        assertNoWaiters();
    }

    /**
     * A node answers under the system context its await was sent with, and a removal wakes the wait on the applier thread:
     * neither context may leak into the query, which continues as the user it came in as.
     */
    public void testTheWaitContinuesInTheCallerContext() throws Exception {
        DiscoveryNode worker = dataNode("worker", "e1");
        apply(state(10, worker));
        awaitAwaits(1);
        ThreadContext threadContext = threadPool.getThreadContext();
        AtomicReference<String> headerSeen = new AtomicReference<>();
        AtomicBoolean systemSeen = new AtomicBoolean(true);
        PlainActionFuture<Void> waited = new PlainActionFuture<>();
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader("caller", "user");
            // Ready, or gone: the second is how the removal branch below ends the wait.
            readiness.awaitEligibleWorker(() -> isReady(worker) || isGone(worker), task(), WHAT, ActionListener.runBefore(waited, () -> {
                headerSeen.set(threadContext.getHeader("caller"));
                systemSeen.set(threadContext.isSystemContext());
            }));
        }
        assertFalse(waited.isDone());

        if (randomBoolean()) {
            // Released the way a node's answer does it, under the system context.
            try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                threadContext.markAsSystemContext();
                awaits.getFirst().listener().onResponse(applied());
            }
        } else {
            // Woken on the applier thread by the node leaving; the wait's condition then holds trivially.
            apply(state(11));
        }

        assertNull(waited.actionGet(TimeValue.timeValueSeconds(30)));
        assertThat(headerSeen.get(), equalTo("user"));
        assertFalse("not the system context the node answered in", systemSeen.get());
    }

    private boolean isGone(DiscoveryNode node) {
        return clusterService.state().nodes().get(node.getId()) == null;
    }

    private boolean isReady(DiscoveryNode node) {
        return readiness.readyNodes(clusterService.state().nodes()).get(node.getId()) != null;
    }

    /** The shape of the coordinator's rule: a ready eligible worker, or no eligible worker at all. */
    private boolean aWorkerIsReadyOrNoneExists() {
        DiscoveryNodes nodes = clusterService.state().nodes();
        // The local node is a data node, hence eligible: leave it out, as a coordinating-only coordinator would have it.
        return NodeEligibilityStrategy.EXTERNAL_WORKER_NODES.eligibleNodes(readiness.readyNodes(nodes))
            .stream()
            .anyMatch(n -> n.getId().equals(local.getId()) == false)
            || NodeEligibilityStrategy.EXTERNAL_WORKER_NODES.eligibleNodes(nodes).stream().allMatch(n -> n.getId().equals(local.getId()));
    }

    /** A decided wait is unregistered by its decision, before the listener hears of it. */
    private void assertNoWaiters() {
        assertThat("nothing of the query outlives its wait", readiness.waiterCount(), equalTo(0));
    }

    private void setWorkerReadyTimeout(TimeValue timeout) {
        clusterSettings.applySettings(Settings.builder().put(ExternalSourceSettings.WORKER_READY_TIMEOUT.getKey(), timeout).build());
    }

    private static CancellableTask task() {
        return new CancellableTask(randomNonNegativeLong(), "transport", "indices:data/read/esql", "", TaskId.EMPTY_TASK_ID, Map.of());
    }

    /** The awaits are sent from the generic pool, after the listener returns; a retry a second after its failure. */
    private void awaitAwaits(int count) throws Exception {
        assertBusy(() -> assertThat(awaits, hasSize(count)), 30, TimeUnit.SECONDS);
    }

    /** Applies {@code state} and returns what the listener saw: the utility bumps the version by one on the way. */
    private ClusterState apply(ClusterState state) {
        ClusterServiceUtils.setState(clusterService, state);
        ClusterState applied = clusterService.state();
        assertThat(applied.version(), equalTo(state.version() + 1));
        return applied;
    }

    private ClusterState state(long version, DiscoveryNode... others) {
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder().add(local).localNodeId(local.getId()).masterNodeId(local.getId());
        for (DiscoveryNode node : others) {
            nodes.add(node);
        }
        return ClusterState.builder(clusterService.state()).version(version).nodes(nodes).build();
    }

    private static DiscoveryNode dataNode(String id, String ephemeralId) {
        return DiscoveryNodeUtils.builder(id).name(id).ephemeralId(ephemeralId).roles(Set.of(DiscoveryNodeRole.DATA_ROLE)).build();
    }

    private static DiscoveryNode dataNode(String id, String ephemeralId, Version version) {
        return DiscoveryNodeUtils.builder(id)
            .name(id)
            .ephemeralId(ephemeralId)
            .roles(Set.of(DiscoveryNodeRole.DATA_ROLE))
            .version(version, IndexVersions.MINIMUM_COMPATIBLE, IndexVersion.current())
            .build();
    }

    private AwaitClusterStateVersionAppliedResponse applied() {
        return new AwaitClusterStateVersionAppliedResponse(clusterService.getClusterName(), List.of(), List.of());
    }

    private AwaitClusterStateVersionAppliedResponse failed(DiscoveryNode node, Exception cause) {
        return new AwaitClusterStateVersionAppliedResponse(
            clusterService.getClusterName(),
            List.of(),
            List.of(new FailedNodeException(node.getId(), "failed", cause))
        );
    }
}

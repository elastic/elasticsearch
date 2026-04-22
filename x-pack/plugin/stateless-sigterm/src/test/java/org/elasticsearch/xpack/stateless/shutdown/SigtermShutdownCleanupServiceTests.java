/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.shutdown;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.shutdown.SigtermShutdownCleanupService.Node;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Delayed;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiPredicate;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.stateless.shutdown.SigtermShutdownCleanupService.CleanupSigtermShutdownTask;
import static org.elasticsearch.xpack.stateless.shutdown.SigtermShutdownCleanupService.RemoveSigtermShutdownTaskExecutor;
import static org.elasticsearch.xpack.stateless.shutdown.SigtermShutdownCleanupService.SubmitCleanupSigtermShutdown;
import static org.elasticsearch.xpack.stateless.shutdown.SigtermShutdownCleanupService.computeDelay;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.theInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class SigtermShutdownCleanupServiceTests extends ESTestCase {

    private static final long GRACE_PERIOD = 60_000;

    public void testCleanIfRemoved() {
        var mocks = newMocks();
        when(mocks.threadPool.absoluteTimeInMillis()).thenReturn(120_000L);
        var taskQueue = newMockTaskQueue(mocks.clusterService);
        var sigtermShutdownService = new SigtermShutdownCleanupService(mocks.clusterService, mocks.rerouteService);
        var master = createDiscoveryNode("node1");
        var other = createDiscoveryNode("node2");
        var another = createDiscoveryNode("node3");

        sigtermShutdownService.clusterChanged(
            new ClusterChangedEvent(
                this.getTestName(),
                createClusterState(
                    new NodesShutdownMetadata(Map.of(another.getId(), sigtermShutdown(another, GRACE_PERIOD, 0))),
                    master,
                    other
                ),
                createClusterState(null, master, other, another)
            )
        );

        assertThat(sigtermShutdownService.cleanups.values(), hasSize(1));

        var schedule = verifySchedule(mocks.threadPool, 1);
        schedule.shutdown.get(0).run();
        assertThat(sigtermShutdownService.cleanups.values(), hasSize(1));
        assertThat(sigtermShutdownService.cleanups.get(node(another)), sameInstance(SigtermShutdownCleanupService.IN_FLIGHT_CLEANUP));

        ArgumentCaptor<SigtermShutdownCleanupService.CleanupSigtermShutdownTask> taskCaptor = ArgumentCaptor.forClass(
            SigtermShutdownCleanupService.CleanupSigtermShutdownTask.class
        );
        Mockito.verify(taskQueue, times(1)).submitTask(eq("sigterm-grace-period-expired"), taskCaptor.capture(), isNull());
        taskCaptor.getValue().markProcessed();

        // Task completion removes the in-flight marker.
        assertThat(sigtermShutdownService.cleanups.values(), hasSize(0));
    }

    public void testDontCleanIfPresent() {
        var mocks = newMocks();
        when(mocks.threadPool.absoluteTimeInMillis()).thenReturn(120_000L);
        var master = createDiscoveryNode("node1");
        var other = createDiscoveryNode("node2");
        var another = createDiscoveryNode("node3");

        var shutdowns = new NodesShutdownMetadata(
            Map.of(another.getId(), sigtermShutdown(another, GRACE_PERIOD, 0), other.getId(), sigtermShutdown(other, GRACE_PERIOD, 20L))
        );

        var clusterChanged = new ClusterChangedEvent(
            this.getTestName(),
            createClusterState(shutdowns, master, other, another),
            createClusterState(shutdowns, master, other, another)
        );

        newMockTaskQueue(mocks.clusterService);
        var sigtermShutdownService = new SigtermShutdownCleanupService(mocks.clusterService, mocks.rerouteService);
        sigtermShutdownService.clusterChanged(clusterChanged);

        var schedule = verifySchedule(mocks.threadPool, 2);

        assertThat(
            RemoveSigtermShutdownTaskExecutor.cleanupSigtermShutdowns(schedule.nodeDelay.keySet(), clusterChanged.state()),
            sameInstance(clusterChanged.state())
        );
    }

    @SuppressWarnings("unchecked")
    public void testExecutorRemoveStale() {
        var mocks = newMocks();
        long now = 100_000L;
        when(mocks.threadPool.absoluteTimeInMillis()).thenReturn(now);
        var taskQueue = newMockTaskQueue(mocks.clusterService);
        var sigtermShutdownService = new SigtermShutdownCleanupService(mocks.clusterService, mocks.rerouteService);

        long grace = GRACE_PERIOD;
        var master = createDiscoveryNode("master1");
        var notTermNode = createDiscoveryNode("notTerm2"); // not term
        var stillExistingNode = createDiscoveryNode("stillExisting3"); // exists
        var withinGraceNode = createDiscoveryNode("withinGrace4"); // within grace
        var outOfGrace2XNode = createDiscoveryNode("outOfGrace2X5"); // out of grace
        var justOutOfGraceNode = createDiscoveryNode("justOutOfGrace6"); // also out of grace

        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        nodesBuilder.masterNodeId(master.getId());
        nodesBuilder.localNodeId(master.getId());
        nodesBuilder.add(master);
        nodesBuilder.add(stillExistingNode);
        Metadata.Builder metadataBuilder = Metadata.builder();
        var shutdownsMap = Map.of(
            notTermNode.getId(),
            otherShutdown(notTermNode, SingleNodeShutdownMetadata.Type.REPLACE, now - 2 * grace).setTargetNodeName(notTermNode.getId())
                .build(),
            stillExistingNode.getId(),
            sigtermShutdown(stillExistingNode, grace, now - 2 * grace),
            withinGraceNode.getId(),
            sigtermShutdown(withinGraceNode, grace, now - grace),
            outOfGrace2XNode.getId(),
            sigtermShutdown(outOfGrace2XNode, grace, now - 2 * grace),
            justOutOfGraceNode.getId(),
            sigtermShutdown(justOutOfGraceNode, grace, now - (grace + grace / 10) - 1)
        );
        metadataBuilder.putCustom(NodesShutdownMetadata.TYPE, new NodesShutdownMetadata(shutdownsMap));
        var state = ClusterState.builder(new ClusterName("test-cluster"))
            .routingTable(RoutingTable.builder().build())
            .metadata(metadataBuilder.build())
            .nodes(nodesBuilder)
            .build();

        sigtermShutdownService.clusterChanged(new ClusterChangedEvent(this.getTestName(), state, state));

        var schedule = verifySchedule(mocks.threadPool, 4);
        var remove = schedule.collect((s, d) -> {
            if (TimeValue.ZERO.equals(d)) {
                s.run();
                return true;
            }
            return false;
        });
        ArgumentCaptor<SigtermShutdownCleanupService.CleanupSigtermShutdownTask> taskCaptor = ArgumentCaptor.forClass(
            SigtermShutdownCleanupService.CleanupSigtermShutdownTask.class
        );
        Mockito.verify(taskQueue, times(remove.size())).submitTask(eq("sigterm-grace-period-expired"), taskCaptor.capture(), isNull());
        taskCaptor.getAllValues().forEach(SigtermShutdownCleanupService.CleanupSigtermShutdownTask::markProcessed);

        var update = RemoveSigtermShutdownTaskExecutor.cleanupSigtermShutdowns(remove, state);

        assertThat(
            Map.of(
                notTermNode.getId(),
                shutdownsMap.get(notTermNode.getId()),
                stillExistingNode.getId(),
                shutdownsMap.get(stillExistingNode.getId()),
                withinGraceNode.getId(),
                shutdownsMap.get(withinGraceNode.getId())
            ),
            equalTo(update.getMetadata().nodeShutdowns().getAll())
        );

        assertThat(sigtermShutdownService.cleanups.values(), hasSize(1));
        assertThat(sigtermShutdownService.cleanups, hasKey(node(withinGraceNode)));
    }

    @SuppressWarnings("unchecked")
    public void testExecutorInitialStateIfFresh() {
        var mocks = newMocks();
        long now = 100_000L;
        when(mocks.threadPool.absoluteTimeInMillis()).thenReturn(now);
        newMockTaskQueue(mocks.clusterService);
        long grace = GRACE_PERIOD;
        var master = createDiscoveryNode("master1");
        var other = createDiscoveryNode("other2"); // not term
        var another = createDiscoveryNode("another3"); // exists
        var yetAnother = createDiscoveryNode("yetAnother4"); // within grace

        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        nodesBuilder.masterNodeId(master.getId());
        nodesBuilder.localNodeId(master.getId());
        nodesBuilder.add(master);
        nodesBuilder.add(another);
        Metadata.Builder metadataBuilder = Metadata.builder();
        var shutdownsMap = Map.of(
            other.getId(),
            otherShutdown(other, SingleNodeShutdownMetadata.Type.REPLACE, now - 2 * grace).setTargetNodeName(other.getId()).build(),
            another.getId(),
            sigtermShutdown(another, grace, now - 2 * grace),
            yetAnother.getId(),
            sigtermShutdown(yetAnother, grace, now - grace)
        );
        metadataBuilder.putCustom(NodesShutdownMetadata.TYPE, new NodesShutdownMetadata(shutdownsMap));
        var state = ClusterState.builder(new ClusterName("test-cluster"))
            .routingTable(RoutingTable.builder().build())
            .metadata(metadataBuilder.build())
            .nodes(nodesBuilder)
            .build();

        var sigtermShutdownService = new SigtermShutdownCleanupService(mocks.clusterService, mocks.rerouteService);
        sigtermShutdownService.clusterChanged(new ClusterChangedEvent(this.getTestName(), state, state));

        var schedule = verifySchedule(mocks.threadPool, 2);
        var remove = schedule.collect((s, d) -> TimeValue.ZERO.equals(d));

        var update = SigtermShutdownCleanupService.RemoveSigtermShutdownTaskExecutor.cleanupSigtermShutdowns(remove, state);

        assertThat(state, sameInstance(update));
    }

    public void testCancelledTasksRescheduled() {
        var mocks = newMocks();
        newMockTaskQueue(mocks.clusterService);
        long grace = GRACE_PERIOD;
        long now = grace * 1_000;
        when(mocks.threadPool.absoluteTimeInMillis()).thenReturn(now);

        var master = createDiscoveryNode("node1");
        var other = createDiscoveryNode("node2");
        var otherNode = node(other);
        var shutdowns = new NodesShutdownMetadata(Map.of(other.getId(), sigtermShutdown(other, grace, now - 10 * grace)));
        var clusterChanged = new ClusterChangedEvent(
            this.getTestName(),
            createClusterState(shutdowns, master, other),
            createClusterState(shutdowns, master, other)
        );
        var sigtermShutdownService = new SigtermShutdownCleanupService(mocks.clusterService, mocks.rerouteService);
        sigtermShutdownService.clusterChanged(clusterChanged);

        assertThat(sigtermShutdownService.cleanups.values(), hasSize(1));
        assertThat(sigtermShutdownService.cleanups, hasKey(otherNode));
        var cancelled = new MockScheduleCancellable(true);
        sigtermShutdownService.cleanups.put(node(other), cancelled);

        sigtermShutdownService.clusterChanged(clusterChanged);
        assertThat(sigtermShutdownService.cleanups.values(), hasSize(1));
        assertThat(sigtermShutdownService.cleanups, hasKey(otherNode));
        assertThat(sigtermShutdownService.cleanups.get(otherNode), not(sameInstance(cancelled)));

        var notCancelled = new MockScheduleCancellable(false);
        sigtermShutdownService.cleanups.put(otherNode, notCancelled);
        assertThat(sigtermShutdownService.cleanups.values(), hasSize(1));
        assertThat(sigtermShutdownService.cleanups, hasKey(otherNode));
        assertThat(sigtermShutdownService.cleanups.get(otherNode), sameInstance(notCancelled));
    }

    public void testPendingDelayedCleanupNotRescheduled() {
        var mocks = newMocks();
        newMockTaskQueue(mocks.clusterService);
        long grace = GRACE_PERIOD;
        long now = grace * 1_000;
        when(mocks.threadPool.absoluteTimeInMillis()).thenReturn(now);

        var master = createDiscoveryNode("node1");
        var other = createDiscoveryNode("node2");
        var otherNode = node(other);
        var shutdowns = new NodesShutdownMetadata(Map.of(other.getId(), sigtermShutdown(other, grace, now - 10 * grace)));
        var clusterChanged = new ClusterChangedEvent(
            this.getTestName(),
            createClusterState(shutdowns, master, other),
            createClusterState(shutdowns, master, other)
        );
        var sigtermShutdownService = new SigtermShutdownCleanupService(mocks.clusterService, mocks.rerouteService);
        sigtermShutdownService.cleanups.put(otherNode, SigtermShutdownCleanupService.PENDING_DELAYED_CLEANUP);

        sigtermShutdownService.clusterChanged(clusterChanged);

        assertThat(sigtermShutdownService.cleanups, hasKey(otherNode));
        assertThat(sigtermShutdownService.cleanups.get(otherNode), sameInstance(SigtermShutdownCleanupService.PENDING_DELAYED_CLEANUP));
        Mockito.verify(mocks.threadPool, times(0))
            .schedule(any(SubmitCleanupSigtermShutdown.class), any(TimeValue.class), any(Executor.class));
    }

    public void testDelayedCleanupScheduledOnceUntilRunnableExecutes() {
        var mocks = newMocks();
        var taskQueue = newMockTaskQueue(mocks.clusterService);
        long grace = GRACE_PERIOD;
        long now = grace * 1_000;
        when(mocks.threadPool.absoluteTimeInMillis()).thenReturn(now);

        var master = createDiscoveryNode("node1");
        var other = createDiscoveryNode("node2");
        var otherNode = node(other);
        long started = now - (grace / 2);
        var shutdowns = new NodesShutdownMetadata(Map.of(other.getId(), sigtermShutdown(other, grace, started)));
        var clusterChanged = new ClusterChangedEvent(
            this.getTestName(),
            createClusterState(shutdowns, master, other),
            createClusterState(shutdowns, master, other)
        );

        var sigtermShutdownService = new SigtermShutdownCleanupService(mocks.clusterService, mocks.rerouteService);
        sigtermShutdownService.clusterChanged(clusterChanged);

        var schedule = verifySchedule(mocks.threadPool, 1);
        assertThat(schedule.delay.get(0).millis(), greaterThan(0L));
        assertThat(sigtermShutdownService.cleanups, hasKey(otherNode));
        assertThat(
            sigtermShutdownService.cleanups.get(otherNode),
            not(sameInstance(SigtermShutdownCleanupService.PENDING_DELAYED_CLEANUP))
        );
        assertThat(sigtermShutdownService.cleanups.get(otherNode), not(sameInstance(SigtermShutdownCleanupService.IN_FLIGHT_CLEANUP)));

        // Another cluster state update should not schedule a duplicate delayed cleanup.
        sigtermShutdownService.clusterChanged(clusterChanged);
        verifySchedule(mocks.threadPool, 1);
        Mockito.verify(taskQueue, times(0))
            .submitTask(eq("sigterm-grace-period-expired"), any(SigtermShutdownCleanupService.CleanupSigtermShutdownTask.class), isNull());

        // Once the delayed runnable executes, it should submit the cleanup and mark the node as in-flight.
        schedule.shutdown.get(0).run();
        assertThat(sigtermShutdownService.cleanups.get(otherNode), sameInstance(SigtermShutdownCleanupService.IN_FLIGHT_CLEANUP));
        Mockito.verify(taskQueue, times(1))
            .submitTask(eq("sigterm-grace-period-expired"), any(SigtermShutdownCleanupService.CleanupSigtermShutdownTask.class), isNull());
    }

    public void testPendingMarkerClearedIfScheduleThrows() {
        var mocks = newMocks();
        newMockTaskQueue(mocks.clusterService);
        long grace = GRACE_PERIOD;
        long now = grace * 1_000;
        when(mocks.threadPool.absoluteTimeInMillis()).thenReturn(now);

        var master = createDiscoveryNode("node1");
        var other = createDiscoveryNode("node2");
        var otherNode = node(other);
        var shutdowns = new NodesShutdownMetadata(Map.of(other.getId(), sigtermShutdown(other, grace, now - grace / 2)));
        var clusterChanged = new ClusterChangedEvent(
            this.getTestName(),
            createClusterState(shutdowns, master, other),
            createClusterState(shutdowns, master, other)
        );

        var rejection = new RuntimeException("simulated schedule rejection");
        Mockito.doThrow(rejection)
            .doReturn(mock(Scheduler.ScheduledCancellable.class))
            .when(mocks.threadPool)
            .schedule(any(SubmitCleanupSigtermShutdown.class), any(TimeValue.class), any(Executor.class));

        var sigtermShutdownService = new SigtermShutdownCleanupService(mocks.clusterService, mocks.rerouteService);
        assertSame(rejection, expectThrows(RuntimeException.class, () -> sigtermShutdownService.clusterChanged(clusterChanged)));
        assertThat(sigtermShutdownService.cleanups, not(hasKey(otherNode)));

        sigtermShutdownService.clusterChanged(clusterChanged);
        assertThat(sigtermShutdownService.cleanups, hasKey(otherNode));
        assertThat(
            sigtermShutdownService.cleanups.get(otherNode),
            not(sameInstance(SigtermShutdownCleanupService.PENDING_DELAYED_CLEANUP))
        );
        assertThat(sigtermShutdownService.cleanups.get(otherNode), not(sameInstance(SigtermShutdownCleanupService.IN_FLIGHT_CLEANUP)));
    }

    public void testShutdownAlreadyRemoved() {
        var mocks = newMocks();
        newMockTaskQueue(mocks.clusterService);
        long grace = GRACE_PERIOD;
        long now = grace * 1_000;
        when(mocks.threadPool.absoluteTimeInMillis()).thenReturn(now);

        var master = createDiscoveryNode("node1");
        var other = createDiscoveryNode("node2");
        var otherNode = node(other);
        var shutdowns = new NodesShutdownMetadata(Map.of(other.getId(), sigtermShutdown(other, grace, now - 10 * grace)));
        var removedShutdown = RemoveSigtermShutdownTaskExecutor.cleanupSigtermShutdowns(
            Set.of(otherNode),
            createClusterState(shutdowns, master)
        );
        assertThat(removedShutdown.metadata().nodeShutdowns().getAll().values(), hasSize(0));

        var shutdownAlreadyRemoved = createClusterState(new NodesShutdownMetadata(Collections.emptyMap()), master);
        assertThat(
            RemoveSigtermShutdownTaskExecutor.cleanupSigtermShutdowns(Set.of(otherNode), shutdownAlreadyRemoved),
            sameInstance(shutdownAlreadyRemoved)
        );
    }

    public void testNoDuplicateResubmissionWhileCleanupTaskInFlight() {
        var mocks = newMocks();
        var taskQueue = newMockTaskQueue(mocks.clusterService);
        long grace = GRACE_PERIOD;
        long now = grace * 1_000;
        when(mocks.threadPool.absoluteTimeInMillis()).thenReturn(now);

        var master = createDiscoveryNode("node1");
        var other = createDiscoveryNode("node2");
        var otherNode = node(other);
        var shutdowns = new NodesShutdownMetadata(Map.of(other.getId(), sigtermShutdown(other, grace, now - 10 * grace)));
        var clusterChanged = new ClusterChangedEvent(
            this.getTestName(),
            createClusterState(shutdowns, master, other),
            createClusterState(shutdowns, master, other)
        );

        var sigtermShutdownService = new SigtermShutdownCleanupService(mocks.clusterService, mocks.rerouteService);
        sigtermShutdownService.clusterChanged(clusterChanged);
        var schedule = verifySchedule(mocks.threadPool, 1);
        schedule.shutdown.get(0).run();
        assertThat(sigtermShutdownService.cleanups, hasKey(otherNode));
        assertThat(sigtermShutdownService.cleanups.get(otherNode), sameInstance(SigtermShutdownCleanupService.IN_FLIGHT_CLEANUP));
        Mockito.verify(taskQueue, times(1))
            .submitTask(eq("sigterm-grace-period-expired"), any(SigtermShutdownCleanupService.CleanupSigtermShutdownTask.class), isNull());

        // Simulate another cluster state update while the cleanup task is still queued on the master service.
        sigtermShutdownService.clusterChanged(clusterChanged);
        assertThat(sigtermShutdownService.cleanups, hasKey(otherNode));
        assertThat(sigtermShutdownService.cleanups.get(otherNode), sameInstance(SigtermShutdownCleanupService.IN_FLIGHT_CLEANUP));
        verifySchedule(mocks.threadPool, 1);

        ArgumentCaptor<SigtermShutdownCleanupService.CleanupSigtermShutdownTask> taskCaptor = ArgumentCaptor.forClass(
            SigtermShutdownCleanupService.CleanupSigtermShutdownTask.class
        );
        Mockito.verify(taskQueue, times(1)).submitTask(eq("sigterm-grace-period-expired"), taskCaptor.capture(), isNull());
        taskCaptor.getValue().onFailure(new RuntimeException("simulated"));
        assertThat(sigtermShutdownService.cleanups, not(hasKey(otherNode)));

        // Once the in-flight task fails we should retry.
        sigtermShutdownService.clusterChanged(clusterChanged);
        assertThat(sigtermShutdownService.cleanups, hasKey(otherNode));
        assertThat(sigtermShutdownService.cleanups.get(otherNode), not(sameInstance(SigtermShutdownCleanupService.IN_FLIGHT_CLEANUP)));
        verifySchedule(mocks.threadPool, 2);
    }

    public void testScheduleCleanForNullEphemeralId() {
        var mocks = newMocks();
        when(mocks.threadPool.absoluteTimeInMillis()).thenReturn(120_000L);
        var taskQueue = newMockTaskQueue(mocks.clusterService);
        var sigtermShutdownService = new SigtermShutdownCleanupService(mocks.clusterService, mocks.rerouteService);

        var master = createDiscoveryNode("node1");
        var other = createDiscoveryNode("node2");

        var shutdowns = new NodesShutdownMetadata(
            Map.of(
                other.getId(),
                // Create the shutdown record without specifying the ephemeralId. This can happen for nodes on old versions
                // or on edge cases where the node leaves the cluster before the shutdown record can be created
                SingleNodeShutdownMetadata.builder()
                    .setNodeId(other.getId())
                    .setType(SingleNodeShutdownMetadata.Type.SIGTERM)
                    .setGracePeriod(new TimeValue(GRACE_PERIOD))
                    .setReason("test")
                    .setStartedAtMillis(20L)
                    .build()
            )
        );
        sigtermShutdownService.clusterChanged(
            new ClusterChangedEvent(
                this.getTestName(),
                createClusterState(shutdowns, master, other),
                createClusterState(null, master, other)
            )
        );

        assertThat(sigtermShutdownService.cleanups.values(), hasSize(1));

        var schedule = verifySchedule(mocks.threadPool, 1);
        schedule.shutdown.get(0).run();
        assertThat(sigtermShutdownService.cleanups.values(), hasSize(1));
        assertThat(
            sigtermShutdownService.cleanups.get(new Node(other.getId(), null)),
            sameInstance(SigtermShutdownCleanupService.IN_FLIGHT_CLEANUP)
        );

        ArgumentCaptor<SigtermShutdownCleanupService.CleanupSigtermShutdownTask> taskCaptor = ArgumentCaptor.forClass(
            SigtermShutdownCleanupService.CleanupSigtermShutdownTask.class
        );
        Mockito.verify(taskQueue, times(1)).submitTask(eq("sigterm-grace-period-expired"), taskCaptor.capture(), isNull());
        taskCaptor.getValue().markProcessed();

        // Task completion removes the in-flight marker.
        assertThat(sigtermShutdownService.cleanups.values(), hasSize(0));
    }

    public void testCleanUpShutdownForNullEphemeralId() {
        var mocks = newMocks();
        newMockTaskQueue(mocks.clusterService);
        long grace = GRACE_PERIOD;
        long now = grace * 1_000;
        when(mocks.threadPool.absoluteTimeInMillis()).thenReturn(now);

        var master = createDiscoveryNode("node1");
        var other = createDiscoveryNode("node2");
        var otherNode = new Node(other.getId(), null);
        var shutdowns = new NodesShutdownMetadata(
            Map.of(
                other.getId(),
                // Create the shutdown record without specifying the ephemeralId. This can happen for nodes on old versions
                // or on edge cases where the node leaves the cluster before the shutdown record can be created
                SingleNodeShutdownMetadata.builder()
                    .setNodeId(other.getId())
                    .setType(SingleNodeShutdownMetadata.Type.SIGTERM)
                    .setGracePeriod(new TimeValue(grace))
                    .setReason("test")
                    .setStartedAtMillis(now - 10 * grace)
                    .build()
            )
        );
        var removedShutdown = RemoveSigtermShutdownTaskExecutor.cleanupSigtermShutdowns(
            Set.of(otherNode),
            createClusterState(shutdowns, master)
        );
        assertThat(removedShutdown.metadata().nodeShutdowns().getAll().values(), hasSize(0));

        var shutdownAlreadyRemoved = createClusterState(new NodesShutdownMetadata(Collections.emptyMap()), master);
        assertThat(
            RemoveSigtermShutdownTaskExecutor.cleanupSigtermShutdowns(Set.of(otherNode), shutdownAlreadyRemoved),
            sameInstance(shutdownAlreadyRemoved)
        );
    }

    public void testNonSigtermShutdown() {
        var mocks = newMocks();
        newMockTaskQueue(mocks.clusterService);
        long grace = GRACE_PERIOD;
        long now = grace * 1_000;
        when(mocks.threadPool.absoluteTimeInMillis()).thenReturn(now);

        var master = createDiscoveryNode("node1");
        var other = createDiscoveryNode("node2");
        var shutdowns = new NodesShutdownMetadata(
            Map.of(other.getId(), otherShutdown(other, SingleNodeShutdownMetadata.Type.REMOVE, now - 10 * grace).build())
        );
        var removedShutdown = RemoveSigtermShutdownTaskExecutor.cleanupSigtermShutdowns(
            Set.of(node(other)),
            createClusterState(shutdowns, master)
        );
        assertThat(removedShutdown.metadata().nodeShutdowns().getAll().values(), hasSize(1));
        assertThat(removedShutdown.metadata().nodeShutdowns(), equalTo(shutdowns));
    }

    public void testDifferentGracePeriods() {
        var mocks = newMocks();
        newMockTaskQueue(mocks.clusterService);
        var node1 = createDiscoveryNode("node1");
        long node1Grace = 93 * 60 * 1_000;
        long node1Started = 33 * 1_000;
        var node2 = createDiscoveryNode("node2");
        long node2Grace = 45 * 60 * 1_000;
        long node2Started = 6 * 60 * 1_000;
        var node3 = createDiscoveryNode("node3");
        long node3Grace = 2 * 60 * 60 * 1_000;
        long node3Started = node3Grace + 5 * 60 * 1_000;
        var shutdowns = new NodesShutdownMetadata(
            Map.of(
                node1.getId(),
                sigtermShutdown(node1, node1Grace, node1Started),
                node2.getId(),
                sigtermShutdown(node2, node2Grace, node2Started),
                node3.getId(),
                sigtermShutdown(node3, node3Grace, node3Started)
            )
        );
        var clusterChanged = new ClusterChangedEvent(
            this.getTestName(),
            createClusterState(shutdowns, node1, node2, node3),
            createClusterState(shutdowns, node1, node2, node3)
        );

        var sigtermShutdownService = new SigtermShutdownCleanupService(mocks.clusterService, mocks.rerouteService);
        long now = 10_000L;
        when(mocks.threadPool.absoluteTimeInMillis()).thenReturn(now);
        sigtermShutdownService.clusterChanged(clusterChanged);

        var schedule = verifySchedule(mocks.threadPool, 3);
        assertThat(schedule.nodeDelay.get(node(node1)), equalTo(computeDelay(now, node1Started, node1Grace)));
        assertThat(schedule.nodeDelay.get(node(node2)), equalTo(computeDelay(now, node2Started, node2Grace)));
        assertThat(schedule.nodeDelay.get(node(node3)), equalTo(computeDelay(now, node3Started, node3Grace)));
    }

    public void testComputeDelay() {
        long now = 100_000;
        long grace = 1_000;
        long grace_safety = grace + (grace / 10);

        // started in the future, elapsed = 0
        assertThat(computeDelay(now, now + grace, grace), equalTo(new TimeValue(grace_safety)));

        long running = 123;
        assertThat(computeDelay(now, now - running, grace), equalTo(new TimeValue(grace_safety - running)));

        assertThat(computeDelay(now, now - 2 * grace_safety, grace), equalTo(TimeValue.ZERO));
    }

    public void testBatchesSuccess() {
        var mocks = newMocks();
        var master = createDiscoveryNode("master1");
        var nodeAExists = createDiscoveryNode("nodeAExists");
        var nodeBGone = createDiscoveryNode("nodeBGone");

        var state = ClusterState.builder(new ClusterName("test-cluster"))
            .routingTable(RoutingTable.builder().build())
            .metadata(
                Metadata.builder()
                    .putCustom(
                        NodesShutdownMetadata.TYPE,
                        new NodesShutdownMetadata(
                            Map.of(
                                nodeAExists.getId(),
                                sigtermShutdown(nodeAExists, 1_000, 200),
                                nodeBGone.getId(),
                                sigtermShutdown(nodeBGone, 1_000, 200)
                            )
                        )
                    )
                    .build()
            )
            .nodes(DiscoveryNodes.builder().masterNodeId(master.getId()).localNodeId(master.getId()).add(master).add(nodeAExists))
            .build();

        var contextA = new TestCleanContext(new AtomicBoolean(false), new CleanupSigtermShutdownTask(node(nodeAExists), ignored -> {}));
        var contextB = new TestCleanContext(new AtomicBoolean(false), new CleanupSigtermShutdownTask(node(nodeBGone), ignored -> {}));
        ClusterState newState = null;
        try {
            newState = new RemoveSigtermShutdownTaskExecutor(mocks.rerouteService).execute(
                new ClusterStateTaskExecutor.BatchExecutionContext<>(state, List.of(contextA, contextB), () -> null)
            );
        } catch (Exception e) {
            fail(e.toString());
        }
        assertThat(newState, not(theInstance(state)));
        assertNotNull(newState.metadata().nodeShutdowns().get(nodeAExists.getId()));
        assertNull(newState.metadata().nodeShutdowns().get(nodeBGone.getId()));
        assertTrue(contextA.successCalled.get());
        assertTrue(contextB.successCalled.get());
    }

    private record TestCleanContext(AtomicBoolean successCalled, CleanupSigtermShutdownTask task)
        implements
            ClusterStateTaskExecutor.TaskContext<CleanupSigtermShutdownTask> {
        @Override
        public CleanupSigtermShutdownTask getTask() {
            return task;
        }

        @Override
        public void success(Runnable onPublicationSuccess) {
            onPublicationSuccess.run();
            successCalled.set(true);
        }

        @Override
        public void success(Consumer<ClusterState> publishedStateConsumer) {}

        @Override
        public void success(Runnable onPublicationSuccess, ClusterStateAckListener clusterStateAckListener) {}

        @Override
        public void success(Consumer<ClusterState> publishedStateConsumer, ClusterStateAckListener clusterStateAckListener) {}

        @Override
        public void onFailure(Exception failure) {}

        @Override
        public Releasable captureResponseHeaders() {
            return null;
        }
    }

    static SingleNodeShutdownMetadata.Builder otherShutdown(
        DiscoveryNode discoveryNode,
        SingleNodeShutdownMetadata.Type type,
        long startedAt
    ) {
        return SingleNodeShutdownMetadata.builder()
            .setNodeId(discoveryNode.getId())
            .setNodeEphemeralId(discoveryNode.getEphemeralId())
            .setType(type)
            .setReason("test " + discoveryNode.getId())
            .setStartedAtMillis(startedAt);
    }

    private static SingleNodeShutdownMetadata sigtermShutdown(DiscoveryNode discoveryNode, long grace, long startedAt) {
        return SingleNodeShutdownMetadata.builder()
            .setNodeId(discoveryNode.getId())
            .setNodeEphemeralId(discoveryNode.getEphemeralId())
            .setType(SingleNodeShutdownMetadata.Type.SIGTERM)
            .setGracePeriod(new TimeValue(grace))
            .setReason("test")
            .setStartedAtMillis(startedAt)
            .build();
    }

    private static ClusterState createClusterState(NodesShutdownMetadata shutdown, DiscoveryNode masterNode, DiscoveryNode... nodes) {
        Metadata.Builder metadataBuilder = Metadata.builder();
        if (shutdown != null) {
            metadataBuilder.putCustom(NodesShutdownMetadata.TYPE, shutdown);
        }
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        if (masterNode != null) {
            nodesBuilder.masterNodeId(masterNode.getId());
            nodesBuilder.localNodeId(masterNode.getId());
            nodesBuilder.add(masterNode);
        }
        for (DiscoveryNode node : nodes) {
            nodesBuilder.add(node);
        }
        return ClusterState.builder(new ClusterName("test-cluster"))
            .routingTable(RoutingTable.builder().build())
            .metadata(metadataBuilder.build())
            .nodes(nodesBuilder)
            .build();
    }

    private static String randomNodeId() {
        return UUID.randomUUID().toString();
    }

    private record Mocks(ClusterService clusterService, ThreadPool threadPool, RerouteService rerouteService) {}

    private static Mocks newMocks() {
        final var mocks = new Mocks(mock(ClusterService.class), mock(ThreadPool.class), mock(RerouteService.class));
        when(mocks.clusterService.threadPool()).thenReturn(mocks.threadPool);
        when(mocks.threadPool.generic()).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        doReturn(mock(Scheduler.ScheduledCancellable.class)).when(mocks.threadPool)
            .schedule(any(SubmitCleanupSigtermShutdown.class), any(TimeValue.class), any(Executor.class));
        return mocks;
    }

    @SuppressWarnings("unchecked")
    private static MasterServiceTaskQueue<CleanupSigtermShutdownTask> newMockTaskQueue(ClusterService clusterService) {
        final var masterServiceTaskQueue = mock(MasterServiceTaskQueue.class);
        when(clusterService.<CleanupSigtermShutdownTask>createTaskQueue(eq("shutdown-sigterm-cleaner"), eq(Priority.NORMAL), any()))
            .thenReturn(masterServiceTaskQueue);
        return masterServiceTaskQueue;
    }

    public record MockScheduleCancellable(boolean isCancelled) implements Scheduler.ScheduledCancellable {
        @Override
        public int compareTo(Delayed o) {
            return 0;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return 0;
        }

        @Override
        public boolean cancel() {
            return false;
        }
    }

    private record Schedule(List<SubmitCleanupSigtermShutdown> shutdown, List<TimeValue> delay, Map<Node, TimeValue> nodeDelay) {
        Set<Node> collect(BiPredicate<SubmitCleanupSigtermShutdown, TimeValue> shouldCollect) {
            var set = new HashSet<Node>();
            for (int i = 0; i < shutdown.size(); i++) {
                var submit = shutdown.get(i);
                if (shouldCollect.test(submit, delay.get(i))) {
                    set.add(submit.node());
                }
            }
            return set;
        }
    }

    private Schedule verifySchedule(ThreadPool threadPool, int times) {
        var shutdown = ArgumentCaptor.forClass(SigtermShutdownCleanupService.SubmitCleanupSigtermShutdown.class);
        var delay = ArgumentCaptor.forClass(TimeValue.class);
        Mockito.verify(threadPool, times(times)).schedule(shutdown.capture(), delay.capture(), any(Executor.class));
        var schedule = new Schedule(shutdown.getAllValues(), delay.getAllValues(), new HashMap<>());
        assertThat(schedule.shutdown, hasSize(schedule.delay.size()));
        for (int i = 0; i < schedule.shutdown.size(); i++) {
            schedule.nodeDelay.put(schedule.shutdown.get(i).node(), schedule.delay.get(i));
        }
        return schedule;
    }

    private static Node node(DiscoveryNode discoveryNode) {
        return new Node(discoveryNode.getId(), discoveryNode.getEphemeralId());
    }

    private static DiscoveryNode createDiscoveryNode(String name) {
        return DiscoveryNodeUtils.builder(randomNodeId()).name(name).ephemeralId(randomNodeId()).build();
    }
}

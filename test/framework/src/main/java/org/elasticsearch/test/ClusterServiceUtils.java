/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.test;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.util.Throwables;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ClusterStatePublicationEvent;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.ClusterStatePublisher;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.cluster.version.CompatibilityVersionsUtils;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static junit.framework.TestCase.fail;

public class ClusterServiceUtils {

    public static void setState(ClusterApplierService executor, ClusterState clusterState) {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> exception = new AtomicReference<>();
        executor.onNewClusterState(
            "test setting state",
            () -> ClusterState.builder(clusterState).version(clusterState.version() + 1).build(),
            new ActionListener<>() {
                @Override
                public void onResponse(Void ignored) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    exception.set(e);
                    latch.countDown();
                }
            }
        );
        try {
            latch.await();
            if (exception.get() != null) {
                Throwables.rethrow(exception.get());
            }
        } catch (InterruptedException e) {
            throw new ElasticsearchException("unexpected exception", e);
        }
    }

    public static void setState(MasterService executor, ClusterState clusterState) {
        CountDownLatch latch = new CountDownLatch(1);
        executor.submitUnbatchedStateUpdateTask("test setting state", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                // make sure we increment versions as listener may depend on it for change
                return ClusterState.builder(clusterState).build();
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("unexpected exception" + e);
            }
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new ElasticsearchException("unexpected interruption", e);
        }
    }

    public static ClusterService createClusterService(ThreadPool threadPool) {
        return createClusterService(threadPool, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
    }

    public static ClusterService createClusterService(ThreadPool threadPool, DiscoveryNode localNode) {
        return createClusterService(threadPool, localNode, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
    }

    public static ClusterService createClusterService(ThreadPool threadPool, ClusterSettings clusterSettings) {
        DiscoveryNode discoveryNode = DiscoveryNodeUtils.create("node", "node");
        return createClusterService(threadPool, discoveryNode, clusterSettings);
    }

    public static ClusterService createClusterService(ThreadPool threadPool, DiscoveryNode localNode, ClusterSettings clusterSettings) {
        return createClusterService(threadPool, localNode, Settings.EMPTY, clusterSettings);
    }

    public static ClusterService createClusterService(ThreadPool threadPool, Settings providedSettings) {
        return createClusterService(
            threadPool,
            DiscoveryNodeUtils.create("node", "node"),
            providedSettings,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
    }

    public static ClusterService createClusterService(
        ThreadPool threadPool,
        DiscoveryNode localNode,
        Settings providedSettings,
        ClusterSettings clusterSettings
    ) {
        Settings settings = Settings.builder()
            .put("node.name", "test")
            .put("cluster.name", "ClusterServiceTests")
            .put(providedSettings)
            .build();
        ClusterService clusterService = new ClusterService(
            settings,
            clusterSettings,
            threadPool,
            new TaskManager(settings, threadPool, Collections.emptySet(), Tracer.NOOP)
        );
        clusterService.setNodeConnectionsService(createNoOpNodeConnectionsService());
        ClusterState initialClusterState = ClusterState.builder(new ClusterName(ClusterServiceUtils.class.getSimpleName()))
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).masterNodeId(localNode.getId()))
            .putCompatibilityVersions(localNode.getId(), CompatibilityVersionsUtils.staticCurrent())
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();
        clusterService.getClusterApplierService().setInitialState(initialClusterState);
        clusterService.getMasterService().setClusterStatePublisher(createClusterStatePublisher(clusterService.getClusterApplierService()));
        clusterService.getMasterService().setClusterStateSupplier(clusterService.getClusterApplierService()::state);
        clusterService.start();
        return clusterService;
    }

    public static NodeConnectionsService createNoOpNodeConnectionsService() {
        return new NodeConnectionsService(Settings.EMPTY, null, null) {
            @Override
            public void connectToNodes(DiscoveryNodes discoveryNodes, Runnable onCompletion) {
                // don't do anything
                onCompletion.run();
            }

            @Override
            public void disconnectFromNodesExcept(DiscoveryNodes nodesToKeep) {
                // don't do anything
            }
        };
    }

    public static ClusterStatePublisher createClusterStatePublisher(ClusterApplier clusterApplier) {
        return (clusterStatePublicationEvent, publishListener, ackListener) -> {
            setAllElapsedMillis(clusterStatePublicationEvent);
            clusterApplier.onNewClusterState(
                "mock_publish_to_self[" + clusterStatePublicationEvent.getSummary() + "]",
                clusterStatePublicationEvent::getNewState,
                publishListener
            );
        };
    }

    public static ClusterService createClusterService(ClusterState initialState, ThreadPool threadPool) {
        ClusterService clusterService = createClusterService(threadPool);
        setState(clusterService, initialState);
        return clusterService;
    }

    public static ClusterService createClusterService(ClusterState initialState, ThreadPool threadPool, ClusterSettings clusterSettings) {
        ClusterService clusterService = createClusterService(threadPool, clusterSettings);
        setState(clusterService, initialState);
        return clusterService;
    }

    public static void setState(ClusterService clusterService, ClusterState.Builder clusterStateBuilder) {
        setState(clusterService, clusterStateBuilder.build());
    }

    /**
     * Sets the state on the cluster applier service
     */
    public static void setState(ClusterService clusterService, ClusterState clusterState) {
        setState(clusterService.getClusterApplierService(), clusterState);
    }

    public static void setAllElapsedMillis(ClusterStatePublicationEvent clusterStatePublicationEvent) {
        clusterStatePublicationEvent.setPublicationContextConstructionElapsedMillis(0L);
        clusterStatePublicationEvent.setPublicationCommitElapsedMillis(0L);
        clusterStatePublicationEvent.setPublicationCompletionElapsedMillis(0L);
        clusterStatePublicationEvent.setMasterApplyElapsedMillis(0L);
    }

    public static void awaitClusterState(Logger logger, Predicate<ClusterState> statePredicate, ClusterService clusterService)
        throws Exception {
        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        ClusterStateObserver.waitForState(
            clusterService,
            clusterService.getClusterApplierService().threadPool().getThreadContext(),
            new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    future.onResponse(null);
                }

                @Override
                public void onClusterServiceClose() {
                    future.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    assert false : "onTimeout called with no timeout set";
                }
            },
            statePredicate,
            null,
            logger
        );
        future.get(30L, TimeUnit.SECONDS);
    }

    public static void awaitNoPendingTasks(ClusterService clusterService) {
        ESTestCase.safeAwait(
            listener -> clusterService.submitUnbatchedStateUpdateTask(
                "await-queue-empty",
                new ClusterStateUpdateTask(Priority.LANGUID, ESTestCase.SAFE_AWAIT_TIMEOUT) {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        return currentState;
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }

                    @Override
                    public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                        listener.onResponse(null);
                    }
                }
            )
        );
    }

    /**
     * Creates a {@link ClusterStateListener} which subscribes to the given {@link ClusterService} and waits for it to apply a cluster state
     * that satisfies {@code predicate}, at which point it unsubscribes itself.
     *
     * @return A {@link SubscribableListener} which is completed when the first cluster state matching {@code predicate} is applied by the
     *         given {@code clusterService}. If the current cluster state already matches {@code predicate} then the returned listener is
     *         already complete. If no matching cluster state is seen within {@link ESTestCase#SAFE_AWAIT_TIMEOUT} then the listener is
     *         completed exceptionally on the scheduler thread that belongs to {@code clusterService}.
     */
    public static SubscribableListener<Void> addTemporaryStateListener(ClusterService clusterService, Predicate<ClusterState> predicate) {
        return addTemporaryStateListener(clusterService, predicate, ESTestCase.SAFE_AWAIT_TIMEOUT);
    }

    /**
     * Creates a {@link ClusterStateListener} which subscribes to the given {@link ClusterService} and waits for it to apply a cluster state
     * that satisfies {@code predicate}, at which point it unsubscribes itself.
     *
     * @return A {@link SubscribableListener} which is completed when the first cluster state matching {@code predicate} is applied by the
     *         given {@code clusterService}. If the current cluster state already matches {@code predicate} then the returned listener is
     *         already complete. If no matching cluster state is seen within the provided {@code timeout} then the listener is
     *         completed exceptionally on the scheduler thread that belongs to {@code clusterService}.
     */
    public static SubscribableListener<Void> addTemporaryStateListener(
        ClusterService clusterService,
        Predicate<ClusterState> predicate,
        TimeValue timeout
    ) {
        final var listener = new SubscribableListener<Void>();
        final ClusterStateListener clusterStateListener = new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                try {
                    if (predicate.test(event.state())) {
                        listener.onResponse(null);
                    }
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }

            @Override
            public String toString() {
                return predicate.toString();
            }
        };
        clusterService.addListener(clusterStateListener);
        listener.addListener(ActionListener.running(() -> clusterService.removeListener(clusterStateListener)));
        if (predicate.test(clusterService.state())) {
            listener.onResponse(null);
        } else {
            listener.addTimeout(timeout, clusterService.threadPool(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        }
        return listener;
    }

    /**
     * Creates a {@link ClusterStateListener} which subscribes to the {@link ClusterService} of one of the nodes in the
     * {@link ESIntegTestCase#internalCluster()}. When the chosen {@link ClusterService} applies a state that satisfies {@code predicate}
     * the listener unsubscribes itself.
     *
     * @return A {@link SubscribableListener} which is completed when the first cluster state matching {@code predicate} is applied by the
     *         {@link ClusterService} belonging to one of the nodes in the {@link ESIntegTestCase#internalCluster()}. If the current cluster
     *         state already matches {@code predicate} then the returned listener is already complete. If no matching cluster state is seen
     *         within {@link ESTestCase#SAFE_AWAIT_TIMEOUT} then the listener is completed exceptionally on the scheduler thread that
     *         belongs to the chosen node's {@link ClusterService}.
     */
    public static SubscribableListener<Void> addTemporaryStateListener(Predicate<ClusterState> predicate) {
        return addTemporaryStateListener(ESIntegTestCase.internalCluster().clusterService(), predicate);
    }

    /**
     * Creates a {@link ClusterStateListener} which subscribes to the {@link ClusterService} of the current elected master node in the
     * {@link ESIntegTestCase#internalCluster()}. When this node's {@link ClusterService} applies a state that satisfies {@code predicate}
     * the listener unsubscribes itself.
     *
     * @return A {@link SubscribableListener} which is completed when the first cluster state matching {@code predicate} is applied by the
     *         {@link ClusterService} belonging to the node that was the elected master node in the
     *         {@link ESIntegTestCase#internalCluster()} when this method was first called. If the current cluster state already matches
     *         {@code predicate} then the returned listener is already complete. If no matching cluster state is seen within
     *         {@link ESTestCase#SAFE_AWAIT_TIMEOUT} then the listener is completed exceptionally on the scheduler thread that belongs to
     *         the elected master node's {@link ClusterService}.
     */
    public static SubscribableListener<Void> addMasterTemporaryStateListener(Predicate<ClusterState> predicate) {
        return addTemporaryStateListener(ESIntegTestCase.internalCluster().getCurrentMasterNodeInstance(ClusterService.class), predicate);
    }
}

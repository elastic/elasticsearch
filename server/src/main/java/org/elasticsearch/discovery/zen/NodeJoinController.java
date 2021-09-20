/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.discovery.zen;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStatePublicationEvent;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.coordination.JoinTaskExecutor;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class processes incoming join request (passed zia {@link ZenDiscovery}). Incoming nodes
 * are directly added to the cluster state or are accumulated during master election.
 */
public class NodeJoinController {

    private static final Logger logger = LogManager.getLogger(NodeJoinController.class);

    private final MasterService masterService;
    private final JoinTaskExecutor joinTaskExecutor;

    // this is set while trying to become a master
    // mutation should be done under lock
    private ElectionContext electionContext = null;


    public NodeJoinController(Settings settings, MasterService masterService, AllocationService allocationService,
                              ElectMasterService electMaster, RerouteService rerouteService) {
        this.masterService = masterService;
        joinTaskExecutor = new JoinTaskExecutor(settings, allocationService, logger, rerouteService) {
            @Override
            public void clusterStatePublished(ClusterStatePublicationEvent clusterStatePublicationEvent) {
                electMaster.logMinimumMasterNodesWarningIfNecessary(
                    clusterStatePublicationEvent.getOldState(),
                    clusterStatePublicationEvent.getNewState());
            }
        };
    }

    /**
     * waits for enough incoming joins from master eligible nodes to complete the master election
     * <p>
     * You must start accumulating joins before calling this method. See {@link #startElectionContext()}
     * <p>
     * The method will return once the local node has been elected as master or some failure/timeout has happened.
     * The exact outcome is communicated via the callback parameter, which is guaranteed to be called.
     *
     * @param requiredMasterJoins the number of joins from master eligible needed to complete the election
     * @param timeValue           how long to wait before failing. a timeout is communicated via the callback's onFailure method.
     * @param callback            the result of the election (success or failure) will be communicated by calling methods on this
     *                            object
     **/
    public void waitToBeElectedAsMaster(int requiredMasterJoins, TimeValue timeValue, final ElectionCallback callback) {
        final CountDownLatch done = new CountDownLatch(1);
        final ElectionCallback wrapperCallback = new ElectionCallback() {
            @Override
            public void onElectedAsMaster(ClusterState state) {
                done.countDown();
                callback.onElectedAsMaster(state);
            }

            @Override
            public void onFailure(Throwable t) {
                done.countDown();
                callback.onFailure(t);
            }
        };

        ElectionContext myElectionContext = null;

        try {
            // check what we have so far..
            // capture the context we add the callback to make sure we fail our own
            synchronized (this) {
                assert electionContext != null : "waitToBeElectedAsMaster is called we are not accumulating joins";
                myElectionContext = electionContext;
                electionContext.onAttemptToBeElected(requiredMasterJoins, wrapperCallback);
                checkPendingJoinsAndElectIfNeeded();
            }

            try {
                if (done.await(timeValue.millis(), TimeUnit.MILLISECONDS)) {
                    // callback handles everything
                    return;
                }
            } catch (InterruptedException e) {

            }
            if (logger.isTraceEnabled()) {
                final int pendingNodes = myElectionContext.getPendingMasterJoinsCount();
                logger.trace("timed out waiting to be elected. waited [{}]. pending master node joins [{}]", timeValue, pendingNodes);
            }
            failContextIfNeeded(myElectionContext, "timed out waiting to be elected");
        } catch (Exception e) {
            logger.error("unexpected failure while waiting for incoming joins", e);
            if (myElectionContext != null) {
                failContextIfNeeded(myElectionContext, "unexpected failure while waiting for pending joins [" + e.getMessage() + "]");
            }
        }
    }

    /**
     * utility method to fail the given election context under the cluster state thread
     */
    private synchronized void failContextIfNeeded(final ElectionContext context, final String reason) {
        if (electionContext == context) {
            stopElectionContext(reason);
        }
    }

    /**
     * Accumulates any future incoming join request. Pending join requests will be processed in the final steps of becoming a
     * master or when {@link #stopElectionContext(String)} is called.
     */
    public synchronized void startElectionContext() {
        logger.trace("starting an election context, will accumulate joins");
        assert electionContext == null : "double startElectionContext() calls";
        electionContext = new ElectionContext();
    }

    /**
     * Stopped accumulating joins. All pending joins will be processed. Future joins will be processed immediately
     */
    public void stopElectionContext(String reason) {
        logger.trace("stopping election ([{}])", reason);
        synchronized (this) {
            assert electionContext != null : "stopElectionContext() called but not accumulating";
            electionContext.closeAndProcessPending(reason);
            electionContext = null;
        }
    }

    /**
     * processes or queues an incoming join request.
     * <p>
     * Note: doesn't do any validation. This should have been done before.
     */
    public synchronized void handleJoinRequest(final DiscoveryNode node, final MembershipAction.JoinCallback callback) {
        if (electionContext != null) {
            electionContext.addIncomingJoin(node, callback);
            checkPendingJoinsAndElectIfNeeded();
        } else {
            masterService.submitStateUpdateTask("zen-disco-node-join",
                new JoinTaskExecutor.Task(node, "no election context"), ClusterStateTaskConfig.build(Priority.URGENT),
                joinTaskExecutor, new JoinTaskListener(callback, logger));
        }
    }

    /**
     * checks if there is an on going request to become master and if it has enough pending joins. If so, the node will
     * become master via a ClusterState update task.
     */
    private synchronized void checkPendingJoinsAndElectIfNeeded() {
        assert electionContext != null : "election check requested but no active context";
        final int pendingMasterJoins = electionContext.getPendingMasterJoinsCount();
        if (electionContext.isEnoughPendingJoins(pendingMasterJoins) == false) {
            if (logger.isTraceEnabled()) {
                logger.trace("not enough joins for election. Got [{}], required [{}]", pendingMasterJoins,
                    electionContext.requiredMasterJoins);
            }
        } else {
            if (logger.isTraceEnabled()) {
                logger.trace("have enough joins for election. Got [{}], required [{}]", pendingMasterJoins,
                    electionContext.requiredMasterJoins);
            }
            electionContext.closeAndBecomeMaster();
            electionContext = null; // clear this out so future joins won't be accumulated
        }
    }

    public interface ElectionCallback {
        /**
         * called when the local node is successfully elected as master
         * Guaranteed to be called on the cluster state update thread
         **/
        void onElectedAsMaster(ClusterState state);

        /**
         * called when the local node failed to be elected as master
         * Guaranteed to be called on the cluster state update thread
         **/
        void onFailure(Throwable t);
    }

    class ElectionContext {
        private ElectionCallback callback = null;
        private int requiredMasterJoins = -1;
        private final Map<DiscoveryNode, List<MembershipAction.JoinCallback>> joinRequestAccumulator = new HashMap<>();

        final AtomicBoolean closed = new AtomicBoolean();

        public synchronized void onAttemptToBeElected(int requiredMasterJoins, ElectionCallback callback) {
            ensureOpen();
            assert this.requiredMasterJoins < 0;
            assert this.callback == null;
            this.requiredMasterJoins = requiredMasterJoins;
            this.callback = callback;
        }

        public synchronized void addIncomingJoin(DiscoveryNode node, MembershipAction.JoinCallback callback) {
            ensureOpen();
            joinRequestAccumulator.computeIfAbsent(node, n -> new ArrayList<>()).add(callback);
        }


        public synchronized boolean isEnoughPendingJoins(int pendingMasterJoins) {
            final boolean hasEnough;
            if (requiredMasterJoins < 0) {
                // requiredMasterNodes is unknown yet, return false and keep on waiting
                hasEnough = false;
            } else {
                assert callback != null : "requiredMasterJoins is set but not the callback";
                hasEnough = pendingMasterJoins >= requiredMasterJoins;
            }
            return hasEnough;
        }

        private Map<JoinTaskExecutor.Task, ClusterStateTaskListener> getPendingAsTasks(String reason) {
            Map<JoinTaskExecutor.Task, ClusterStateTaskListener> tasks = new HashMap<>();
            joinRequestAccumulator.entrySet().stream().forEach(e -> tasks.put(
                new JoinTaskExecutor.Task(e.getKey(), reason), new JoinTaskListener(e.getValue(), logger)));
            return tasks;
        }

        public synchronized int getPendingMasterJoinsCount() {
            int pendingMasterJoins = 0;
            for (DiscoveryNode node : joinRequestAccumulator.keySet()) {
                if (node.isMasterNode()) {
                    pendingMasterJoins++;
                }
            }
            return pendingMasterJoins;
        }

        public synchronized void closeAndBecomeMaster() {
            assert callback != null : "becoming a master but the callback is not yet set";
            assert isEnoughPendingJoins(getPendingMasterJoinsCount()) : "becoming a master but pending joins of "
                + getPendingMasterJoinsCount() + " are not enough. needs [" + requiredMasterJoins + "];";

            innerClose();

            Map<JoinTaskExecutor.Task, ClusterStateTaskListener> tasks = getPendingAsTasks("become master");
            final String source = "zen-disco-elected-as-master ([" + tasks.size() + "] nodes joined)";

            // noop listener, the election finished listener determines result
            tasks.put(JoinTaskExecutor.newBecomeMasterTask(), (source1, e) -> {});
            tasks.put(JoinTaskExecutor.newFinishElectionTask(), electionFinishedListener);
            masterService.submitStateUpdateTasks(source, tasks, ClusterStateTaskConfig.build(Priority.URGENT), joinTaskExecutor);
        }

        public synchronized void closeAndProcessPending(String reason) {
            innerClose();
            Map<JoinTaskExecutor.Task, ClusterStateTaskListener> tasks = getPendingAsTasks(reason);
            final String source = "zen-disco-election-stop [" + reason + "]";
            tasks.put(JoinTaskExecutor.newFinishElectionTask(), electionFinishedListener);
            masterService.submitStateUpdateTasks(source, tasks, ClusterStateTaskConfig.build(Priority.URGENT), joinTaskExecutor);
        }

        private void innerClose() {
            if (closed.getAndSet(true)) {
                throw new AlreadyClosedException("election context is already closed");
            }
        }

        private void ensureOpen() {
            if (closed.get()) {
                throw new AlreadyClosedException("election context is already closed");
            }
        }

        private synchronized ElectionCallback getCallback() {
            return callback;
        }

        private void onElectedAsMaster(ClusterState state) {
            assert MasterService.assertMasterUpdateThread();
            assert state.nodes().isLocalNodeElectedMaster() : "onElectedAsMaster called but local node is not master";
            ElectionCallback callback = getCallback(); // get under lock
            if (callback != null) {
                callback.onElectedAsMaster(state);
            }
        }

        private void onFailure(Throwable t) {
            assert MasterService.assertMasterUpdateThread();
            ElectionCallback callback = getCallback(); // get under lock
            if (callback != null) {
                callback.onFailure(t);
            }
        }

        private final ClusterStateTaskListener electionFinishedListener = new ClusterStateTaskListener() {

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (newState.nodes().isLocalNodeElectedMaster()) {
                    ElectionContext.this.onElectedAsMaster(newState);
                } else {
                    onFailure(source, new NotMasterException("election stopped [" + source + "]"));
                }
            }

            @Override
            public void onFailure(String source, Exception e) {
                ElectionContext.this.onFailure(e);
            }
        };

    }

    static class JoinTaskListener implements ClusterStateTaskListener {
        final List<MembershipAction.JoinCallback> callbacks;
        private final Logger logger;

        JoinTaskListener(MembershipAction.JoinCallback callback, Logger logger) {
            this(Collections.singletonList(callback), logger);
        }

        JoinTaskListener(List<MembershipAction.JoinCallback> callbacks, Logger logger) {
            this.callbacks = callbacks;
            this.logger = logger;
        }

        @Override
        public void onFailure(String source, Exception e) {
            for (MembershipAction.JoinCallback callback : callbacks) {
                try {
                    callback.onFailure(e);
                } catch (Exception inner) {
                    logger.error(() -> new ParameterizedMessage("error handling task failure [{}]", e), inner);
                }
            }
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            for (MembershipAction.JoinCallback callback : callbacks) {
                try {
                    callback.onSuccess();
                } catch (Exception e) {
                    logger.error(() -> new ParameterizedMessage("unexpected error during [{}]", source), e);
                }
            }
        }
    }

}

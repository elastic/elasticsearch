/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.core.Strings.format;

/**
 * A {@link BatchedRerouteService} is a {@link RerouteService} that batches together reroute requests to avoid unnecessary extra reroutes.
 * This component only does meaningful work on the elected master node. Reroute requests will fail with a {@link NotMasterException} on
 * other nodes.
 */
public class BatchedRerouteService implements RerouteService {
    private static final Logger logger = LogManager.getLogger(BatchedRerouteService.class);

    private static final String CLUSTER_UPDATE_TASK_SOURCE = "cluster_reroute";

    private final ClusterService clusterService;
    private final RerouteAction reroute;

    private final Object mutex = new Object();
    @Nullable // null if no reroute is currently pending
    private List<ActionListener<ClusterState>> pendingRerouteListeners;
    private Priority pendingTaskPriority = Priority.LANGUID;

    public interface RerouteAction {
        ClusterState reroute(ClusterState state, String reason);
    }

    /**
     * @param reroute Function that computes the updated cluster state after it has been rerouted.
     */
    public BatchedRerouteService(ClusterService clusterService, RerouteAction reroute) {
        this.clusterService = clusterService;
        this.reroute = reroute;
    }

    /**
     * Initiates a reroute.
     */
    @Override
    public final void reroute(String reason, Priority priority, ActionListener<ClusterState> listener) {
        final ActionListener<ClusterState> wrappedListener = ContextPreservingActionListener.wrapPreservingContext(
            listener,
            clusterService.getClusterApplierService().threadPool().getThreadContext()
        );
        final List<ActionListener<ClusterState>> currentListeners;
        synchronized (mutex) {
            if (pendingRerouteListeners != null) {
                if (priority.sameOrAfter(pendingTaskPriority)) {
                    logger.trace(
                        "already has pending reroute at priority [{}], adding [{}] with priority [{}] to batch",
                        pendingTaskPriority,
                        reason,
                        priority
                    );
                    pendingRerouteListeners.add(wrappedListener);
                    return;
                } else {
                    logger.trace(
                        "already has pending reroute at priority [{}], promoting batch to [{}] and adding [{}]",
                        pendingTaskPriority,
                        priority,
                        reason
                    );
                    currentListeners = new ArrayList<>(1 + pendingRerouteListeners.size());
                    currentListeners.add(wrappedListener);
                    currentListeners.addAll(pendingRerouteListeners);
                    pendingRerouteListeners.clear();
                    pendingRerouteListeners = currentListeners;
                    pendingTaskPriority = priority;
                }
            } else {
                logger.trace("no pending reroute, scheduling reroute [{}] at priority [{}]", reason, priority);
                currentListeners = new ArrayList<>(1);
                currentListeners.add(wrappedListener);
                pendingRerouteListeners = currentListeners;
                pendingTaskPriority = priority;
            }
        }
        try {
            final String source = CLUSTER_UPDATE_TASK_SOURCE + "(" + reason + ")";
            submitUnbatchedTask(source, new ClusterStateUpdateTask(priority) {

                @Override
                public ClusterState execute(ClusterState currentState) {
                    final boolean currentListenersArePending;
                    synchronized (mutex) {
                        assert currentListeners.isEmpty() == (pendingRerouteListeners != currentListeners)
                            : "currentListeners=" + currentListeners + ", pendingRerouteListeners=" + pendingRerouteListeners;
                        currentListenersArePending = pendingRerouteListeners == currentListeners;
                        if (currentListenersArePending) {
                            pendingRerouteListeners = null;
                        }
                    }
                    if (currentListenersArePending) {
                        logger.trace("performing batched reroute [{}]", reason);
                        return reroute.reroute(currentState, reason);
                    } else {
                        logger.trace("batched reroute [{}] was promoted", reason);
                        return currentState;
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    synchronized (mutex) {
                        if (pendingRerouteListeners == currentListeners) {
                            pendingRerouteListeners = null;
                        }
                    }
                    final ClusterState state = clusterService.state();
                    if (MasterService.isPublishFailureException(e)) {
                        logger.debug(() -> format("unexpected failure during [%s], current state:\n%s", source, state), e);
                        // no big deal, the new master will reroute again
                    } else if (logger.isTraceEnabled()) {
                        logger.error(() -> format("unexpected failure during [%s], current state:\n%s", source, state), e);
                    } else {
                        logger.error(
                            () -> format("unexpected failure during [%s], current state version [%s]", source, state.version()),
                            e
                        );
                    }
                    ActionListener.onFailure(currentListeners, new ElasticsearchException("delayed reroute [" + reason + "] failed", e));
                }

                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    ActionListener.onResponse(currentListeners, newState);
                }
            });
        } catch (Exception e) {
            synchronized (mutex) {
                assert currentListeners.isEmpty() == (pendingRerouteListeners != currentListeners);
                if (pendingRerouteListeners == currentListeners) {
                    pendingRerouteListeners = null;
                }
            }
            ClusterState state = clusterService.state();
            logger.warn(() -> "failed to reroute routing table, current state:\n" + state, e);
            ActionListener.onFailure(
                currentListeners,
                new ElasticsearchException("delayed reroute [" + reason + "] could not be submitted", e)
            );
        }
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }
}

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.routing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;

import java.util.function.BiFunction;

/**
 * A {@link RoutingService} listens to clusters state. When this service
 * receives a {@link ClusterChangedEvent} the cluster state will be verified and
 * the routing tables might be updated.
 * <p>
 * Note: The {@link RoutingService} is responsible for cluster wide operations
 * that include modifications to the cluster state. Such an operation can only
 * be performed on the clusters master node. Unless the local node this service
 * is running on is the clusters master node this service will not perform any
 * actions.
 * </p>
 */
public class RoutingService extends AbstractLifecycleComponent {
    private static final Logger logger = LogManager.getLogger(RoutingService.class);

    private static final String CLUSTER_UPDATE_TASK_SOURCE = "cluster_reroute";

    private final ClusterService clusterService;
    private final BiFunction<ClusterState, String, ClusterState> reroute;

    private final Object mutex = new Object();
    @Nullable // null if no reroute is currently pending
    private PlainListenableActionFuture<Void> pendingRerouteListeners;

    @Inject
    public RoutingService(ClusterService clusterService, BiFunction<ClusterState, String, ClusterState> reroute) {
        this.clusterService = clusterService;
        this.reroute = reroute;
    }

    @Override
    protected void doStart() {
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() {
    }

    /**
     * Initiates a reroute.
     */
    public final void reroute(String reason, ActionListener<Void> listener) {
        if (lifecycle.started() == false) {
            listener.onFailure(new IllegalStateException(
                "rejecting delayed reroute [" + reason + "] in state [" + lifecycleState() + "]"));
            return;
        }
        final PlainListenableActionFuture<Void> currentListeners;
        synchronized (mutex) {
            if (pendingRerouteListeners != null) {
                logger.trace("already has pending reroute, adding [{}] to batch", reason);
                pendingRerouteListeners.addListener(listener);
                return;
            }
            currentListeners = PlainListenableActionFuture.newListenableFuture();
            currentListeners.addListener(listener);
            pendingRerouteListeners = currentListeners;
        }
        logger.trace("rerouting [{}]", reason);
        try {
            clusterService.submitStateUpdateTask(CLUSTER_UPDATE_TASK_SOURCE + "(" + reason + ")",
                new ClusterStateUpdateTask(Priority.HIGH) {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        synchronized (mutex) {
                            assert pendingRerouteListeners == currentListeners;
                            pendingRerouteListeners = null;
                        }
                        return reroute.apply(currentState, reason);
                    }

                    @Override
                    public void onNoLongerMaster(String source) {
                        synchronized (mutex) {
                            if (pendingRerouteListeners == currentListeners) {
                                pendingRerouteListeners = null;
                            }
                        }
                        currentListeners.onFailure(new NotMasterException("delayed reroute [" + reason + "] cancelled"));
                        // no big deal, the new master will reroute again
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        synchronized (mutex) {
                            if (pendingRerouteListeners == currentListeners) {
                                pendingRerouteListeners = null;
                            }
                        }
                        final ClusterState state = clusterService.state();
                        if (logger.isTraceEnabled()) {
                            logger.error(() -> new ParameterizedMessage("unexpected failure during [{}], current state:\n{}",
                                source, state), e);
                        } else {
                            logger.error(() -> new ParameterizedMessage("unexpected failure during [{}], current state version [{}]",
                                source, state.version()), e);
                        }
                        currentListeners.onFailure(new ElasticsearchException("delayed reroute [" + reason + "] failed", e));
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        currentListeners.onResponse(null);
                    }
                });
        } catch (Exception e) {
            synchronized (mutex) {
                assert pendingRerouteListeners == currentListeners;
                pendingRerouteListeners = null;
            }
            ClusterState state = clusterService.state();
            logger.warn(() -> new ParameterizedMessage("failed to reroute routing table, current state:\n{}", state), e);
            currentListeners.onFailure(new ElasticsearchException("delayed reroute [" + reason + "] could not be submitted", e));
        }
    }
}

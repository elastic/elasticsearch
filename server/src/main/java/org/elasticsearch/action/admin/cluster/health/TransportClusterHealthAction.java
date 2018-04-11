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

package org.elasticsearch.action.admin.cluster.health;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.LocalClusterUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.health.ClusterShardHealth;
import org.elasticsearch.cluster.health.ClusterStateHealth;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public class TransportClusterHealthAction extends TransportMasterNodeReadAction<ClusterHealthRequest, ClusterHealthResponse> {

    private final GatewayAllocator gatewayAllocator;

    @Inject
    public TransportClusterHealthAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                        ThreadPool threadPool, ActionFilters actionFilters,
                                        IndexNameExpressionResolver indexNameExpressionResolver, GatewayAllocator gatewayAllocator) {
        super(settings, ClusterHealthAction.NAME, false, transportService, clusterService, threadPool, actionFilters,
            ClusterHealthRequest::new, indexNameExpressionResolver);
        this.gatewayAllocator = gatewayAllocator;
    }

    @Override
    protected String executor() {
        // this should be executing quickly no need to fork off
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterHealthRequest request, ClusterState state) {
        return null; // we want users to be able to call this even when there are global blocks, just to check the health (are there blocks?)
    }

    @Override
    protected ClusterHealthResponse newResponse() {
        return new ClusterHealthResponse();
    }

    @Override
    protected final void masterOperation(ClusterHealthRequest request, ClusterState state, ActionListener<ClusterHealthResponse> listener) throws Exception {
        logger.warn("attempt to execute a cluster health operation without a task");
        throw new UnsupportedOperationException("task parameter is required for this operation");
    }

    @Override
    protected void masterOperation(Task task, final ClusterHealthRequest request, final ClusterState unusedState, final ActionListener<ClusterHealthResponse> listener) {
        if (request.waitForEvents() != null) {
            final long endTimeMS = TimeValue.nsecToMSec(System.nanoTime()) + request.timeout().millis();
            if (request.local()) {
                clusterService.submitStateUpdateTask("cluster_health (wait_for_events [" + request.waitForEvents() + "])", new LocalClusterUpdateTask(request.waitForEvents()) {
                    @Override
                    public ClusterTasksResult<LocalClusterUpdateTask> execute(ClusterState currentState) {
                        return unchanged();
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        final long timeoutInMillis = Math.max(0, endTimeMS - TimeValue.nsecToMSec(System.nanoTime()));
                        final TimeValue newTimeout = TimeValue.timeValueMillis(timeoutInMillis);
                        request.timeout(newTimeout);
                        executeHealth(request, listener);
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        logger.error(() -> new ParameterizedMessage("unexpected failure during [{}]", source), e);
                        listener.onFailure(e);
                    }
                });
            } else {
                clusterService.submitStateUpdateTask("cluster_health (wait_for_events [" + request.waitForEvents() + "])", new ClusterStateUpdateTask(request.waitForEvents()) {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        return currentState;
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        final long timeoutInMillis = Math.max(0, endTimeMS - TimeValue.nsecToMSec(System.nanoTime()));
                        final TimeValue newTimeout = TimeValue.timeValueMillis(timeoutInMillis);
                        request.timeout(newTimeout);
                        executeHealth(request, listener);
                    }

                    @Override
                    public void onNoLongerMaster(String source) {
                        logger.trace("stopped being master while waiting for events with priority [{}]. retrying.", request.waitForEvents());
                        // TransportMasterNodeAction implements the retry logic, which is triggered by passing a NotMasterException
                        listener.onFailure(new NotMasterException("no longer master. source: [" + source + "]"));
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        logger.error(() -> new ParameterizedMessage("unexpected failure during [{}]", source), e);
                        listener.onFailure(e);
                    }
                });
            }
        } else {
            executeHealth(request, listener);
        }

    }

    private void executeHealth(final ClusterHealthRequest request, final ActionListener<ClusterHealthResponse> listener) {
        int waitFor = 0;
        if (request.waitForStatus() != null) {
            waitFor++;
        }
        if (request.waitForNoRelocatingShards()) {
            waitFor++;
        }
        if (request.waitForNoInitializingShards()) {
            waitFor++;
        }
        if (request.waitForActiveShards().equals(ActiveShardCount.NONE) == false) {
            waitFor++;
        }
        if (request.waitForNodes().isEmpty() == false) {
            waitFor++;
        }
        if (request.indices() != null && request.indices().length > 0) { // check that they actually exists in the meta data
            waitFor++;
        }

        final ClusterState state = clusterService.state();
        final ClusterStateObserver observer = new ClusterStateObserver(state, clusterService, null, logger, threadPool.getThreadContext());
        if (request.timeout().millis() == 0) {
            listener.onResponse(getResponse(request, state, waitFor, request.timeout().millis() == 0));
            return;
        }
        final int concreteWaitFor = waitFor;
        final Predicate<ClusterState> validationPredicate = newState -> validateRequest(request, newState, concreteWaitFor);

        final ClusterStateObserver.Listener stateListener = new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState clusterState) {
                listener.onResponse(getResponse(request, clusterState, concreteWaitFor, false));
            }

            @Override
            public void onClusterServiceClose() {
                listener.onFailure(new IllegalStateException("ClusterService was close during health call"));
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                final ClusterHealthResponse response = getResponse(request, observer.setAndGetObservedState(), concreteWaitFor, true);
                listener.onResponse(response);
            }
        };
        if (validationPredicate.test(state)) {
            stateListener.onNewClusterState(state);
        } else {
            observer.waitForNextChange(stateListener, validationPredicate, request.timeout());
        }
    }

    private boolean validateRequest(final ClusterHealthRequest request, ClusterState clusterState, final int waitFor) {
        ClusterHealthResponse response = clusterHealth(request, clusterState, clusterService.getMasterService().numberOfPendingTasks(),
                gatewayAllocator.getNumberOfInFlightFetch(), clusterService.getMasterService().getMaxTaskWaitTime());
        int readyCounter = prepareResponse(request, response, clusterState, indexNameExpressionResolver);
        return readyCounter == waitFor;
    }

    private ClusterHealthResponse getResponse(final ClusterHealthRequest request, ClusterState clusterState, final int waitFor, boolean timedOut) {
        ClusterHealthResponse response = clusterHealth(request, clusterState, clusterService.getMasterService().numberOfPendingTasks(),
                gatewayAllocator.getNumberOfInFlightFetch(), clusterService.getMasterService().getMaxTaskWaitTime());
        int readyCounter = prepareResponse(request, response, clusterState, indexNameExpressionResolver);
        boolean valid = (readyCounter == waitFor);
        assert valid || timedOut;
        // we check for a timeout here since this method might be called from the wait_for_events
        // response handler which might have timed out already.
        // if the state is sufficient for what we where waiting for we don't need to mark this as timedOut.
        // We spend too much time in waiting for events such that we might already reached a valid state.
        // this should not mark the request as timed out
        response.setTimedOut(timedOut && valid == false);
        return response;
    }

    static int prepareResponse(final ClusterHealthRequest request, final ClusterHealthResponse response,
                                   final ClusterState clusterState, final IndexNameExpressionResolver indexNameExpressionResolver) {
        int waitForCounter = 0;
        if (request.waitForStatus() != null && response.getStatus().value() <= request.waitForStatus().value()) {
            waitForCounter++;
        }
        if (request.waitForNoRelocatingShards() && response.getRelocatingShards() == 0) {
            waitForCounter++;
        }
        if (request.waitForNoInitializingShards() && response.getInitializingShards() == 0) {
            waitForCounter++;
        }
        if (request.waitForActiveShards().equals(ActiveShardCount.NONE) == false) {
            ActiveShardCount waitForActiveShards = request.waitForActiveShards();
            assert waitForActiveShards.equals(ActiveShardCount.DEFAULT) == false :
                "waitForActiveShards must not be DEFAULT on the request object, instead it should be NONE";
            if (waitForActiveShards.equals(ActiveShardCount.ALL)
                    && response.getUnassignedShards() == 0
                    && response.getInitializingShards() == 0) {
                // if we are waiting for all shards to be active, then the num of unassigned and num of initializing shards must be 0
                waitForCounter++;
            } else if (waitForActiveShards.enoughShardsActive(response.getActiveShards())) {
                // there are enough active shards to meet the requirements of the request
                waitForCounter++;
            }
        }
        if (request.indices() != null && request.indices().length > 0) {
            try {
                indexNameExpressionResolver.concreteIndexNames(clusterState, IndicesOptions.strictExpand(), request.indices());
                waitForCounter++;
            } catch (IndexNotFoundException e) {
                response.setStatus(ClusterHealthStatus.RED); // no indices, make sure its RED
                // missing indices, wait a bit more...
            }
        }
        if (!request.waitForNodes().isEmpty()) {
            if (request.waitForNodes().startsWith(">=")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(2));
                if (response.getNumberOfNodes() >= expected) {
                    waitForCounter++;
                }
            } else if (request.waitForNodes().startsWith("ge(")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(3, request.waitForNodes().length() - 1));
                if (response.getNumberOfNodes() >= expected) {
                    waitForCounter++;
                }
            } else if (request.waitForNodes().startsWith("<=")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(2));
                if (response.getNumberOfNodes() <= expected) {
                    waitForCounter++;
                }
            } else if (request.waitForNodes().startsWith("le(")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(3, request.waitForNodes().length() - 1));
                if (response.getNumberOfNodes() <= expected) {
                    waitForCounter++;
                }
            } else if (request.waitForNodes().startsWith(">")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(1));
                if (response.getNumberOfNodes() > expected) {
                    waitForCounter++;
                }
            } else if (request.waitForNodes().startsWith("gt(")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(3, request.waitForNodes().length() - 1));
                if (response.getNumberOfNodes() > expected) {
                    waitForCounter++;
                }
            } else if (request.waitForNodes().startsWith("<")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(1));
                if (response.getNumberOfNodes() < expected) {
                    waitForCounter++;
                }
            } else if (request.waitForNodes().startsWith("lt(")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(3, request.waitForNodes().length() - 1));
                if (response.getNumberOfNodes() < expected) {
                    waitForCounter++;
                }
            } else {
                int expected = Integer.parseInt(request.waitForNodes());
                if (response.getNumberOfNodes() == expected) {
                    waitForCounter++;
                }
            }
        }
        return waitForCounter;
    }


    private ClusterHealthResponse clusterHealth(ClusterHealthRequest request, ClusterState clusterState, int numberOfPendingTasks, int numberOfInFlightFetch,
                                                TimeValue pendingTaskTimeInQueue) {
        if (logger.isTraceEnabled()) {
            logger.trace("Calculating health based on state version [{}]", clusterState.version());
        }

        String[] concreteIndices;
        try {
            concreteIndices = indexNameExpressionResolver.concreteIndexNames(clusterState, request);
        } catch (IndexNotFoundException e) {
            ClusterStateHealth stateHealth = calculateStateHealth(clusterState, Strings.EMPTY_ARRAY);
            ClusterHealthResponse response = new ClusterHealthResponse(clusterState.getClusterName().value(), numberOfPendingTasks,
                    numberOfInFlightFetch, UnassignedInfo.getNumberOfDelayedUnassigned(clusterState), pendingTaskTimeInQueue,
                    false, stateHealth);
            // one of the specified indices is not there - treat it as RED.
            response.setStatus(ClusterHealthStatus.RED);
            return response;
        }
        ClusterStateHealth stateHealth = calculateStateHealth(clusterState, concreteIndices);
        return new ClusterHealthResponse(clusterState.getClusterName().value(), numberOfPendingTasks, numberOfInFlightFetch,
                UnassignedInfo.getNumberOfDelayedUnassigned(clusterState), pendingTaskTimeInQueue, false, stateHealth);
    }

    /**
     * Creates a new <code>ClusterStateHealth</code> instance considering the current cluster state and all indices in the cluster.
     *
     * @param clusterState The current cluster state. Must not be null.
     */
    public static ClusterStateHealth calculateStateHealth(final ClusterState clusterState) {
        return calculateStateHealth(clusterState, clusterState.metaData().getConcreteAllIndices());
    }

    /**
     * Creates a new <code>ClusterStateHealth</code> instance considering the current cluster state and the provided index names.
     *
     * @param clusterState    The current cluster state. Must not be null.
     * @param concreteIndices An array of index names to consider. Must not be null but may be empty.
     */
    public static ClusterStateHealth calculateStateHealth(final ClusterState clusterState, final String[] concreteIndices) {
        int numberOfNodes = clusterState.nodes().getSize();
        int numberOfDataNodes = clusterState.nodes().getDataNodes().size();
        Map<String, ClusterIndexHealth> indices = new HashMap<>();
        for (String index : concreteIndices) {
            IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(index);
            IndexMetaData indexMetaData = clusterState.metaData().index(index);
            if (indexRoutingTable == null) {
                continue;
            }
            ClusterIndexHealth indexHealth = calculateIndexHealth(indexMetaData, indexRoutingTable);
            indices.put(indexHealth.getIndex(), indexHealth);
        }
        ClusterHealthStatus computeStatus = ClusterHealthStatus.GREEN;
        int computeActivePrimaryShards = 0;
        int computeActiveShards = 0;
        int computeRelocatingShards = 0;
        int computeInitializingShards = 0;
        int computeUnassignedShards = 0;

        for (ClusterIndexHealth indexHealth : indices.values()) {
            computeActivePrimaryShards += indexHealth.getActivePrimaryShards();
            computeActiveShards += indexHealth.getActiveShards();
            computeRelocatingShards += indexHealth.getRelocatingShards();
            computeInitializingShards += indexHealth.getInitializingShards();
            computeUnassignedShards += indexHealth.getUnassignedShards();
            if (indexHealth.getStatus() == ClusterHealthStatus.RED) {
                computeStatus = ClusterHealthStatus.RED;
            } else if (indexHealth.getStatus() == ClusterHealthStatus.YELLOW && computeStatus != ClusterHealthStatus.RED) {
                computeStatus = ClusterHealthStatus.YELLOW;
            }
        }
        if (clusterState.blocks().hasGlobalBlock(RestStatus.SERVICE_UNAVAILABLE)) {
            computeStatus = ClusterHealthStatus.RED;
        }
        double activeShardsPercent;
        // shortcut on green
        if (computeStatus.equals(ClusterHealthStatus.GREEN)) {
            activeShardsPercent = 100;
        } else {
            List<ShardRouting> shardRoutings = clusterState.getRoutingTable().allShards();
            int activeShardCount = 0;
            int totalShardCount = 0;
            for (ShardRouting shardRouting : shardRoutings) {
                if (shardRouting.active()) activeShardCount++;
                totalShardCount++;
            }
            activeShardsPercent = (((double) activeShardCount) / totalShardCount) * 100;
        }
        return new ClusterStateHealth(computeActivePrimaryShards, computeActiveShards, computeRelocatingShards,
                computeInitializingShards, computeUnassignedShards, numberOfNodes, numberOfDataNodes, activeShardsPercent,
                computeStatus, indices);
    }

    public static ClusterIndexHealth calculateIndexHealth(final IndexMetaData indexMetaData, final IndexRoutingTable indexRoutingTable) {
        String index = indexMetaData.getIndex().getName();
        int numberOfShards = indexMetaData.getNumberOfShards();
        int numberOfReplicas = indexMetaData.getNumberOfReplicas();

        Map<Integer, ClusterShardHealth> shards = new HashMap<>();
        for (IndexShardRoutingTable shardRoutingTable : indexRoutingTable) {
            int shardId = shardRoutingTable.shardId().id();
            shards.put(shardId, calculateShardHealth(shardId, shardRoutingTable));
        }

        // update the index status
        ClusterHealthStatus computeStatus = ClusterHealthStatus.GREEN;
        int computeActivePrimaryShards = 0;
        int computeActiveShards = 0;
        int computeRelocatingShards = 0;
        int computeInitializingShards = 0;
        int computeUnassignedShards = 0;
        for (ClusterShardHealth shardHealth : shards.values()) {
            if (shardHealth.isPrimaryActive()) {
                computeActivePrimaryShards++;
            }
            computeActiveShards += shardHealth.getActiveShards();
            computeRelocatingShards += shardHealth.getRelocatingShards();
            computeInitializingShards += shardHealth.getInitializingShards();
            computeUnassignedShards += shardHealth.getUnassignedShards();

            if (shardHealth.getStatus() == ClusterHealthStatus.RED) {
                computeStatus = ClusterHealthStatus.RED;
            } else if (shardHealth.getStatus() == ClusterHealthStatus.YELLOW && computeStatus != ClusterHealthStatus.RED) {
                // do not override an existing red
                computeStatus = ClusterHealthStatus.YELLOW;
            }
        }
        if (shards.isEmpty()) { // might be since none has been created yet (two phase index creation)
            computeStatus = ClusterHealthStatus.RED;
        }
        return new ClusterIndexHealth(index, numberOfShards, numberOfReplicas, computeActiveShards, computeRelocatingShards,
                computeInitializingShards, computeUnassignedShards, computeActivePrimaryShards, computeStatus, shards);
    }

    public static ClusterShardHealth calculateShardHealth(final int shardId, final IndexShardRoutingTable shardRoutingTable) {
        int computeActiveShards = 0;
        int computeRelocatingShards = 0;
        int computeInitializingShards = 0;
        int computeUnassignedShards = 0;
        for (ShardRouting shardRouting : shardRoutingTable) {
            if (shardRouting.active()) {
                computeActiveShards++;
                if (shardRouting.relocating()) {
                    // the shard is relocating, the one it is relocating to will be in initializing state, so we don't count it
                    computeRelocatingShards++;
                }
            } else if (shardRouting.initializing()) {
                computeInitializingShards++;
            } else if (shardRouting.unassigned()) {
                computeUnassignedShards++;
            }
        }
        ClusterHealthStatus computeStatus;
        final ShardRouting primaryRouting = shardRoutingTable.primaryShard();
        if (primaryRouting.active()) {
            if (computeActiveShards == shardRoutingTable.size()) {
                computeStatus = ClusterHealthStatus.GREEN;
            } else {
                computeStatus = ClusterHealthStatus.YELLOW;
            }
        } else {
            computeStatus = getInactivePrimaryHealth(primaryRouting);
        }
        return new ClusterShardHealth(shardId, computeStatus, computeActiveShards, computeRelocatingShards, computeInitializingShards,
                computeUnassignedShards, primaryRouting.active());
    }

    /**
     * Checks if an inactive primary shard should cause the cluster health to go RED.
     *
     * An inactive primary shard in an index should cause the cluster health to be RED to make it visible that some of the existing data is
     * unavailable. In case of index creation, snapshot restore or index shrinking, which are unexceptional events in the cluster lifecycle,
     * cluster health should not turn RED for the time where primaries are still in the initializing state but go to YELLOW instead.
     * However, in case of exceptional events, for example when the primary shard cannot be assigned to a node or initialization fails at
     * some point, cluster health should still turn RED.
     *
     * NB: this method should *not* be called on active shards nor on non-primary shards.
     */
    public static ClusterHealthStatus getInactivePrimaryHealth(final ShardRouting shardRouting) {
        assert shardRouting.primary() : "cannot invoke on a replica shard: " + shardRouting;
        assert shardRouting.active() == false : "cannot invoke on an active shard: " + shardRouting;
        assert shardRouting.unassignedInfo() != null : "cannot invoke on a shard with no UnassignedInfo: " + shardRouting;
        assert shardRouting.recoverySource() != null : "cannot invoke on a shard that has no recovery source" + shardRouting;
        final UnassignedInfo unassignedInfo = shardRouting.unassignedInfo();
        RecoverySource.Type recoveryType = shardRouting.recoverySource().getType();
        if (unassignedInfo.getLastAllocationStatus() != AllocationStatus.DECIDERS_NO && unassignedInfo.getNumFailedAllocations() == 0
                && (recoveryType == RecoverySource.Type.EMPTY_STORE
                || recoveryType == RecoverySource.Type.LOCAL_SHARDS
                || recoveryType == RecoverySource.Type.SNAPSHOT)) {
            return ClusterHealthStatus.YELLOW;
        } else {
            return ClusterHealthStatus.RED;
        }
    }
}

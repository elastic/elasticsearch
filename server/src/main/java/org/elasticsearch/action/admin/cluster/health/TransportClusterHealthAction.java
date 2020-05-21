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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.LocalClusterUpdateTask;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class TransportClusterHealthAction extends TransportMasterNodeReadAction<ClusterHealthRequest, ClusterHealthResponse> {

    private static final Logger logger = LogManager.getLogger(TransportClusterHealthAction.class);

    private final AllocationService allocationService;

    @Inject
    public TransportClusterHealthAction(TransportService transportService, ClusterService clusterService,
                                        ThreadPool threadPool, ActionFilters actionFilters,
                                        IndexNameExpressionResolver indexNameExpressionResolver, AllocationService allocationService) {
        super(ClusterHealthAction.NAME, false, transportService, clusterService, threadPool, actionFilters,
            ClusterHealthRequest::new, indexNameExpressionResolver);
        this.allocationService = allocationService;
    }

    @Override
    protected String executor() {
        // this should be executing quickly no need to fork off
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterHealthResponse read(StreamInput in) throws IOException {
        return new ClusterHealthResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterHealthRequest request, ClusterState state) {
        // we want users to be able to call this even when there are global blocks, just to check the health (are there blocks?)
        return null;
    }

    @Override
    protected void masterOperation(final Task task,
                                   final ClusterHealthRequest request,
                                   final ClusterState unusedState,
                                   final ActionListener<ClusterHealthResponse> listener) {

        final int waitCount = getWaitCount(request);

        if (request.waitForEvents() != null) {
            waitForEventsAndExecuteHealth(request, listener, waitCount, threadPool.relativeTimeInMillis() + request.timeout().millis());
        } else {
            executeHealth(request, clusterService.state(), listener, waitCount,
                clusterState -> listener.onResponse(getResponse(request, clusterState, waitCount, false)));
        }
    }

    private void waitForEventsAndExecuteHealth(final ClusterHealthRequest request,
                                               final ActionListener<ClusterHealthResponse> listener,
                                               final int waitCount,
                                               final long endTimeRelativeMillis) {
        assert request.waitForEvents() != null;
        if (request.local()) {
            clusterService.submitStateUpdateTask("cluster_health (wait_for_events [" + request.waitForEvents() + "])",
                new LocalClusterUpdateTask(request.waitForEvents()) {
                    @Override
                    public ClusterTasksResult<LocalClusterUpdateTask> execute(ClusterState currentState) {
                        return unchanged();
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        final long timeoutInMillis = Math.max(0, endTimeRelativeMillis - threadPool.relativeTimeInMillis());
                        final TimeValue newTimeout = TimeValue.timeValueMillis(timeoutInMillis);
                        request.timeout(newTimeout);
                        executeHealth(request, clusterService.state(), listener, waitCount,
                            observedState -> waitForEventsAndExecuteHealth(request, listener, waitCount, endTimeRelativeMillis));
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        logger.error(() -> new ParameterizedMessage("unexpected failure during [{}]", source), e);
                        listener.onFailure(e);
                    }
                });
        } else {
            clusterService.submitStateUpdateTask("cluster_health (wait_for_events [" + request.waitForEvents() + "])",
                new ClusterStateUpdateTask(request.waitForEvents()) {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        return currentState;
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        final long timeoutInMillis = Math.max(0, endTimeRelativeMillis - threadPool.relativeTimeInMillis());
                        final TimeValue newTimeout = TimeValue.timeValueMillis(timeoutInMillis);
                        request.timeout(newTimeout);

                        // we must use the state from the applier service, because if the state-not-recovered block is in place then the
                        // applier service has a different view of the cluster state from the one supplied here
                        final ClusterState appliedState = clusterService.state();
                        assert newState.stateUUID().equals(appliedState.stateUUID())
                            : newState.stateUUID() + " vs " + appliedState.stateUUID();
                        executeHealth(request, appliedState, listener, waitCount,
                            observedState -> waitForEventsAndExecuteHealth(request, listener, waitCount, endTimeRelativeMillis));
                    }

                    @Override
                    public void onNoLongerMaster(String source) {
                        logger.trace("stopped being master while waiting for events with priority [{}]. retrying.",
                            request.waitForEvents());
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
    }

    private void executeHealth(final ClusterHealthRequest request,
                               final ClusterState currentState,
                               final ActionListener<ClusterHealthResponse> listener,
                               final int waitCount,
                               final Consumer<ClusterState> onNewClusterStateAfterDelay) {

        if (request.timeout().millis() == 0) {
            listener.onResponse(getResponse(request, currentState, waitCount, true));
            return;
        }

        final Predicate<ClusterState> validationPredicate = newState -> validateRequest(request, newState, waitCount);
        if (validationPredicate.test(currentState)) {
            listener.onResponse(getResponse(request, currentState, waitCount, false));
        } else {
            final ClusterStateObserver observer
                = new ClusterStateObserver(currentState, clusterService, null, logger, threadPool.getThreadContext());
            final ClusterStateObserver.Listener stateListener = new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState newState) {
                    onNewClusterStateAfterDelay.accept(newState);
                }

                @Override
                public void onClusterServiceClose() {
                    listener.onFailure(new IllegalStateException("ClusterService was close during health call"));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    listener.onResponse(getResponse(request, observer.setAndGetObservedState(), waitCount, true));
                }
            };
            observer.waitForNextChange(stateListener, validationPredicate, request.timeout());
        }
    }

    private static int getWaitCount(ClusterHealthRequest request) {
        int waitCount = 0;
        if (request.waitForStatus() != null) {
            waitCount++;
        }
        if (request.waitForNoRelocatingShards()) {
            waitCount++;
        }
        if (request.waitForNoInitializingShards()) {
            waitCount++;
        }
        if (request.waitForActiveShards().equals(ActiveShardCount.NONE) == false) {
            waitCount++;
        }
        if (request.waitForNodes().isEmpty() == false) {
            waitCount++;
        }
        if (CollectionUtils.isEmpty(request.indices()) == false) { // check that they actually exists in the meta data
            waitCount++;
        }
        return waitCount;
    }

    private boolean validateRequest(final ClusterHealthRequest request, ClusterState clusterState, final int waitCount) {
        ClusterHealthResponse response = clusterHealth(request, clusterState, clusterService.getMasterService().numberOfPendingTasks(),
            allocationService.getNumberOfInFlightFetches(), clusterService.getMasterService().getMaxTaskWaitTime());
        return prepareResponse(request, response, clusterState, indexNameExpressionResolver) == waitCount;
    }

    private ClusterHealthResponse getResponse(final ClusterHealthRequest request, ClusterState clusterState,
                                              final int waitFor, boolean timedOut) {
        ClusterHealthResponse response = clusterHealth(request, clusterState, clusterService.getMasterService().numberOfPendingTasks(),
            allocationService.getNumberOfInFlightFetches(), clusterService.getMasterService().getMaxTaskWaitTime());
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
            if (waitForActiveShards.equals(ActiveShardCount.ALL)) {
                if (response.getUnassignedShards() == 0 && response.getInitializingShards() == 0) {
                    // if we are waiting for all shards to be active, then the num of unassigned and num of initializing shards must be 0
                    waitForCounter++;
                }
            } else if (waitForActiveShards.enoughShardsActive(response.getActiveShards())) {
                // there are enough active shards to meet the requirements of the request
                waitForCounter++;
            }
        }
        if (CollectionUtils.isEmpty(request.indices()) == false) {
            try {
                indexNameExpressionResolver.concreteIndexNames(clusterState, IndicesOptions.strictExpand(), true, request.indices());
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


    private ClusterHealthResponse clusterHealth(ClusterHealthRequest request, ClusterState clusterState, int numberOfPendingTasks,
                                                int numberOfInFlightFetch, TimeValue pendingTaskTimeInQueue) {
        if (logger.isTraceEnabled()) {
            logger.trace("Calculating health based on state version [{}]", clusterState.version());
        }

        String[] concreteIndices;
        try {
            concreteIndices = indexNameExpressionResolver.concreteIndexNames(clusterState, request, true);
        } catch (IndexNotFoundException e) {
            // one of the specified indices is not there - treat it as RED.
            ClusterHealthResponse response = new ClusterHealthResponse(clusterState.getClusterName().value(), Strings.EMPTY_ARRAY,
                clusterState, numberOfPendingTasks, numberOfInFlightFetch, UnassignedInfo.getNumberOfDelayedUnassigned(clusterState),
                pendingTaskTimeInQueue);
            response.setStatus(ClusterHealthStatus.RED);
            return response;
        }

        return new ClusterHealthResponse(clusterState.getClusterName().value(), concreteIndices, clusterState, numberOfPendingTasks,
                numberOfInFlightFetch, UnassignedInfo.getNumberOfDelayedUnassigned(clusterState), pendingTaskTimeInQueue);
    }
}

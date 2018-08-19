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
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

public class JoinHelper extends AbstractComponent {

    public static final String JOIN_ACTION_NAME = "internal:cluster/coordination/join";

    private final MasterService masterService;
    private final TransportService transportService;
    private final JoinTaskExecutor joinTaskExecutor;
    private final Map<DiscoveryNode, JoinCallback> joinRequestAccumulator = new HashMap<>();

    public JoinHelper(Settings settings, AllocationService allocationService, MasterService masterService,
                      TransportService transportService, LongSupplier currentTermSupplier,
                      BiConsumer<JoinRequest, JoinCallback> joinRequestHandler) {
        super(settings);
        this.masterService = masterService;
        this.transportService = transportService;
        this.joinTaskExecutor = new JoinTaskExecutor(allocationService, logger) {

            @Override
            public ClusterTasksResult<JoinTaskExecutor.Task> execute(ClusterState currentState, List<JoinTaskExecutor.Task> joiningTasks)
                throws Exception {
                // This is called when preparing the next cluster state for publication. There is no guarantee that the term we see here is
                // the term under which this state will eventually be published: the current term may be increased after this check due to
                // some other activity. That the term is correct is, however, checked properly during publication, so it is sufficient to
                // check it here on a best-effort basis. This is fine because a concurrent change indicates the existence of another leader
                // in a higher term which will cause this node to stand down.

                final long currentTerm = currentTermSupplier.getAsLong();
                if (currentState.term() != currentTerm) {
                    currentState = ClusterState.builder(currentState).term(currentTerm).build();
                }
                return super.execute(currentState, joiningTasks);
            }

        };

        transportService.registerRequestHandler(JOIN_ACTION_NAME, ThreadPool.Names.GENERIC, false, false, JoinRequest::new,
            (request, channel, task) -> joinRequestHandler.accept(request, new JoinCallback() {
                @Override
                public void onSuccess() {
                    try {
                        channel.sendResponse(TransportResponse.Empty.INSTANCE);
                    } catch (IOException e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception inner) {
                        inner.addSuppressed(e);
                        logger.warn("failed to send back failure on join request", inner);
                    }
                }
            }));
    }

    public void addPendingJoin(JoinRequest joinRequest, JoinCallback joinCallback) {
        JoinCallback prev = joinRequestAccumulator.put(joinRequest.getSourceNode(), joinCallback);
        if (prev != null) {
            prev.onFailure(new CoordinationStateRejectedException("received a newer join from " + joinRequest.getSourceNode()));
        }
    }

    public int getNumberOfPendingJoins() {
        return joinRequestAccumulator.size();
    }

    public void clearAndFailJoins(String reason) {
        joinRequestAccumulator.values().forEach(
            joinCallback -> joinCallback.onFailure(new CoordinationStateRejectedException(reason)));
        joinRequestAccumulator.clear();
    }

    public void clearAndSubmitPendingJoins() {
        final Map<JoinTaskExecutor.Task, ClusterStateTaskListener> pendingAsTasks = new HashMap<>();
        joinRequestAccumulator.forEach((key, value) -> pendingAsTasks.put(new JoinTaskExecutor.Task(key, "elect leader"),
            new JoinTaskListener(value)));
        joinRequestAccumulator.clear();

        pendingAsTasks.put(JoinTaskExecutor.BECOME_MASTER_TASK, (source, e) -> {});
        pendingAsTasks.put(JoinTaskExecutor.FINISH_ELECTION_TASK, (source, e) -> {});
        final String source = "elected-as-master ([" + pendingAsTasks.size() + "] nodes joined)";
        masterService.submitStateUpdateTasks(source, pendingAsTasks, ClusterStateTaskConfig.build(Priority.URGENT), joinTaskExecutor);
    }

    public void joinLeader(JoinRequest joinRequest, JoinCallback joinCallback) {
        // submit as cluster state update task
        masterService.submitStateUpdateTask("node-join",
            new JoinTaskExecutor.Task(joinRequest.getSourceNode(), "join existing leader"), ClusterStateTaskConfig.build(Priority.URGENT),
            joinTaskExecutor, new JoinTaskListener(joinCallback));
    }

    public interface JoinCallback {
        void onSuccess();

        void onFailure(Exception e);
    }

    static class JoinTaskListener implements ClusterStateTaskListener {
        private final JoinCallback joinCallback;

        JoinTaskListener(JoinCallback joinCallback) {
            this.joinCallback = joinCallback;
        }

        @Override
        public void onFailure(String source, Exception e) {
            joinCallback.onFailure(e);
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            joinCallback.onSuccess();
        }
    }

}

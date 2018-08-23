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
import org.elasticsearch.cluster.coordination.Coordinator.Mode;
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

    public JoinHelper(Settings settings, AllocationService allocationService, MasterService masterService,
                      TransportService transportService, LongSupplier currentTermSupplier,
                      BiConsumer<JoinRequest, JoinCallback> joinHandler) {
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
            (request, channel, task) -> joinHandler.accept(request, new JoinCallback() {

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

                @Override
                public String toString() {
                    return "JoinCallback{request=" + request + "}";
                }
            }));
    }

    public interface JoinCallback {
        void onSuccess();

        void onFailure(Exception e);
    }

    static class JoinTaskListener implements ClusterStateTaskListener {
        private final JoinTaskExecutor.Task task;
        private final JoinCallback joinCallback;

        JoinTaskListener(JoinTaskExecutor.Task task, JoinCallback joinCallback) {
            this.task = task;
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

        @Override
        public String toString() {
            return "JoinTaskListener{task=" + task + "}";
        }
    }

    interface JoinAccumulator {
        void handleJoinRequest(DiscoveryNode sender, JoinCallback joinCallback);

        default void close(Mode newMode) {

        }
    }

    class LeaderJoinAccumulator implements JoinAccumulator {
        @Override
        public void handleJoinRequest(DiscoveryNode sender, JoinCallback joinCallback) {
            final JoinTaskExecutor.Task task = new JoinTaskExecutor.Task(sender, "join existing leader");
            masterService.submitStateUpdateTask("node-join", task, ClusterStateTaskConfig.build(Priority.URGENT),
                joinTaskExecutor, new JoinTaskListener(task, joinCallback));
        }

        @Override
        public String toString() {
            return "LeaderJoinAccumulator";
        }
    }

    static class FollowerJoinAccumulator implements JoinAccumulator {
        @Override
        public void handleJoinRequest(DiscoveryNode sender, JoinCallback joinCallback) {
            joinCallback.onFailure(new CoordinationStateRejectedException("join target is a follower"));
        }

        @Override
        public String toString() {
            return "FollowerJoinAccumulator";
        }
    }

    class CandidateJoinAccumulator implements JoinAccumulator {

        private final Map<DiscoveryNode, JoinCallback> joinRequestAccumulator = new HashMap<>();
        boolean closed;

        @Override
        public void handleJoinRequest(DiscoveryNode sender, JoinCallback joinCallback) {
            assert closed == false : "CandidateJoinAccumulator closed";
            JoinCallback prev = joinRequestAccumulator.put(sender, joinCallback);
            if (prev != null) {
                prev.onFailure(new CoordinationStateRejectedException("received a newer join from " + sender));
            }
        }

        @Override
        public void close(Mode newMode) {
            assert closed == false : "CandidateJoinAccumulator closed";
            closed = true;
            if (newMode == Mode.LEADER) {
                final Map<JoinTaskExecutor.Task, ClusterStateTaskListener> pendingAsTasks = new HashMap<>();
                joinRequestAccumulator.forEach((key, value) -> {
                    final JoinTaskExecutor.Task task = new JoinTaskExecutor.Task(key, "elect leader");
                    pendingAsTasks.put(task, new JoinTaskListener(task, value));
                });

                final String stateUpdateSource = "elected-as-master ([" + pendingAsTasks.size() + "] nodes joined)";

                pendingAsTasks.put(JoinTaskExecutor.BECOME_MASTER_TASK, (source, e) -> {
                });
                pendingAsTasks.put(JoinTaskExecutor.FINISH_ELECTION_TASK, (source, e) -> {
                });
                masterService.submitStateUpdateTasks(stateUpdateSource, pendingAsTasks, ClusterStateTaskConfig.build(Priority.URGENT),
                    joinTaskExecutor);
            } else if (newMode == Mode.FOLLOWER) {
                joinRequestAccumulator.values().forEach(joinCallback -> joinCallback.onFailure(
                    new CoordinationStateRejectedException("became follower")));
            } else {
                assert newMode == Mode.CANDIDATE;
                assert joinRequestAccumulator.isEmpty() : joinRequestAccumulator.keySet();
            }
        }

        @Override
        public String toString() {
            return "CandidateJoinAccumulator{" + joinRequestAccumulator.keySet() + '}';
        }
    }
}

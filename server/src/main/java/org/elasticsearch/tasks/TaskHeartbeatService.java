/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.tasks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractAsyncTask;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * The TaskHeartbeatService is responsible for sending heartbeat messages to the set of nodes provided
 * by {@link TaskHeartbeatSource#getNodes()} and updating those heartbeats to the corresponding sub-tasks
 * via {@link TaskHeartbeatApplier#onUpdateTaskHeartbeats(Map)}.
 */
public final class TaskHeartbeatService extends AbstractLifecycleComponent implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(TaskHeartbeatService.class);

    /**
     * The time between two runs that send heartbeat messages to the nodes provided by {@link TaskHeartbeatSource#getNodes()}
     * Defaults to 10 seconds.
     */
    public static final Setting<TimeValue> TASK_HEARTBEAT_INTERVAL_SETTING =
        Setting.timeSetting("cluster.tasks.heartbeat_interval",
            TimeValue.timeValueSeconds(10), TimeValue.timeValueMillis(100), Setting.Property.NodeScope, Setting.Property.Dynamic);

    private static final Version HEARTBEAT_AVAILABLE_VERSION = Version.V_8_0_0;
    public static final String TASK_KEEP_ALIVE_ACTION_NAME = "internal:admin/tasks/heartbeat";

    private final TransportService transportService;
    private final ThreadPool threadPool;
    private Map<String, Long> lastReceivedHeartbeats = new HashMap<>(); // guarded by this
    private volatile DiscoveryNodes nodesInCluster = DiscoveryNodes.EMPTY_NODES; // for BWC
    private final List<TaskHeartbeatSource> sources = new CopyOnWriteArrayList<>();
    private final List<TaskHeartbeatApplier> appliers = new CopyOnWriteArrayList<>();
    private final SendHeartBeatsTask sendHeartbeatTask;
    private final UpdateHeartbeatsTask updateHeartbeatsTask;

    @Inject
    public TaskHeartbeatService(TransportService transportService, Settings settings, ClusterSettings clusterSettings) {
        this.transportService = transportService;
        this.threadPool = transportService.getThreadPool();
        transportService.registerRequestHandler(
            TASK_KEEP_ALIVE_ACTION_NAME, ThreadPool.Names.SAME, HeartbeatRequest::new, new HeartbeatRequestHandler());
        TransportActionProxy.registerProxyAction(transportService, TASK_KEEP_ALIVE_ACTION_NAME, TransportService.HandshakeResponse::new);
        final TimeValue heartbeatInterval = TASK_HEARTBEAT_INTERVAL_SETTING.get(settings);
        this.sendHeartbeatTask = new SendHeartBeatsTask(heartbeatInterval);
        this.updateHeartbeatsTask = new UpdateHeartbeatsTask(updateHeartbeatTaskInterval(heartbeatInterval));
        clusterSettings.addSettingsUpdateConsumer(TASK_HEARTBEAT_INTERVAL_SETTING, this::setHeartbeatInterval);
    }

    @Override
    protected void doStart() {
        sendHeartbeatTask.rescheduleIfNecessary();
        updateHeartbeatsTask.rescheduleIfNecessary();
    }

    @Override
    protected void doStop() {
        sendHeartbeatTask.close();
        updateHeartbeatsTask.cancel();
    }

    @Override
    protected void doClose() {

    }

    public void addApplier(TaskHeartbeatApplier applier) {
        appliers.add(applier);
    }

    public void removeApplier(TaskHeartbeatApplier applier) {
        appliers.remove(applier);
    }

    public void addSource(TaskHeartbeatSource source) {
        sources.add(source);
    }

    public void removeSource(TaskHeartbeatSource source) {
        sources.remove(source);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        nodesInCluster = event.state().getNodes();
    }

    private void setHeartbeatInterval(TimeValue heartbeatInterval) {
        sendHeartbeatTask.setInterval(heartbeatInterval);
        updateHeartbeatsTask.setInterval(updateHeartbeatTaskInterval(heartbeatInterval));
    }

    private static TimeValue updateHeartbeatTaskInterval(TimeValue heartbeatInterval) {
        // more frequently than the send task as the update task is light and does not send messages.
        return TimeValue.timeValueMillis(heartbeatInterval.millis() / 4);
    }

    private class SendHeartBeatsTask extends AbstractAsyncTask {
        final EmptyTransportResponseHandler handler = new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
            @Override
            public void handleException(TransportException exp) {
                assert ExceptionsHelper.unwrapCause(exp) instanceof ElasticsearchSecurityException == false;
            }
        };

        SendHeartBeatsTask(TimeValue interval) {
            super(logger, threadPool, interval, true);
        }

        @Override
        protected boolean mustReschedule() {
            return lifecycle.stoppedOrClosed() == false;
        }

        @Override
        protected void runInternal() {
            final Set<DiscoveryNode> childNodes = new HashSet<>();
            for (TaskHeartbeatSource source : sources) {
                childNodes.addAll(source.getNodes());
            }
            final HeartbeatRequest request = new HeartbeatRequest(transportService.getLocalNode().getId());
            final ThreadContext threadContext = threadPool.getThreadContext();
            try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                threadContext.markAsSystemContext();
                for (DiscoveryNode childNode : childNodes) {
                    if (childNode.getVersion().onOrAfter(HEARTBEAT_AVAILABLE_VERSION)) {
                        logger.trace("send task heartbeat to child node {}", childNode);
                        transportService.sendRequest(childNode, TASK_KEEP_ALIVE_ACTION_NAME, request, handler);
                    }
                }
            }
        }
    }

    private class UpdateHeartbeatsTask extends AbstractAsyncTask {
        UpdateHeartbeatsTask(TimeValue interval) {
            super(logger, threadPool, interval, true);
        }

        @Override
        protected boolean mustReschedule() {
            return lifecycle.stoppedOrClosed() == false;
        }

        @Override
        protected void runInternal() {
            final Map<String, Long> lastHeartbeats;
            synchronized (TaskHeartbeatService.this) {
                lastHeartbeats = TaskHeartbeatService.this.lastReceivedHeartbeats;
                TaskHeartbeatService.this.lastReceivedHeartbeats = new HashMap<>();
            }
            for (DiscoveryNode node : nodesInCluster) {
                if (node.getVersion().before(HEARTBEAT_AVAILABLE_VERSION)) {
                    lastHeartbeats.put(node.getId(), threadPool.relativeTimeInMillis());
                }
            }
            logger.trace("update task heartbeats from nodes {}", lastHeartbeats.keySet());
            for (TaskHeartbeatApplier applier : appliers) {
                try {
                    applier.onUpdateTaskHeartbeats(Collections.unmodifiableMap(lastHeartbeats));
                } catch (Exception e) {
                    assert false : new AssertionError("failed to update task heartbeats", e);
                    logger.warn("failed to update task heartbeats", e);
                }
            }
        }
    }

    static final class HeartbeatRequest extends TransportRequest {
        final String nodeId;

        HeartbeatRequest(String nodeId) {
            this.nodeId = Objects.requireNonNull(nodeId);
        }

        HeartbeatRequest(StreamInput in) throws IOException {
            super(in);
            this.nodeId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(nodeId);
        }
    }

    final class HeartbeatRequestHandler implements TransportRequestHandler<HeartbeatRequest> {
        @Override
        public void messageReceived(HeartbeatRequest request, TransportChannel channel, Task task) throws Exception {
            logger.trace("received task heartbeat from node {}", request.nodeId);
            final long nowInMillis = threadPool.relativeTimeInMillis();
            synchronized (TaskHeartbeatService.this) {
                lastReceivedHeartbeats.compute(request.nodeId, (k, v) -> v != null ? Math.max(v, nowInMillis) : nowInMillis);
            }
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    /**
     * A component that provides a set of nodes that require task heartbeat from the current node.
     */
    public interface TaskHeartbeatSource {
        Collection<DiscoveryNode> getNodes();
    }

    /**
     * A component that is responsible for updating the heartbeats to the corresponding sub-tasks.
     */
    public interface TaskHeartbeatApplier {
        void onUpdateTaskHeartbeats(Map<String, Long> lastHeartbeatsInMillis);
    }
}

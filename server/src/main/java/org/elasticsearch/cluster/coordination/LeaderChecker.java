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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.NodeDisconnectedException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportConnectionListener;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportRequestOptions.Type;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponse.Empty;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * The LeaderChecker is responsible for allowing followers to check that the currently elected leader is still connected and healthy. We are
 * fairly lenient, possibly allowing multiple checks to fail before considering the leader to be faulty, to allow for the leader to
 * temporarily stand down on occasion, e.g. if it needs to move to a higher term. On deciding that the leader has failed a follower will
 * become a candidate and attempt to become a leader itself.
 */
public class LeaderChecker {

    private static final Logger logger = LogManager.getLogger(LeaderChecker.class);

    static final String LEADER_CHECK_ACTION_NAME = "internal:coordination/fault_detection/leader_check";

    // the time between checks sent to the leader
    public static final Setting<TimeValue> LEADER_CHECK_INTERVAL_SETTING =
        Setting.timeSetting("cluster.fault_detection.leader_check.interval",
            TimeValue.timeValueMillis(1000), TimeValue.timeValueMillis(100), Setting.Property.NodeScope);

    // the timeout for each check sent to the leader
    public static final Setting<TimeValue> LEADER_CHECK_TIMEOUT_SETTING =
        Setting.timeSetting("cluster.fault_detection.leader_check.timeout",
            TimeValue.timeValueMillis(10000), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);

    // the number of failed checks that must happen before the leader is considered to have failed.
    public static final Setting<Integer> LEADER_CHECK_RETRY_COUNT_SETTING =
        Setting.intSetting("cluster.fault_detection.leader_check.retry_count", 3, 1, Setting.Property.NodeScope);

    private final TimeValue leaderCheckInterval;
    private final TimeValue leaderCheckTimeout;
    private final int leaderCheckRetryCount;
    private final TransportService transportService;
    private final Consumer<Exception> onLeaderFailure;

    private AtomicReference<CheckScheduler> currentChecker = new AtomicReference<>();

    private volatile DiscoveryNodes discoveryNodes;

    LeaderChecker(final Settings settings, final TransportService transportService, final Consumer<Exception> onLeaderFailure) {
        leaderCheckInterval = LEADER_CHECK_INTERVAL_SETTING.get(settings);
        leaderCheckTimeout = LEADER_CHECK_TIMEOUT_SETTING.get(settings);
        leaderCheckRetryCount = LEADER_CHECK_RETRY_COUNT_SETTING.get(settings);
        this.transportService = transportService;
        this.onLeaderFailure = onLeaderFailure;

        transportService.registerRequestHandler(LEADER_CHECK_ACTION_NAME, Names.SAME, false, false, LeaderCheckRequest::new,
            (request, channel, task) -> {
                handleLeaderCheck(request);
                channel.sendResponse(Empty.INSTANCE);
            });

        transportService.addConnectionListener(new TransportConnectionListener() {
            @Override
            public void onNodeDisconnected(DiscoveryNode node, Transport.Connection connection) {
                handleDisconnectedNode(node);
            }
        });
    }

    public DiscoveryNode leader() {
        CheckScheduler checkScheduler = currentChecker.get();
        return checkScheduler == null ? null : checkScheduler.leader;
    }

    /**
     * Starts and / or stops a leader checker for the given leader. Should only be called after successfully joining this leader.
     *
     * @param leader the node to be checked as leader, or null if checks should be disabled
     */
    void updateLeader(@Nullable final DiscoveryNode leader) {
        assert transportService.getLocalNode().equals(leader) == false;
        final CheckScheduler checkScheduler;
        if (leader != null) {
            checkScheduler = new CheckScheduler(leader);
        } else {
            checkScheduler = null;
        }
        CheckScheduler previousChecker = currentChecker.getAndSet(checkScheduler);
        if (previousChecker != null) {
            previousChecker.close();
        }
        if (checkScheduler != null) {
            checkScheduler.handleWakeUp();
        }
    }

    /**
     * Update the "known" discovery nodes. Should be called on the leader before a new cluster state is published to reflect the new
     * publication targets, and also called if a leader becomes a non-leader.
     */
    void setCurrentNodes(DiscoveryNodes discoveryNodes) {
        logger.trace("setCurrentNodes: {}", discoveryNodes);
        this.discoveryNodes = discoveryNodes;
    }

    // For assertions
    boolean currentNodeIsMaster() {
        return discoveryNodes.isLocalNodeElectedMaster();
    }

    private void handleLeaderCheck(LeaderCheckRequest request) {
        final DiscoveryNodes discoveryNodes = this.discoveryNodes;
        assert discoveryNodes != null;

        if (discoveryNodes.isLocalNodeElectedMaster() == false) {
            logger.debug("rejecting leader check on non-master {}", request);
            throw new CoordinationStateRejectedException(
                "rejecting leader check from [" + request.getSender() + "] sent to a node that is no longer the master");
        } else if (discoveryNodes.nodeExists(request.getSender()) == false) {
            logger.debug("rejecting leader check from removed node: {}", request);
            throw new CoordinationStateRejectedException(
                "rejecting leader check since [" + request.getSender() + "] has been removed from the cluster");
        } else {
            logger.trace("handling {}", request);
        }
    }

    private void handleDisconnectedNode(DiscoveryNode discoveryNode) {
        CheckScheduler checkScheduler = currentChecker.get();
        if (checkScheduler != null) {
            checkScheduler.handleDisconnectedNode(discoveryNode);
        } else {
            logger.trace("disconnect event ignored for {}, no check scheduler", discoveryNode);
        }
    }

    private class CheckScheduler implements Releasable {

        private final AtomicBoolean isClosed = new AtomicBoolean();
        private final AtomicLong failureCountSinceLastSuccess = new AtomicLong();
        private final DiscoveryNode leader;

        CheckScheduler(final DiscoveryNode leader) {
            this.leader = leader;
        }

        @Override
        public void close() {
            if (isClosed.compareAndSet(false, true) == false) {
                logger.trace("already closed, doing nothing");
            } else {
                logger.debug("closed");
            }
        }

        void handleWakeUp() {
            if (isClosed.get()) {
                logger.trace("closed check scheduler woken up, doing nothing");
                return;
            }

            logger.trace("checking {} with [{}] = {}", leader, LEADER_CHECK_TIMEOUT_SETTING.getKey(), leaderCheckTimeout);

            transportService.sendRequest(leader, LEADER_CHECK_ACTION_NAME, new LeaderCheckRequest(transportService.getLocalNode()),
                TransportRequestOptions.builder().withTimeout(leaderCheckTimeout).withType(Type.PING).build(),

                new TransportResponseHandler<TransportResponse.Empty>() {

                    @Override
                    public Empty read(StreamInput in) {
                        return Empty.INSTANCE;
                    }

                    @Override
                    public void handleResponse(Empty response) {
                        if (isClosed.get()) {
                            logger.debug("closed check scheduler received a response, doing nothing");
                            return;
                        }

                        failureCountSinceLastSuccess.set(0);
                        scheduleNextWakeUp(); // logs trace message indicating success
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        if (isClosed.get()) {
                            logger.debug("closed check scheduler received a response, doing nothing");
                            return;
                        }

                        if (exp instanceof ConnectTransportException || exp.getCause() instanceof ConnectTransportException) {
                            logger.debug(new ParameterizedMessage(
                                "leader [{}] disconnected during check", leader), exp);
                            leaderFailed(new ConnectTransportException(leader, "disconnected during check", exp));
                            return;
                        }

                        long failureCount = failureCountSinceLastSuccess.incrementAndGet();
                        if (failureCount >= leaderCheckRetryCount) {
                            logger.debug(new ParameterizedMessage(
                                "leader [{}] has failed {} consecutive checks (limit [{}] is {}); last failure was:",
                                leader, failureCount, LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), leaderCheckRetryCount), exp);
                            leaderFailed(new ElasticsearchException(
                                "node [" + leader + "] failed [" + failureCount + "] consecutive checks", exp));
                            return;
                        }

                        logger.debug(new ParameterizedMessage("{} consecutive failures (limit [{}] is {}) with leader [{}]",
                            failureCount, LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), leaderCheckRetryCount, leader), exp);
                        scheduleNextWakeUp();
                    }

                    @Override
                    public String executor() {
                        return Names.SAME;
                    }
                });
        }

        void leaderFailed(Exception e) {
            if (isClosed.compareAndSet(false, true)) {
                transportService.getThreadPool().generic().execute(new Runnable() {
                    @Override
                    public void run() {
                        onLeaderFailure.accept(e);
                    }

                    @Override
                    public String toString() {
                        return "notification of leader failure: " + e.getMessage();
                    }
                });
            } else {
                logger.trace("already closed, not failing leader");
            }
        }

        void handleDisconnectedNode(DiscoveryNode discoveryNode) {
            if (discoveryNode.equals(leader)) {
                logger.debug("leader [{}] disconnected", leader);
                leaderFailed(new NodeDisconnectedException(discoveryNode, "disconnected"));
            }
        }

        private void scheduleNextWakeUp() {
            logger.trace("scheduling next check of {} for [{}] = {}", leader, LEADER_CHECK_INTERVAL_SETTING.getKey(), leaderCheckInterval);
            transportService.getThreadPool().schedule(new Runnable() {
                @Override
                public void run() {
                    handleWakeUp();
                }

                @Override
                public String toString() {
                    return "scheduled check of leader " + leader;
                }
            }, leaderCheckInterval, Names.SAME);
        }
    }

    static class LeaderCheckRequest extends TransportRequest {

        private final DiscoveryNode sender;

        LeaderCheckRequest(final DiscoveryNode sender) {
            this.sender = sender;
        }

        LeaderCheckRequest(final StreamInput in) throws IOException {
            super(in);
            sender = new DiscoveryNode(in);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            sender.writeTo(out);
        }

        public DiscoveryNode getSender() {
            return sender;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final LeaderCheckRequest that = (LeaderCheckRequest) o;
            return Objects.equals(sender, that.sender);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sender);
        }

        @Override
        public String toString() {
            return "LeaderCheckRequest{" +
                "sender=" + sender +
                '}';
        }
    }
}


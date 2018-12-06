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

package org.elasticsearch.discovery.zen;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class MembershipAction {

    private static final Logger logger = LogManager.getLogger(MembershipAction.class);

    public static final String DISCOVERY_JOIN_ACTION_NAME = "internal:discovery/zen/join";
    public static final String DISCOVERY_JOIN_VALIDATE_ACTION_NAME = "internal:discovery/zen/join/validate";
    public static final String DISCOVERY_LEAVE_ACTION_NAME = "internal:discovery/zen/leave";

    public interface JoinCallback {
        void onSuccess();

        void onFailure(Exception e);
    }

    public interface MembershipListener {
        void onJoin(DiscoveryNode node, JoinCallback callback);

        void onLeave(DiscoveryNode node);
    }

    private final TransportService transportService;

    private final MembershipListener listener;

    public MembershipAction(TransportService transportService, MembershipListener listener,
                            Collection<BiConsumer<DiscoveryNode,ClusterState>> joinValidators) {
        this.transportService = transportService;
        this.listener = listener;


        transportService.registerRequestHandler(DISCOVERY_JOIN_ACTION_NAME, JoinRequest::new,
            ThreadPool.Names.GENERIC, new JoinRequestRequestHandler());
        transportService.registerRequestHandler(DISCOVERY_JOIN_VALIDATE_ACTION_NAME,
            () -> new ValidateJoinRequest(), ThreadPool.Names.GENERIC,
            new ValidateJoinRequestRequestHandler(transportService::getLocalNode, joinValidators));
        transportService.registerRequestHandler(DISCOVERY_LEAVE_ACTION_NAME, LeaveRequest::new,
            ThreadPool.Names.GENERIC, new LeaveRequestRequestHandler());
    }

    public void sendLeaveRequest(DiscoveryNode masterNode, DiscoveryNode node) {
        transportService.sendRequest(node, DISCOVERY_LEAVE_ACTION_NAME, new LeaveRequest(masterNode),
            EmptyTransportResponseHandler.INSTANCE_SAME);
    }

    public void sendLeaveRequestBlocking(DiscoveryNode masterNode, DiscoveryNode node, TimeValue timeout) {
        transportService.submitRequest(masterNode, DISCOVERY_LEAVE_ACTION_NAME, new LeaveRequest(node),
            EmptyTransportResponseHandler.INSTANCE_SAME).txGet(timeout.millis(), TimeUnit.MILLISECONDS);
    }

    public void sendJoinRequestBlocking(DiscoveryNode masterNode, DiscoveryNode node, TimeValue timeout) {
        transportService.submitRequest(masterNode, DISCOVERY_JOIN_ACTION_NAME, new JoinRequest(node),
            EmptyTransportResponseHandler.INSTANCE_SAME).txGet(timeout.millis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Validates the join request, throwing a failure if it failed.
     */
    public void sendValidateJoinRequestBlocking(DiscoveryNode node, ClusterState state, TimeValue timeout) {
        transportService.submitRequest(node, DISCOVERY_JOIN_VALIDATE_ACTION_NAME, new ValidateJoinRequest(state),
            EmptyTransportResponseHandler.INSTANCE_SAME).txGet(timeout.millis(), TimeUnit.MILLISECONDS);
    }

    public static class JoinRequest extends TransportRequest {

        public DiscoveryNode node;

        public JoinRequest() {
        }

        public JoinRequest(DiscoveryNode node) {
            this.node = node;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            node = new DiscoveryNode(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            node.writeTo(out);
        }
    }


    private class JoinRequestRequestHandler implements TransportRequestHandler<JoinRequest> {

        @Override
        public void messageReceived(final JoinRequest request, final TransportChannel channel, Task task) throws Exception {
            listener.onJoin(request.node, new JoinCallback() {
                @Override
                public void onSuccess() {
                    try {
                        channel.sendResponse(TransportResponse.Empty.INSTANCE);
                    } catch (Exception e) {
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
            });
        }
    }

    public static class ValidateJoinRequest extends TransportRequest {
        private ClusterState state;

        public ValidateJoinRequest() {}

        ValidateJoinRequest(ClusterState state) {
            this.state = state;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            this.state = ClusterState.readFrom(in, null);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            this.state.writeTo(out);
        }
    }

    static class ValidateJoinRequestRequestHandler implements TransportRequestHandler<ValidateJoinRequest> {
        private final Supplier<DiscoveryNode> localNodeSupplier;
        private final Collection<BiConsumer<DiscoveryNode, ClusterState>> joinValidators;

        ValidateJoinRequestRequestHandler(Supplier<DiscoveryNode> localNodeSupplier,
                                          Collection<BiConsumer<DiscoveryNode, ClusterState>> joinValidators) {
            this.localNodeSupplier = localNodeSupplier;
            this.joinValidators = joinValidators;
        }

        @Override
        public void messageReceived(ValidateJoinRequest request, TransportChannel channel, Task task) throws Exception {
            DiscoveryNode node = localNodeSupplier.get();
            assert node != null : "local node is null";
            joinValidators.stream().forEach(action -> action.accept(node, request.state));
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    public static class LeaveRequest extends TransportRequest {

        private DiscoveryNode node;

        public LeaveRequest() {
        }

        private LeaveRequest(DiscoveryNode node) {
            this.node = node;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            node = new DiscoveryNode(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            node.writeTo(out);
        }
    }

    private class LeaveRequestRequestHandler implements TransportRequestHandler<LeaveRequest> {

        @Override
        public void messageReceived(LeaveRequest request, TransportChannel channel, Task task) throws Exception {
            listener.onLeave(request.node);
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }
}

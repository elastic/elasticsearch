/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.discovery.zen.membership;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.io.stream.VoidStreamable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.zen.DiscoveryNodesProvider;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class MembershipAction extends AbstractComponent {

    public static interface MembershipListener {
        ClusterState onJoin(DiscoveryNode node);

        void onLeave(DiscoveryNode node);
    }

    private final TransportService transportService;

    private final DiscoveryNodesProvider nodesProvider;

    private final MembershipListener listener;

    public MembershipAction(Settings settings, TransportService transportService, DiscoveryNodesProvider nodesProvider, MembershipListener listener) {
        super(settings);
        this.transportService = transportService;
        this.nodesProvider = nodesProvider;
        this.listener = listener;

        transportService.registerHandler(JoinRequestRequestHandler.ACTION, new JoinRequestRequestHandler());
        transportService.registerHandler(LeaveRequestRequestHandler.ACTION, new LeaveRequestRequestHandler());
    }

    public void close() {
        transportService.removeHandler(JoinRequestRequestHandler.ACTION);
        transportService.removeHandler(LeaveRequestRequestHandler.ACTION);
    }

    public void sendLeaveRequest(DiscoveryNode masterNode, DiscoveryNode node) {
        transportService.sendRequest(node, LeaveRequestRequestHandler.ACTION, new LeaveRequest(masterNode), VoidTransportResponseHandler.INSTANCE_SAME);
    }

    public void sendLeaveRequestBlocking(DiscoveryNode masterNode, DiscoveryNode node, TimeValue timeout) throws ElasticSearchException {
        transportService.submitRequest(masterNode, LeaveRequestRequestHandler.ACTION, new LeaveRequest(node), VoidTransportResponseHandler.INSTANCE_SAME).txGet(timeout.millis(), TimeUnit.MILLISECONDS);
    }

    public void sendJoinRequest(DiscoveryNode masterNode, DiscoveryNode node) {
        transportService.sendRequest(masterNode, JoinRequestRequestHandler.ACTION, new JoinRequest(node, false), VoidTransportResponseHandler.INSTANCE_SAME);
    }

    public ClusterState sendJoinRequestBlocking(DiscoveryNode masterNode, DiscoveryNode node, TimeValue timeout) throws ElasticSearchException {
        return transportService.submitRequest(masterNode, JoinRequestRequestHandler.ACTION, new JoinRequest(node, true), new FutureTransportResponseHandler<JoinResponse>() {
            @Override
            public JoinResponse newInstance() {
                return new JoinResponse();
            }
        }).txGet(timeout.millis(), TimeUnit.MILLISECONDS).clusterState;
    }

    static class JoinRequest implements Streamable {

        DiscoveryNode node;

        boolean withClusterState;

        private JoinRequest() {
        }

        private JoinRequest(DiscoveryNode node, boolean withClusterState) {
            this.node = node;
            this.withClusterState = withClusterState;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            node = DiscoveryNode.readNode(in);
            withClusterState = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            node.writeTo(out);
            out.writeBoolean(withClusterState);
        }
    }

    class JoinResponse implements Streamable {

        ClusterState clusterState;

        JoinResponse() {
        }

        JoinResponse(ClusterState clusterState) {
            this.clusterState = clusterState;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            clusterState = ClusterState.Builder.readFrom(in, nodesProvider.nodes().localNode());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            ClusterState.Builder.writeTo(clusterState, out);
        }
    }

    private class JoinRequestRequestHandler extends BaseTransportRequestHandler<JoinRequest> {

        static final String ACTION = "discovery/zen/join";

        @Override
        public JoinRequest newInstance() {
            return new JoinRequest();
        }

        @Override
        public void messageReceived(JoinRequest request, TransportChannel channel) throws Exception {
            ClusterState clusterState = listener.onJoin(request.node);
            if (request.withClusterState) {
                channel.sendResponse(new JoinResponse(clusterState));
            } else {
                channel.sendResponse(VoidStreamable.INSTANCE);
            }
        }

        @Override
        public String executor() {
            return ThreadPool.Names.GENERIC;
        }
    }

    private static class LeaveRequest implements Streamable {

        private DiscoveryNode node;

        private LeaveRequest() {
        }

        private LeaveRequest(DiscoveryNode node) {
            this.node = node;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            node = DiscoveryNode.readNode(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            node.writeTo(out);
        }
    }

    private class LeaveRequestRequestHandler extends BaseTransportRequestHandler<LeaveRequest> {

        static final String ACTION = "discovery/zen/leave";

        @Override
        public LeaveRequest newInstance() {
            return new LeaveRequest();
        }

        @Override
        public void messageReceived(LeaveRequest request, TransportChannel channel) throws Exception {
            listener.onLeave(request.node);
            channel.sendResponse(VoidStreamable.INSTANCE);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }
}

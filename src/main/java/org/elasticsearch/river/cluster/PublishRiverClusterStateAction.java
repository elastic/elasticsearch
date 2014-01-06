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

package org.elasticsearch.river.cluster;

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;

/**
 *
 */
public class PublishRiverClusterStateAction extends AbstractComponent {

    public static interface NewClusterStateListener {
        void onNewClusterState(RiverClusterState clusterState);
    }

    private final TransportService transportService;

    private final ClusterService clusterService;

    private final NewClusterStateListener listener;

    public PublishRiverClusterStateAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                          NewClusterStateListener listener) {
        super(settings);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.listener = listener;
        transportService.registerHandler(PublishClusterStateRequestHandler.ACTION, new PublishClusterStateRequestHandler());
    }

    public void close() {
        transportService.removeHandler(PublishClusterStateRequestHandler.ACTION);
    }

    public void publish(RiverClusterState clusterState) {
        final DiscoveryNodes discoNodes = clusterService.state().nodes();
        final DiscoveryNode localNode = discoNodes.localNode();
        for (final DiscoveryNode node : discoNodes) {
            if (node.equals(localNode)) {
                // no need to send to our self
                continue;
            }

            // we only want to send nodes that are either possible master nodes or river nodes
            // master nodes because they will handle the state and the allocation of rivers
            // and river nodes since they will end up creating indexes

            if (!node.masterNode() && !RiverNodeHelper.isRiverNode(node)) {
                continue;
            }

            transportService.sendRequest(node, PublishClusterStateRequestHandler.ACTION, new PublishClusterStateRequest(clusterState), new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                @Override
                public void handleException(TransportException exp) {
                    logger.debug("failed to send cluster state to [{}], should be detected as failed soon...", exp, node);
                }
            });
        }
    }

    private class PublishClusterStateRequest extends TransportRequest {

        private RiverClusterState clusterState;

        private PublishClusterStateRequest() {
        }

        private PublishClusterStateRequest(RiverClusterState clusterState) {
            this.clusterState = clusterState;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            clusterState = RiverClusterState.Builder.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            RiverClusterState.Builder.writeTo(clusterState, out);
        }
    }

    private class PublishClusterStateRequestHandler extends BaseTransportRequestHandler<PublishClusterStateRequest> {

        static final String ACTION = "river/state/publish";

        @Override
        public PublishClusterStateRequest newInstance() {
            return new PublishClusterStateRequest();
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        public void messageReceived(PublishClusterStateRequest request, TransportChannel channel) throws Exception {
            listener.onNewClusterState(request.clusterState);
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }
}

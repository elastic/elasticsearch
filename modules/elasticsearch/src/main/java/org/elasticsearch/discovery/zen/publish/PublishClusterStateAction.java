/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.discovery.zen.publish;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.io.stream.VoidStreamable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zen.DiscoveryNodesProvider;
import org.elasticsearch.transport.*;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class PublishClusterStateAction extends AbstractComponent {

    public static interface NewClusterStateListener {
        void onNewClusterState(ClusterState clusterState);
    }

    private final TransportService transportService;

    private final DiscoveryNodesProvider nodesProvider;

    private final NewClusterStateListener listener;

    public PublishClusterStateAction(Settings settings, TransportService transportService, DiscoveryNodesProvider nodesProvider,
                                     NewClusterStateListener listener) {
        super(settings);
        this.transportService = transportService;
        this.nodesProvider = nodesProvider;
        this.listener = listener;
        transportService.registerHandler(PublishClusterStateRequestHandler.ACTION, new PublishClusterStateRequestHandler());
    }

    public void close() {
        transportService.removeHandler(PublishClusterStateRequestHandler.ACTION);
    }

    public void publish(ClusterState clusterState) {
        DiscoveryNode localNode = nodesProvider.nodes().localNode();
        for (final DiscoveryNode node : clusterState.nodes()) {
            if (node.equals(localNode)) {
                // no need to send to our self
                continue;
            }
            transportService.sendRequest(node, PublishClusterStateRequestHandler.ACTION, new PublishClusterStateRequest(clusterState), new VoidTransportResponseHandler(false) {
                @Override public void handleException(TransportException exp) {
                    logger.debug("failed to send cluster state to [{}], should be detected as failed soon...", exp, node);
                }
            });
        }
    }

    private class PublishClusterStateRequest implements Streamable {

        private ClusterState clusterState;

        private PublishClusterStateRequest() {
        }

        private PublishClusterStateRequest(ClusterState clusterState) {
            this.clusterState = clusterState;
        }

        @Override public void readFrom(StreamInput in) throws IOException {
            clusterState = ClusterState.Builder.readFrom(in, nodesProvider.nodes().localNode());
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            ClusterState.Builder.writeTo(clusterState, out);
        }
    }

    private class PublishClusterStateRequestHandler extends BaseTransportRequestHandler<PublishClusterStateRequest> {

        static final String ACTION = "discovery/zen/publish";

        @Override public PublishClusterStateRequest newInstance() {
            return new PublishClusterStateRequest();
        }

        @Override public void messageReceived(PublishClusterStateRequest request, TransportChannel channel) throws Exception {
            listener.onNewClusterState(request.clusterState);
            channel.sendResponse(VoidStreamable.INSTANCE);
        }

        /**
         * No need to spawn, we add submit a new cluster state directly. This allows for faster application.
         */
        @Override public boolean spawn() {
            return false;
        }
    }
}

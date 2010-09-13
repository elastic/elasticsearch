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

package org.elasticsearch.indexer.cluster;

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.io.stream.VoidStreamable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.*;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class PublishIndexerClusterStateAction extends AbstractComponent {

    public static interface NewClusterStateListener {
        void onNewClusterState(IndexerClusterState clusterState);
    }

    private final TransportService transportService;

    private final ClusterService clusterService;

    private final NewClusterStateListener listener;

    public PublishIndexerClusterStateAction(Settings settings, TransportService transportService, ClusterService clusterService,
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

    public void publish(IndexerClusterState clusterState) {
        final DiscoveryNodes discoNodes = clusterService.state().nodes();
        for (final DiscoveryNode node : discoNodes) {
            if (node.equals(discoNodes.localNode())) {
                // no need to send to our self
                continue;
            }

            // we only want to send nodes that are either possible master nodes or indexer nodes
            // master nodes because they will handle the state and the allocation of indexers
            // and indexer nodes since they will end up creating indexes

            if (node.clientNode()) {
                continue;
            }

            if (!node.masterNode() && !IndexerNodeHelper.isIndexerNode(node)) {
                continue;
            }

            transportService.sendRequest(node, PublishClusterStateRequestHandler.ACTION, new PublishClusterStateRequest(clusterState), new VoidTransportResponseHandler(false) {
                @Override public void handleException(RemoteTransportException exp) {
                    logger.debug("failed to send cluster state to [{}], should be detected as failed soon...", exp, node);
                }
            });
        }
    }

    private class PublishClusterStateRequest implements Streamable {

        private IndexerClusterState clusterState;

        private PublishClusterStateRequest() {
        }

        private PublishClusterStateRequest(IndexerClusterState clusterState) {
            this.clusterState = clusterState;
        }

        @Override public void readFrom(StreamInput in) throws IOException {
            clusterState = IndexerClusterState.Builder.readFrom(in, settings);
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            IndexerClusterState.Builder.writeTo(clusterState, out);
        }
    }

    private class PublishClusterStateRequestHandler extends BaseTransportRequestHandler<PublishClusterStateRequest> {

        static final String ACTION = "indexer/state/publish";

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

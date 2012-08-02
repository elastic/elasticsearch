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

package org.elasticsearch.discovery.zen.publish;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.*;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zen.DiscoveryNodesProvider;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;

/**
 *
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

        // serialize the cluster state here, so we won't do it several times per node
        CachedStreamOutput.Entry cachedEntry = CachedStreamOutput.popEntry();
        byte[] clusterStateInBytes;
        try {
            StreamOutput stream = cachedEntry.handles(CompressorFactory.defaultCompressor());
            ClusterState.Builder.writeTo(clusterState, stream);
            stream.close();
            clusterStateInBytes = cachedEntry.bytes().bytes().copyBytesArray().toBytes();
        } catch (Exception e) {
            logger.warn("failed to serialize cluster_state before publishing it to nodes", e);
            return;
        } finally {
            CachedStreamOutput.pushEntry(cachedEntry);
        }

        for (final DiscoveryNode node : clusterState.nodes()) {
            if (node.equals(localNode)) {
                // no need to send to our self
                continue;
            }
            transportService.sendRequest(node, PublishClusterStateRequestHandler.ACTION,
                    new PublishClusterStateRequest(clusterStateInBytes),
                    TransportRequestOptions.options().withHighType().withCompress(false), // no need to compress, we already compressed the bytes

                    new VoidTransportResponseHandler(ThreadPool.Names.SAME) {
                        @Override
                        public void handleException(TransportException exp) {
                            logger.debug("failed to send cluster state to [{}], should be detected as failed soon...", exp, node);
                        }
                    });
        }
    }

    class PublishClusterStateRequest implements Streamable {

        BytesReference clusterStateInBytes;

        private PublishClusterStateRequest() {
        }

        private PublishClusterStateRequest(byte[] clusterStateInBytes) {
            this.clusterStateInBytes = new BytesArray(clusterStateInBytes);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            clusterStateInBytes = in.readBytesReference();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBytesReference(clusterStateInBytes);
        }
    }

    private class PublishClusterStateRequestHandler extends BaseTransportRequestHandler<PublishClusterStateRequest> {

        static final String ACTION = "discovery/zen/publish";

        @Override
        public PublishClusterStateRequest newInstance() {
            return new PublishClusterStateRequest();
        }

        @Override
        public void messageReceived(PublishClusterStateRequest request, TransportChannel channel) throws Exception {
            Compressor compressor = CompressorFactory.compressor(request.clusterStateInBytes);
            StreamInput in;
            if (compressor != null) {
                in = CachedStreamInput.cachedHandlesCompressed(compressor, request.clusterStateInBytes.streamInput());
            } else {
                in = CachedStreamInput.cachedHandles(request.clusterStateInBytes.streamInput());
            }
            ClusterState clusterState = ClusterState.Builder.readFrom(in, nodesProvider.nodes().localNode());
            listener.onNewClusterState(clusterState);
            channel.sendResponse(VoidStreamable.INSTANCE);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }
}

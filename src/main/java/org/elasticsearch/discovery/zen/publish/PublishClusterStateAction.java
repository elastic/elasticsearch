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

import com.google.common.collect.Maps;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
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
import java.util.Map;

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

        Map<Version, BytesReference> serializedStates = Maps.newHashMap();
        for (final DiscoveryNode node : clusterState.nodes()) {
            if (node.equals(localNode)) {
                // no need to send to our self
                continue;
            }
            // try and serialize the cluster state once (or per version), so we don't serialize it
            // per node when we send it over the wire, compress it while we are at it...
            BytesReference bytes = serializedStates.get(node.version());
            if (bytes == null) {
                try {
                    BytesStreamOutput bStream = new BytesStreamOutput();
                    StreamOutput stream = new HandlesStreamOutput(CompressorFactory.defaultCompressor().streamOutput(bStream));
                    stream.setVersion(node.version());
                    ClusterState.Builder.writeTo(clusterState, stream);
                    stream.close();
                    bytes = bStream.bytes();
                    serializedStates.put(node.version(), bytes);
                } catch (Exception e) {
                    logger.warn("failed to serialize cluster_state before publishing it to nodes", e);
                    return;
                }
            }
            transportService.sendRequest(node, PublishClusterStateRequestHandler.ACTION,
                    new PublishClusterStateRequest(bytes, node.version()),
                    TransportRequestOptions.options().withHighType().withCompress(false), // no need to compress, we already compressed the bytes

                    new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                        @Override
                        public void handleException(TransportException exp) {
                            logger.debug("failed to send cluster state to [{}], should be detected as failed soon...", exp, node);
                        }
                    });
        }
    }

    class PublishClusterStateRequest extends TransportRequest {

        BytesReference clusterStateInBytes;
        Version version;

        PublishClusterStateRequest() {
        }

        PublishClusterStateRequest(BytesReference clusterStateInBytes, Version version) {
            this.clusterStateInBytes = clusterStateInBytes;
            this.version = version;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            clusterStateInBytes = in.readBytesReference();
            version = in.getVersion();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
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
            in.setVersion(request.version);
            ClusterState clusterState = ClusterState.Builder.readFrom(in, nodesProvider.nodes().localNode());
            listener.onNewClusterState(clusterState);
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }
}

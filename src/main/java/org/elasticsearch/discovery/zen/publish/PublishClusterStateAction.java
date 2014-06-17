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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.AckClusterStatePublishResponseHandler;
import org.elasticsearch.discovery.ClusterStatePublishResponseHandler;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.zen.DiscoveryNodesProvider;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.util.Map;

/**
 *
 */
public class PublishClusterStateAction extends AbstractComponent {

    public static interface NewClusterStateListener {

        static interface NewStateProcessed {

            void onNewClusterStateProcessed();

            void onNewClusterStateFailed(Throwable t);
        }

        void onNewClusterState(ClusterState clusterState, NewStateProcessed newStateProcessed);
    }

    private final TransportService transportService;
    private final DiscoveryNodesProvider nodesProvider;
    private final NewClusterStateListener listener;
    private final DiscoverySettings discoverySettings;

    public PublishClusterStateAction(Settings settings, TransportService transportService, DiscoveryNodesProvider nodesProvider,
                                     NewClusterStateListener listener, DiscoverySettings discoverySettings) {
        super(settings);
        this.transportService = transportService;
        this.nodesProvider = nodesProvider;
        this.listener = listener;
        this.discoverySettings = discoverySettings;
        transportService.registerHandler(PublishClusterStateRequestHandler.ACTION, new PublishClusterStateRequestHandler());
    }

    public void close() {
        transportService.removeHandler(PublishClusterStateRequestHandler.ACTION);
    }

    public void publish(ClusterState clusterState, final Discovery.AckListener ackListener) {
        publish(clusterState, new AckClusterStatePublishResponseHandler(clusterState.nodes().size() - 1, ackListener));
    }

    private void publish(ClusterState clusterState, final ClusterStatePublishResponseHandler publishResponseHandler) {

        DiscoveryNode localNode = nodesProvider.nodes().localNode();

        Map<Version, BytesReference> serializedStates = Maps.newHashMap();

        for (final DiscoveryNode node : clusterState.nodes()) {
            if (node.equals(localNode)) {
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
                } catch (Throwable e) {
                    logger.warn("failed to serialize cluster_state before publishing it to node {}", e, node);
                    publishResponseHandler.onFailure(node, e);
                    continue;
                }
            }
            try {
                TransportRequestOptions options = TransportRequestOptions.options().withType(TransportRequestOptions.Type.STATE).withCompress(false);
                // no need to put a timeout on the options here, because we want the response to eventually be received
                // and not log an error if it arrives after the timeout
                transportService.sendRequest(node, PublishClusterStateRequestHandler.ACTION,
                        new BytesTransportRequest(bytes, node.version()),
                        options, // no need to compress, we already compressed the bytes

                        new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {

                            @Override
                            public void handleResponse(TransportResponse.Empty response) {
                                publishResponseHandler.onResponse(node);
                            }

                            @Override
                            public void handleException(TransportException exp) {
                                logger.debug("failed to send cluster state to [{}]", exp, node);
                                publishResponseHandler.onFailure(node, exp);
                            }
                        });
            } catch (Throwable t) {
                logger.debug("error sending cluster state to [{}]", t, node);
                publishResponseHandler.onFailure(node, t);
            }
        }

        TimeValue publishTimeout = discoverySettings.getPublishTimeout();
        if (publishTimeout.millis() > 0) {
            // only wait if the publish timeout is configured...
            try {
                boolean awaited = publishResponseHandler.awaitAllNodes(publishTimeout);
                if (!awaited) {
                    logger.debug("awaiting all nodes to process published state {} timed out, timeout {}", clusterState.version(), publishTimeout);
                }
            } catch (InterruptedException e) {
                // ignore & restore interrupt
                Thread.currentThread().interrupt();
            }
        }
    }

    private class PublishClusterStateRequestHandler extends BaseTransportRequestHandler<BytesTransportRequest> {

        static final String ACTION = "discovery/zen/publish";

        @Override
        public BytesTransportRequest newInstance() {
            return new BytesTransportRequest();
        }

        @Override
        public void messageReceived(BytesTransportRequest request, final TransportChannel channel) throws Exception {
            Compressor compressor = CompressorFactory.compressor(request.bytes());
            StreamInput in;
            if (compressor != null) {
                in = CachedStreamInput.cachedHandlesCompressed(compressor, request.bytes().streamInput());
            } else {
                in = CachedStreamInput.cachedHandles(request.bytes().streamInput());
            }
            in.setVersion(request.version());
            ClusterState clusterState = ClusterState.Builder.readFrom(in, nodesProvider.nodes().localNode());
            clusterState.status(ClusterState.ClusterStateStatus.RECEIVED);
            logger.debug("received cluster state version {}", clusterState.version());
            listener.onNewClusterState(clusterState, new NewClusterStateListener.NewStateProcessed() {
                @Override
                public void onNewClusterStateProcessed() {
                    try {
                        channel.sendResponse(TransportResponse.Empty.INSTANCE);
                    } catch (Throwable e) {
                        logger.debug("failed to send response on cluster state processed", e);
                    }
                }

                @Override
                public void onNewClusterStateFailed(Throwable t) {
                    try {
                        channel.sendResponse(t);
                    } catch (Throwable e) {
                        logger.debug("failed to send response on cluster state processed", e);
                    }
                }
            });
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }
}

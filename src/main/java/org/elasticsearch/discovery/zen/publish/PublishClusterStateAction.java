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
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.AckClusterStatePublishResponseHandler;
import org.elasticsearch.discovery.BlockingClusterStatePublishResponseHandler;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.zen.DiscoveryNodesProvider;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class PublishClusterStateAction extends AbstractComponent {

    public static final String ACTION_NAME = "internal:discovery/zen/publish";

    public interface NewClusterStateListener {

        interface NewStateProcessed {

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
        transportService.registerRequestHandler(ACTION_NAME, BytesTransportRequest.class, ThreadPool.Names.SAME, new PublishClusterStateRequestHandler());
    }

    public void close() {
        transportService.removeHandler(ACTION_NAME);
    }

    public void publish(ClusterState clusterState, final Discovery.AckListener ackListener) {
        Set<DiscoveryNode> nodesToPublishTo = new HashSet<>(clusterState.nodes().size());
        DiscoveryNode localNode = nodesProvider.nodes().localNode();
        for (final DiscoveryNode node : clusterState.nodes()) {
            if (node.equals(localNode)) {
                continue;
            }
            nodesToPublishTo.add(node);
        }
        publish(clusterState, nodesToPublishTo, new AckClusterStatePublishResponseHandler(nodesToPublishTo, ackListener));
    }

    private void publish(final ClusterState clusterState, final Set<DiscoveryNode> nodesToPublishTo,
                         final BlockingClusterStatePublishResponseHandler publishResponseHandler) {

        Map<Version, BytesReference> serializedStates = Maps.newHashMap();

        final AtomicBoolean timedOutWaitingForNodes = new AtomicBoolean(false);
        final TimeValue publishTimeout = discoverySettings.getPublishTimeout();

        for (final DiscoveryNode node : nodesToPublishTo) {

            // try and serialize the cluster state once (or per version), so we don't serialize it
            // per node when we send it over the wire, compress it while we are at it...
            BytesReference bytes = serializedStates.get(node.version());
            if (bytes == null) {
                try {
                    BytesStreamOutput bStream = new BytesStreamOutput();
                    StreamOutput stream = CompressorFactory.defaultCompressor().streamOutput(bStream);
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
                transportService.sendRequest(node, ACTION_NAME,
                        new BytesTransportRequest(bytes, node.version()),
                        options, // no need to compress, we already compressed the bytes

                        new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {

                            @Override
                            public void handleResponse(TransportResponse.Empty response) {
                                if (timedOutWaitingForNodes.get()) {
                                    logger.debug("node {} responded for cluster state [{}] (took longer than [{}])", node, clusterState.version(), publishTimeout);
                                }
                                publishResponseHandler.onResponse(node);
                            }

                            @Override
                            public void handleException(TransportException exp) {
                                logger.debug("failed to send cluster state to {}", exp, node);
                                publishResponseHandler.onFailure(node, exp);
                            }
                        });
            } catch (Throwable t) {
                logger.debug("error sending cluster state to {}", t, node);
                publishResponseHandler.onFailure(node, t);
            }
        }

        if (publishTimeout.millis() > 0) {
            // only wait if the publish timeout is configured...
            try {
                timedOutWaitingForNodes.set(!publishResponseHandler.awaitAllNodes(publishTimeout));
                if (timedOutWaitingForNodes.get()) {
                    DiscoveryNode[] pendingNodes = publishResponseHandler.pendingNodes();
                    // everyone may have just responded
                    if (pendingNodes.length > 0) {
                        logger.warn("timed out waiting for all nodes to process published state [{}] (timeout [{}], pending nodes: {})", clusterState.version(), publishTimeout, pendingNodes);
                    }
                }
            } catch (InterruptedException e) {
                // ignore & restore interrupt
                Thread.currentThread().interrupt();
            }
        }
    }

    private class PublishClusterStateRequestHandler implements TransportRequestHandler<BytesTransportRequest> {

        @Override
        public void messageReceived(BytesTransportRequest request, final TransportChannel channel) throws Exception {
            Compressor compressor = CompressorFactory.compressor(request.bytes());
            StreamInput in;
            if (compressor != null) {
                in = compressor.streamInput(request.bytes().streamInput());
            } else {
                in = request.bytes().streamInput();
            }
            in.setVersion(request.version());
            ClusterState clusterState = ClusterState.Builder.readFrom(in, nodesProvider.nodes().localNode());
            clusterState.status(ClusterState.ClusterStateStatus.RECEIVED);
            logger.debug("received cluster state version {}", clusterState.version());
            try {
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
            } catch (Exception e) {
                logger.warn("unexpected error while processing cluster state version [{}]", e, clusterState.version());
                try {
                    channel.sendResponse(e);
                } catch (Throwable e1) {
                    logger.debug("failed to send response on cluster state processed", e1);
                }
            }
        }
    }
}

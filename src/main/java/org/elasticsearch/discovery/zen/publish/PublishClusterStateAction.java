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
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterState.ClusterStateDiff;
import org.elasticsearch.cluster.IncompatibleClusterStateVersionException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.CopyOnWriteHashMap;
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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.collect.Maps.newHashMap;

/**
 *
 */
public class PublishClusterStateAction extends AbstractComponent {

    public static final String ACTION_NAME = "internal:discovery/zen/publish";

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
    private final ClusterName clusterName;
    private final Map<String, Long> lastFullVersionSent = newHashMap();
    private final Lock versionMapLock = new ReentrantLock();
    private volatile ClusterState lastProcessedClusterState;

    public PublishClusterStateAction(Settings settings, TransportService transportService, DiscoveryNodesProvider nodesProvider,
                                     NewClusterStateListener listener, DiscoverySettings discoverySettings, ClusterName clusterName) {
        super(settings);
        this.transportService = transportService;
        this.nodesProvider = nodesProvider;
        this.listener = listener;
        this.discoverySettings = discoverySettings;
        this.clusterName = clusterName;
        transportService.registerHandler(ACTION_NAME, new PublishClusterStateRequestHandler());
        lastProcessedClusterState = null;
    }

    public void close() {
        transportService.removeHandler(ACTION_NAME);
    }

    public void publish(ClusterState clusterState, final Discovery.AckListener ackListener) {
        publish(clusterState, new AckClusterStatePublishResponseHandler(clusterState.nodes().size() - 1, ackListener));
    }

    private void publish(final ClusterState clusterState, final ClusterStatePublishResponseHandler publishResponseHandler) {

        DiscoveryNode localNode = nodesProvider.nodes().localNode();

        Map<Version, BytesReference> serializedStates = newHashMap();
        Map<Version, BytesReference> serializedDiffs = newHashMap();

        final AtomicBoolean timedOutWaitingForNodes = new AtomicBoolean(false);
        final TimeValue publishTimeout = discoverySettings.getPublishTimeout();
        final boolean sendFullVersion = lastProcessedClusterState == null || clusterState.version() - lastProcessedClusterState.version() != 1;

        // Remove nodes that are no longer part of the cluster
        versionMapLock.lock();
        try {
            Iterator<String> iterator = lastFullVersionSent.keySet().iterator();
            while (iterator.hasNext()) {
                if (!clusterState.nodes().nodeExists(iterator.next())) {
                    iterator.remove();
                }
            }
        } finally {
            versionMapLock.unlock();
        }

        for (final DiscoveryNode node : clusterState.nodes()) {
            if (node.equals(localNode)) {
                continue;
            }
            // try and serialize the cluster state once (or per version), so we don't serialize it
            // per node when we send it over the wire, compress it while we are at it...
            BytesReference bytes;
            boolean newlyAddedNode = !lastProcessedClusterState.nodes().nodeExists(node.id());
            if (sendFullVersion || newlyAddedNode) {
                versionMapLock.lock();
                try {
                    lastFullVersionSent.put(node.getId(), clusterState.version());
                } finally {
                    versionMapLock.unlock();
                }
                bytes = serializedStates.get(node.version());
                if (bytes == null) {
                    try {
                        bytes = serializeFullClusterState(clusterState, node.version());
                        serializedStates.put(node.version(), bytes);
                    } catch (Throwable e) {
                        logger.warn("failed to serialize cluster_state before publishing it to node {}", e, node);
                        publishResponseHandler.onFailure(node, e);
                        continue;
                    }
                }
            } else {
                bytes = serializedDiffs.get(node.version());
                if (bytes == null) {
                    try {
                        bytes = serializeDiffClusterState(clusterState, node.version());
                        serializedDiffs.put(node.version(), bytes);
                    } catch (Throwable e) {
                        logger.warn("failed to serialize cluster_state before publishing it to node {}", e, node);
                        publishResponseHandler.onFailure(node, e);
                        continue;
                    }
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
                                if (exp.unwrapCause() instanceof IncompatibleClusterStateVersionException) {
                                    logger.debug("resending full cluster state to node {} reason {}", node, exp.getDetailedMessage());
                                    resendFullClusterState(clusterState, node, timedOutWaitingForNodes, publishResponseHandler);
                                } else {
                                    logger.debug("failed to send cluster state to {}", exp, node);
                                    publishResponseHandler.onFailure(node, exp);
                                }
                            }
                        });
            } catch (Throwable t) {
                logger.debug("error sending cluster state to {}", t, node);
                publishResponseHandler.onFailure(node, t);
            }
        }
        lastProcessedClusterState = clusterState;
        if (publishTimeout.millis() > 0) {
            // only wait if the publish timeout is configured...
            try {
                timedOutWaitingForNodes.set(!publishResponseHandler.awaitAllNodes(publishTimeout));
                if (timedOutWaitingForNodes.get()) {
                    logger.debug("timed out waiting for all nodes to process published state [{}] (timeout [{}])", clusterState.version(), publishTimeout);
                }
            } catch (InterruptedException e) {
                // ignore & restore interrupt
                Thread.currentThread().interrupt();
            }
        }
    }

    private void resendFullClusterState(final ClusterState clusterState, final DiscoveryNode node, final AtomicBoolean timedOutWaitingForNodes, final ClusterStatePublishResponseHandler publishResponseHandler) {
        final TimeValue publishTimeout = discoverySettings.getPublishTimeout();
        final ClusterState currentClusterState = lastProcessedClusterState;
        final ClusterState clusterStateToSend;
        if (currentClusterState == null || currentClusterState.version() < clusterState.version()) {
            clusterStateToSend = clusterState;
        } else {
            clusterStateToSend = currentClusterState;
        }
        boolean shouldSendAnUpdate = true;
        final Long lastVersionSent;
        versionMapLock.lock();
        try {
            lastVersionSent = lastFullVersionSent.get(node.getId());
            if (lastVersionSent != null && clusterStateToSend.version() <= lastVersionSent) {
                //we already sent a newer full version of cluster state to this node - no reason to send it again
                shouldSendAnUpdate = false;
            } else {
                lastFullVersionSent.put(node.getId(), clusterStateToSend.version());
            }
        } finally {
            versionMapLock.unlock();
        }

        if (shouldSendAnUpdate) {

            BytesReference bytes;
            try {
                bytes = serializeFullClusterState(clusterStateToSend, node.version());
            } catch (Throwable e) {
                logger.warn("failed to serialize cluster_state before publishing it to node {}", e, node);
                publishResponseHandler.onFailure(node, e);
                return;
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
        } else {
            logger.debug("skipping sending version {} to node {}, node already received version {}", clusterStateToSend.version(), node, lastVersionSent);
            // TODO: Should this be a failure since we are not really sure if this node got the cluster state or not at this moment
            publishResponseHandler.onResponse(node);
        }
    }

    private BytesReference serializeFullClusterState(ClusterState clusterState, Version nodeVersion) throws IOException {
        BytesStreamOutput bStream = new BytesStreamOutput();
        StreamOutput stream = new HandlesStreamOutput(CompressorFactory.defaultCompressor().streamOutput(bStream));
        stream.setVersion(nodeVersion);
        stream.writeBoolean(true);
        ClusterState.Builder.writeTo(clusterState, stream);
        stream.close();
        return bStream.bytes();
    }

    private BytesReference serializeDiffClusterState(ClusterState clusterState, Version nodeVersion) throws IOException {
        BytesStreamOutput bStream = new BytesStreamOutput();
        StreamOutput stream = new HandlesStreamOutput(CompressorFactory.defaultCompressor().streamOutput(bStream));
        stream.setVersion(nodeVersion);
        stream.writeBoolean(false);
        ClusterStateDiff diff = ClusterState.Builder.diff(lastProcessedClusterState, clusterState);
        ClusterState.Builder.writeDiffTo(diff, stream);
        stream.close();
        return bStream.bytes();
    }

    private class PublishClusterStateRequestHandler extends BaseTransportRequestHandler<BytesTransportRequest> {

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
            final ClusterState clusterState;
            if (in.readBoolean()) {
                clusterState = ClusterState.Builder.readFrom(in, nodesProvider.nodes().localNode());
                logger.debug("received full cluster state version {} with size {}", clusterState.version(), request.bytes().length());
            } else {
                if(lastProcessedClusterState == null) {
                    logger.debug("received diff cluster state version {} but don't have any local cluster state - requesting full state");
                    throw new IncompatibleClusterStateVersionException("have no local cluster state");
                }
                ClusterState.ClusterStateDiff diff = ClusterState.Builder.readDiffFrom(in, nodesProvider.nodes().localNode());
                if (lastProcessedClusterState.version() >= diff.version()) {
                    logger.debug("got diffs for obsolete version {}, current version {}, ignoring the diff", diff.version(), lastProcessedClusterState.version());
                    return;
                }
                clusterState = diff.apply(lastProcessedClusterState);
                logger.debug("received diff cluster state version {} with size {}", clusterState.version(), request.bytes().length());
            }
            lastProcessedClusterState = clusterState;

            clusterState.status(ClusterState.ClusterStateStatus.RECEIVED);
//            logger.debug("received cluster state version {}", clusterState.version());
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

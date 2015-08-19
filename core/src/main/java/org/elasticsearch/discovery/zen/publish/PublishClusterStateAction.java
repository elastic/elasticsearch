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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
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
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class PublishClusterStateAction extends AbstractComponent {

    public static final String SEND_ACTION_NAME = "internal:discovery/zen/publish/send";
    public static final String COMMIT_ACTION_NAME = "internal:discovery/zen/publish/commit";

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
    private final ClusterName clusterName;

    public PublishClusterStateAction(Settings settings, TransportService transportService, DiscoveryNodesProvider nodesProvider,
                                     NewClusterStateListener listener, DiscoverySettings discoverySettings, ClusterName clusterName) {
        super(settings);
        this.transportService = transportService;
        this.nodesProvider = nodesProvider;
        this.listener = listener;
        this.discoverySettings = discoverySettings;
        this.clusterName = clusterName;
        transportService.registerRequestHandler(SEND_ACTION_NAME, BytesTransportRequest.class, ThreadPool.Names.SAME, new SendClusterStateRequestHandler());
        transportService.registerRequestHandler(COMMIT_ACTION_NAME, CommitClusterStateRequest.class, ThreadPool.Names.SAME, new CommitClusterStateRequestHandler());
    }

    public void close() {
        transportService.removeHandler(SEND_ACTION_NAME);
        transportService.removeHandler(COMMIT_ACTION_NAME);
    }

    public void publish(ClusterChangedEvent clusterChangedEvent, int minMasterNodes, final Discovery.AckListener ackListener) {
        Set<DiscoveryNode> nodesToPublishTo = new HashSet<>(clusterChangedEvent.state().nodes().size());
        DiscoveryNode localNode = nodesProvider.nodes().localNode();
        int totalMasterNodes = 0;
        for (final DiscoveryNode node : clusterChangedEvent.state().nodes()) {
            if (node.isMasterNode()) {
                totalMasterNodes++;
            }
            if (node.equals(localNode) == false) {
                nodesToPublishTo.add(node);
            }
        }
        publish(clusterChangedEvent, minMasterNodes, totalMasterNodes, nodesToPublishTo, new AckClusterStatePublishResponseHandler(nodesToPublishTo, ackListener));
    }

    private void publish(final ClusterChangedEvent clusterChangedEvent, int minMasterNodes, int totalMasterNodes, final Set<DiscoveryNode> nodesToPublishTo,
                         final BlockingClusterStatePublishResponseHandler publishResponseHandler) {

        Map<Version, BytesReference> serializedStates = Maps.newHashMap();
        Map<Version, BytesReference> serializedDiffs = Maps.newHashMap();

        final ClusterState clusterState = clusterChangedEvent.state();
        final ClusterState previousState = clusterChangedEvent.previousState();
        final AtomicBoolean timedOutWaitingForNodes = new AtomicBoolean(false);
        final TimeValue publishTimeout = discoverySettings.getPublishTimeout();
        final boolean sendFullVersion = !discoverySettings.getPublishDiff() || previousState == null;
        final SendingController sendingController = new SendingController(clusterChangedEvent.state(), minMasterNodes, totalMasterNodes, publishResponseHandler);
        Diff<ClusterState> diff = null;

        for (final DiscoveryNode node : nodesToPublishTo) {

            // try and serialize the cluster state once (or per version), so we don't serialize it
            // per node when we send it over the wire, compress it while we are at it...
            // we don't send full version if node didn't exist in the previous version of cluster state
            if (sendFullVersion || !previousState.nodes().nodeExists(node.id())) {
                sendFullClusterState(clusterState, serializedStates, node, timedOutWaitingForNodes, publishTimeout, sendingController);
            } else {
                if (diff == null) {
                    diff = clusterState.diff(previousState);
                }
                sendClusterStateDiff(clusterState, diff, serializedDiffs, node, timedOutWaitingForNodes, publishTimeout, sendingController);
            }
        }

        sendingController.waitForCommit(discoverySettings.getCommitTimeout());

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

    private void sendFullClusterState(ClusterState clusterState, @Nullable Map<Version, BytesReference> serializedStates,
                                      DiscoveryNode node, AtomicBoolean timedOutWaitingForNodes, TimeValue publishTimeout,
                                      SendingController sendingController) {
        BytesReference bytes = null;
        if (serializedStates != null) {
            bytes = serializedStates.get(node.version());
        }
        if (bytes == null) {
            try {
                bytes = serializeFullClusterState(clusterState, node.version());
                if (serializedStates != null) {
                    serializedStates.put(node.version(), bytes);
                }
            } catch (Throwable e) {
                logger.warn("failed to serialize cluster_state before publishing it to node {}", e, node);
                sendingController.onNodeSendFailed(node, e);
                return;
            }
        }
        sendClusterStateToNode(clusterState, bytes, node, timedOutWaitingForNodes, publishTimeout, sendingController, false);
    }

    private void sendClusterStateDiff(ClusterState clusterState, Diff diff, Map<Version, BytesReference> serializedDiffs, DiscoveryNode node,
                                      AtomicBoolean timedOutWaitingForNodes, TimeValue publishTimeout,
                                      SendingController sendingController) {
        BytesReference bytes = serializedDiffs.get(node.version());
        if (bytes == null) {
            try {
                bytes = serializeDiffClusterState(diff, node.version());
                serializedDiffs.put(node.version(), bytes);
            } catch (Throwable e) {
                logger.warn("failed to serialize diff of cluster_state before publishing it to node {}", e, node);
                sendingController.onNodeSendFailed(node, e);
                return;
            }
        }
        sendClusterStateToNode(clusterState, bytes, node, timedOutWaitingForNodes, publishTimeout, sendingController, true);
    }

    private void sendClusterStateToNode(final ClusterState clusterState, BytesReference bytes,
                                        final DiscoveryNode node, final AtomicBoolean timedOutWaitingForNodes,
                                        final TimeValue publishTimeout,
                                        final SendingController sendingController,
                                        final boolean sendDiffs) {
        try {
            TransportRequestOptions options = TransportRequestOptions.options().withType(TransportRequestOptions.Type.STATE).withCompress(false);
            // no need to put a timeout on the options here, because we want the response to eventually be received
            // and not log an error if it arrives after the timeout
            transportService.sendRequest(node, SEND_ACTION_NAME,
                    new BytesTransportRequest(bytes, node.version()),
                    options, // no need to compress, we already compressed the bytes

                    new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {

                        @Override
                        public void handleResponse(TransportResponse.Empty response) {
                            if (timedOutWaitingForNodes.get()) {
                                logger.debug("node {} responded for cluster state [{}] (took longer than [{}])", node, clusterState.version(), publishTimeout);
                            }
                            sendingController.onNodeSendAck(node);
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            if (sendDiffs && exp.unwrapCause() instanceof IncompatibleClusterStateVersionException) {
                                logger.debug("resending full cluster state to node {} reason {}", node, exp.getDetailedMessage());
                                sendFullClusterState(clusterState, null, node, timedOutWaitingForNodes, publishTimeout, sendingController);
                            } else {
                                logger.debug("failed to send cluster state to {}", exp, node);
                                sendingController.onNodeSendFailed(node, exp);
                            }
                        }
                    });
        } catch (Throwable t) {
            logger.warn("error sending cluster state to {}", t, node);
            sendingController.onNodeSendFailed(node, t);
        }
    }

    private void sendCommitToNode(final DiscoveryNode node, final ClusterState clusterState, final BlockingClusterStatePublishResponseHandler publishResponseHandler) {
        try {
            logger.trace("sending commit for cluster state (uuid: [{}], version [{}]) to [{}]", clusterState.stateUUID(), clusterState.version(), node);
            TransportRequestOptions options = TransportRequestOptions.options().withType(TransportRequestOptions.Type.STATE).withCompress(false);
            // no need to put a timeout on the options here, because we want the response to eventually be received
            // and not log an error if it arrives after the timeout
            transportService.sendRequest(node, COMMIT_ACTION_NAME,
                    new CommitClusterStateRequest(clusterState.stateUUID()),
                    options, // no need to compress, we already compressed the bytes

                    new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {

                        @Override
                        public void handleResponse(TransportResponse.Empty response) {
//                            if (timedOutWaitingForNodes.get()) {
                            logger.debug("node {} responded to cluster state commit [{}]", node, clusterState.version());
//                            }
                            publishResponseHandler.onResponse(node);
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            logger.debug("failed to commit cluster state (uuid [{}], version [{}]) to {}", exp, clusterState.stateUUID(), clusterState.version(), node);
                            publishResponseHandler.onFailure(node, exp);
                        }
                    });
        } catch (Throwable t) {
            logger.warn("error sending cluster state commit (uuid [{}], version [{}]) to {}", t, clusterState.stateUUID(), clusterState.version(), node);
            publishResponseHandler.onFailure(node, t);
        }
    }


    public static BytesReference serializeFullClusterState(ClusterState clusterState, Version nodeVersion) throws IOException {
        BytesStreamOutput bStream = new BytesStreamOutput();
        try (StreamOutput stream = CompressorFactory.defaultCompressor().streamOutput(bStream)) {
            stream.setVersion(nodeVersion);
            stream.writeBoolean(true);
            clusterState.writeTo(stream);
        }
        return bStream.bytes();
    }

    public static BytesReference serializeDiffClusterState(Diff diff, Version nodeVersion) throws IOException {
        BytesStreamOutput bStream = new BytesStreamOutput();
        try (StreamOutput stream = CompressorFactory.defaultCompressor().streamOutput(bStream)) {
            stream.setVersion(nodeVersion);
            stream.writeBoolean(false);
            diff.writeTo(stream);
        }
        return bStream.bytes();
    }

    private Object lastSeenClusterStateMutex = new Object();
    private ClusterState lastSeenClusterState;

    private class SendClusterStateRequestHandler implements TransportRequestHandler<BytesTransportRequest> {

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
            synchronized (lastSeenClusterStateMutex) {
                final ClusterState incomingState;
                // If true we received full cluster state - otherwise diffs
                if (in.readBoolean()) {
                    incomingState = ClusterState.Builder.readFrom(in, nodesProvider.nodes().localNode());
                    logger.debug("received full cluster state version [{}] with size [{}]", incomingState.version(), request.bytes().length());
                } else if (lastSeenClusterState != null) {
                    Diff<ClusterState> diff = lastSeenClusterState.readDiffFrom(in);
                    incomingState = diff.apply(lastSeenClusterState);
                    logger.debug("received diff cluster state version [{}] with uuid [{}], diff size [{}]", incomingState.version(), incomingState.stateUUID(), request.bytes().length());
                } else {
                    logger.debug("received diff for but don't have any local cluster state - requesting full state");
                    throw new IncompatibleClusterStateVersionException("have no local cluster state");
                }
                // sanity check incoming state
                final ClusterName incomingClusterName = incomingState.getClusterName();
                if (!incomingClusterName.equals(PublishClusterStateAction.this.clusterName)) {
                    logger.warn("received cluster state from [{}] which is also master but with a different cluster name [{}]", incomingState.nodes().masterNode(), incomingClusterName);
                    throw new IllegalStateException("received state from a node that is not part of the cluster");
                }
                if (incomingState.nodes().localNode() == null) {
                    logger.warn("received a cluster state from [{}] and not part of the cluster, should not happen", incomingState.nodes().masterNode());
                    throw new IllegalStateException("received state from a node that is not part of the cluster");
                }
                // state from another master requires more subtle checks, so we let it pass for now (it will be checked in ZenDiscovery)
                if (nodesProvider.nodes().localNodeMaster() == false) {
                    ZenDiscovery.rejectNewClusterStateIfNeeded(logger, nodesProvider.nodes(), incomingState);
                }

                lastSeenClusterState = incomingState;
                lastSeenClusterState.status(ClusterState.ClusterStateStatus.RECEIVED);
            }
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    private class CommitClusterStateRequestHandler implements TransportRequestHandler<CommitClusterStateRequest> {
        @Override
        public void messageReceived(CommitClusterStateRequest request, final TransportChannel channel) throws Exception {
            ClusterState committedClusterState;
            synchronized (lastSeenClusterStateMutex) {
                committedClusterState = lastSeenClusterState;
            }
            if (committedClusterState.stateUUID().equals(request.stateUUID) == false) {
                // nocommit: we need something better here
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
                return;
            }

            try {
                listener.onNewClusterState(committedClusterState, new NewClusterStateListener.NewStateProcessed() {
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
                logger.warn("unexpected error while processing cluster state version [{}]", e, lastSeenClusterState.version());
                try {
                    channel.sendResponse(e);
                } catch (Throwable e1) {
                    logger.debug("failed to send response on cluster state processed", e1);
                }
            }
        }
    }

    static class CommitClusterStateRequest extends TransportRequest {

        String stateUUID;

        public CommitClusterStateRequest() {
        }

        public CommitClusterStateRequest(String stateUUID) {
            this.stateUUID = stateUUID;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            stateUUID = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(stateUUID);
        }
    }


    public class FailedToCommitException extends ElasticsearchException {

        public FailedToCommitException(String msg) {
            super(msg);
        }
    }

    class SendingController {

        private final ClusterState clusterState;
        private final BlockingClusterStatePublishResponseHandler publishResponseHandler;
        volatile int neededMastersToCommit;
        int pendingMasterNodes;
        final ArrayList<DiscoveryNode> sendAckedBeforeCommit = new ArrayList<>();
        final CountDownLatch comittedOrFailed;
        final AtomicBoolean committed;

        private SendingController(ClusterState clusterState, int minMasterNodes, int totalMasterNodes, BlockingClusterStatePublishResponseHandler publishResponseHandler) {
            this.clusterState = clusterState;
            this.publishResponseHandler = publishResponseHandler;
            this.neededMastersToCommit = Math.max(0, minMasterNodes - 1); // we are one of the master nodes
            this.pendingMasterNodes = totalMasterNodes - 1;
            this.committed = new AtomicBoolean(neededMastersToCommit == 0);
            this.comittedOrFailed = new CountDownLatch(committed.get() ? 0 : 1);
        }

        public void waitForCommit(TimeValue commitTimeout) {
            try {
                comittedOrFailed.await(commitTimeout.millis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {

            }
            if (committed.get() == false) {
                throw new FailedToCommitException("failed to get enough masters to ack sent cluster state. [" + neededMastersToCommit + "] left");
            }
        }

        synchronized public void onNodeSendAck(DiscoveryNode node) {
            if (committed.get() == false) {
                sendAckedBeforeCommit.add(node);
                if (node.isMasterNode()) {
                    onMasterNodeSendAck(node);
                }
            } else {
                assert sendAckedBeforeCommit.isEmpty();
                sendCommitToNode(node, clusterState, publishResponseHandler);
            }

        }

        private void onMasterNodeSendAck(DiscoveryNode node) {
            neededMastersToCommit--;
            if (neededMastersToCommit == 0) {
                logger.trace("committing version [{}]", clusterState.version());
                for (DiscoveryNode nodeToCommit : sendAckedBeforeCommit) {
                    sendCommitToNode(nodeToCommit, clusterState, publishResponseHandler);
                }
                sendAckedBeforeCommit.clear();
                boolean success = committed.compareAndSet(false, true);
                assert success;
                comittedOrFailed.countDown();
            }
            onMasterNodeDone(node);
        }

        private void onMasterNodeDone(DiscoveryNode node) {
            pendingMasterNodes--;
            if (pendingMasterNodes == 0) {
                comittedOrFailed.countDown();
            }
        }

        synchronized public void onNodeSendFailed(DiscoveryNode node, Throwable t) {
            if (node.isMasterNode()) {
                onMasterNodeDone(node);
            }
            publishResponseHandler.onFailure(node, t);
        }

    }
}
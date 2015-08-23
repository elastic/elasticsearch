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
import org.elasticsearch.cluster.node.DiscoveryNodes;
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
        final DiscoveryNodes nodes = clusterChangedEvent.state().nodes();
        Set<DiscoveryNode> nodesToPublishTo = new HashSet<>(nodes.size());
        DiscoveryNode localNode = nodes.localNode();
        final int totalMasterNodes = nodes.masterNodes().size();
        for (final DiscoveryNode node : nodes) {
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
        final TimeValue publishTimeout = discoverySettings.getPublishTimeout();
        final boolean sendFullVersion = !discoverySettings.getPublishDiff() || previousState == null;
        final SendingController sendingController = new SendingController(clusterChangedEvent.state(), minMasterNodes, totalMasterNodes, publishResponseHandler);

        final long publishingStartInNanos = System.nanoTime();

        // we build these early as a best effort not to commit in the case of error.
        // sadly this is not water tight as it may that a failed diff based publishing to a node
        // will cause a full serialization based on an older version, which may fail after the
        // change has been committed.
        buildDiffAndSerializeStates(clusterState, previousState, nodesToPublishTo, sendFullVersion, serializedStates, serializedDiffs);

        for (final DiscoveryNode node : nodesToPublishTo) {
            // try and serialize the cluster state once (or per version), so we don't serialize it
            // per node when we send it over the wire, compress it while we are at it...
            // we don't send full version if node didn't exist in the previous version of cluster state
            if (sendFullVersion || !previousState.nodes().nodeExists(node.id())) {
                sendFullClusterState(clusterState, serializedStates, node, publishTimeout, sendingController);
            } else {
                sendClusterStateDiff(clusterState, serializedDiffs, serializedStates, node, publishTimeout, sendingController);
            }
        }

        sendingController.waitForCommit(discoverySettings.getCommitTimeout());

        try {
            long timeLeftInNanos = Math.max(0, publishTimeout.nanos() - (System.nanoTime() - publishingStartInNanos));
            sendingController.setPublishingTimedOut(!publishResponseHandler.awaitAllNodes(TimeValue.timeValueNanos(timeLeftInNanos)));
            if (sendingController.getPublishingTimedOut()) {
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

    private void buildDiffAndSerializeStates(ClusterState clusterState, ClusterState previousState, Set<DiscoveryNode> nodesToPublishTo,
                                             boolean sendFullVersion, Map<Version, BytesReference> serializedStates, Map<Version, BytesReference> serializedDiffs) {
        Diff<ClusterState> diff = null;
        for (final DiscoveryNode node : nodesToPublishTo) {
            try {
                if (sendFullVersion || !previousState.nodes().nodeExists(node.id())) {
                    // will send a full reference
                    if (serializedStates.containsKey(node.version()) == false) {
                        serializedStates.put(node.version(), serializeFullClusterState(clusterState, node.version()));
                    }
                } else {
                    // will send a diff
                    if (diff == null) {
                        diff = clusterState.diff(previousState);
                    }
                    if (serializedDiffs.containsKey(node.version()) == false) {
                        serializedDiffs.put(node.version(), serializeDiffClusterState(diff, node.version()));
                    }
                }
            } catch (IOException e) {
                throw new ElasticsearchException("failed to serialize cluster_state for publishing to node {}", e, node);
            }
        }
    }

    private void sendFullClusterState(ClusterState clusterState, @Nullable Map<Version, BytesReference> serializedStates,
                                      DiscoveryNode node, TimeValue publishTimeout, SendingController sendingController) {
        BytesReference bytes = serializedStates.get(node.version());
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
        sendClusterStateToNode(clusterState, bytes, node, publishTimeout, sendingController, false, serializedStates);
    }

    private void sendClusterStateDiff(ClusterState clusterState,
                                      Map<Version, BytesReference> serializedDiffs, Map<Version, BytesReference> serializedStates,
                                      DiscoveryNode node, TimeValue publishTimeout, SendingController sendingController) {
        BytesReference bytes = serializedDiffs.get(node.version());
        assert bytes != null : "failed to find serialized diff for node " + node + " of version [" + node.version() + "]";
        sendClusterStateToNode(clusterState, bytes, node, publishTimeout, sendingController, true, serializedStates);
    }

    private void sendClusterStateToNode(final ClusterState clusterState, BytesReference bytes,
                                        final DiscoveryNode node,
                                        final TimeValue publishTimeout,
                                        final SendingController sendingController,
                                        final boolean sendDiffs, final Map<Version, BytesReference> serializedStates) {
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
                            if (sendingController.getPublishingTimedOut()) {
                                logger.debug("node {} responded for cluster state [{}] (took longer than [{}])", node, clusterState.version(), publishTimeout);
                            }
                            sendingController.onNodeSendAck(node);
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            if (sendDiffs && exp.unwrapCause() instanceof IncompatibleClusterStateVersionException) {
                                logger.debug("resending full cluster state to node {} reason {}", node, exp.getDetailedMessage());
                                sendFullClusterState(clusterState, serializedStates, node, publishTimeout, sendingController);
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

    private void sendCommitToNode(final DiscoveryNode node, final ClusterState clusterState, final SendingController sendingController) {
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
                            if (sendingController.getPublishingTimedOut()) {
                                logger.debug("node {} responded to cluster state commit [{}]", node, clusterState.version());
                            }
                            sendingController.getPublishResponseHandler().onResponse(node);
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            logger.debug("failed to commit cluster state (uuid [{}], version [{}]) to {}", exp, clusterState.stateUUID(), clusterState.version(), node);
                            sendingController.getPublishResponseHandler().onFailure(node, exp);
                        }
                    });
        } catch (Throwable t) {
            logger.warn("error sending cluster state commit (uuid [{}], version [{}]) to {}", t, clusterState.stateUUID(), clusterState.version(), node);
            sendingController.getPublishResponseHandler().onFailure(node, t);
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

    protected void handleIncomingClusterStateRequest(BytesTransportRequest request, TransportChannel channel) throws IOException {
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
            validateIncomingState(incomingState);

            lastSeenClusterState = incomingState;
            lastSeenClusterState.status(ClusterState.ClusterStateStatus.RECEIVED);
        }
        channel.sendResponse(TransportResponse.Empty.INSTANCE);
    }

    // package private for testing

    /**
     * does simple sanity check of the incoming cluster state. Throws an exception on rejections.
     */
    void validateIncomingState(ClusterState state) {
        final ClusterName incomingClusterName = state.getClusterName();
        if (!incomingClusterName.equals(PublishClusterStateAction.this.clusterName)) {
            logger.warn("received cluster state from [{}] which is also master but with a different cluster name [{}]", state.nodes().masterNode(), incomingClusterName);
            throw new IllegalStateException("received state from a node that is not part of the cluster");
        }
        final DiscoveryNodes currentNodes = nodesProvider.nodes();

        if (currentNodes.localNode().equals(state.nodes().localNode()) == false) {
            logger.warn("received a cluster state from [{}] and not part of the cluster, should not happen", state.nodes().masterNode());
            throw new IllegalStateException("received state from a node that is not part of the cluster");
        }
        // state from another master requires more subtle checks, so we let it pass for now (it will be checked in ZenDiscovery)
        if (currentNodes.localNodeMaster() == false) {
            ZenDiscovery.validateStateIsFromCurrentMaster(logger, currentNodes, state);
        }
    }

    protected void handleCommitRequest(CommitClusterStateRequest request, final TransportChannel channel) {
        ClusterState committedClusterState;
        synchronized (lastSeenClusterStateMutex) {
            committedClusterState = lastSeenClusterState;
        }

        // if this message somehow comes without a previous send, we won't have a cluster state
        String lastSeenUUID = committedClusterState == null ? null : committedClusterState.stateUUID();
        if (request.stateUUID.equals(lastSeenUUID) == false) {
            throw new IllegalStateException("tried to commit cluster state UUID [" + request.stateUUID + "], but last seen UUID is [" + lastSeenUUID + "]");
        }

        try {
            listener.onNewClusterState(committedClusterState, new NewClusterStateListener.NewStateProcessed() {
                @Override
                public void onNewClusterStateProcessed() {
                    try {
                        channel.sendResponse(TransportResponse.Empty.INSTANCE);
                    } catch (Throwable e) {
                        logger.debug("failed to send response on cluster state processed", e);
                        onNewClusterStateFailed(e);
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
            throw e;
        }
    }

    private class SendClusterStateRequestHandler implements TransportRequestHandler<BytesTransportRequest> {

        @Override
        public void messageReceived(BytesTransportRequest request, final TransportChannel channel) throws Exception {
            handleIncomingClusterStateRequest(request, channel);
        }
    }

    private class CommitClusterStateRequestHandler implements TransportRequestHandler<CommitClusterStateRequest> {
        @Override
        public void messageReceived(CommitClusterStateRequest request, final TransportChannel channel) throws Exception {
            handleCommitRequest(request, channel);
        }
    }

    protected static class CommitClusterStateRequest extends TransportRequest {

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


    public static class FailedToCommitException extends ElasticsearchException {

        public FailedToCommitException(StreamInput in) throws IOException {
            super(in);
        }

        public FailedToCommitException(String msg, Object... args) {
            super(msg, args);
        }
    }

    class SendingController {

        private final ClusterState clusterState;

        public BlockingClusterStatePublishResponseHandler getPublishResponseHandler() {
            return publishResponseHandler;
        }

        private final BlockingClusterStatePublishResponseHandler publishResponseHandler;
        volatile int neededMastersToCommit;
        int pendingMasterNodes;
        final ArrayList<DiscoveryNode> sendAckedBeforeCommit = new ArrayList<>();
        final CountDownLatch comittedOrFailed;
        final AtomicBoolean committed;

        // an external marker to note that the publishing process is timed out. This is usefull for proper logging.
        final AtomicBoolean publishingTimedOut = new AtomicBoolean();

        private SendingController(ClusterState clusterState, int minMasterNodes, int totalMasterNodes, BlockingClusterStatePublishResponseHandler publishResponseHandler) {
            this.clusterState = clusterState;
            this.publishResponseHandler = publishResponseHandler;
            this.neededMastersToCommit = Math.max(0, minMasterNodes - 1); // we are one of the master nodes
            this.pendingMasterNodes = totalMasterNodes - 1;
            if (this.neededMastersToCommit > this.pendingMasterNodes) {
                throw new FailedToCommitException("not enough masters to ack sent cluster state. [{}] needed , have [{}]", neededMastersToCommit, pendingMasterNodes);
            }
            this.committed = new AtomicBoolean(neededMastersToCommit == 0);
            this.comittedOrFailed = new CountDownLatch(committed.get() ? 0 : 1);
        }

        public void waitForCommit(TimeValue commitTimeout) {
            boolean timedout = false;
            try {
                timedout = comittedOrFailed.await(commitTimeout.millis(), TimeUnit.MILLISECONDS) == false;
            } catch (InterruptedException e) {

            }
            //nocommit: make sure we prevent publishing successfully!
            if (committed.get() == false) {
                throw new FailedToCommitException("{} enough masters to ack sent cluster state. [{}] left",
                        timedout ? "timed out while waiting for" : "failed to get", neededMastersToCommit);
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
                sendCommitToNode(node, clusterState, this);
            }

        }

        private void onMasterNodeSendAck(DiscoveryNode node) {
            logger.trace("master node {} acked cluster state version [{}]. processing ... (current pending [{}], needed [{}])",
                    node, clusterState.version(), pendingMasterNodes, neededMastersToCommit);
            neededMastersToCommit--;
            if (neededMastersToCommit == 0) {
                logger.trace("committing version [{}]", clusterState.version());
                for (DiscoveryNode nodeToCommit : sendAckedBeforeCommit) {
                    sendCommitToNode(nodeToCommit, clusterState, this);
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
            if (pendingMasterNodes == 0 && neededMastersToCommit > 0) {
                logger.trace("failed to commit version [{}]. All master nodes acked or failed but [{}] acks are still needed",
                        clusterState.version(), neededMastersToCommit);
                comittedOrFailed.countDown();
            }
        }

        synchronized public void onNodeSendFailed(DiscoveryNode node, Throwable t) {
            if (node.isMasterNode()) {
                logger.trace("master node {} failed to ack cluster state version [{}]. processing ... (current pending [{}], needed [{}])",
                        node, clusterState.version(), pendingMasterNodes, neededMastersToCommit);
                onMasterNodeDone(node);
            }
            publishResponseHandler.onFailure(node, t);
        }

        public boolean getPublishingTimedOut() {
            return publishingTimedOut.get();
        }

        public void setPublishingTimedOut(boolean isTimedOut) {
            publishingTimedOut.set(isTimedOut);
        }
    }
}
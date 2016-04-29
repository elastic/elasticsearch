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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.IncompatibleClusterStateVersionException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
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
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BytesTransportRequest;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 *
 */
public class PublishClusterStateAction extends AbstractComponent {

    public static final String SEND_ACTION_NAME = "internal:discovery/zen/publish/send";
    public static final String COMMIT_ACTION_NAME = "internal:discovery/zen/publish/commit";

    public static final String SETTINGS_MAX_PENDING_CLUSTER_STATES = "discovery.zen.publish.max_pending_cluster_states";

    public interface NewPendingClusterStateListener {

        /** a new cluster state has been committed and is ready to process via {@link #pendingStatesQueue()} */
        void onNewClusterState(String reason);
    }

    private final TransportService transportService;
    private final Supplier<ClusterState> clusterStateSupplier;
    private final NewPendingClusterStateListener newPendingClusterStatelistener;
    private final DiscoverySettings discoverySettings;
    private final ClusterName clusterName;
    private final PendingClusterStatesQueue pendingStatesQueue;

    public PublishClusterStateAction(
            Settings settings,
            TransportService transportService,
            Supplier<ClusterState> clusterStateSupplier,
            NewPendingClusterStateListener listener,
            DiscoverySettings discoverySettings,
            ClusterName clusterName) {
        super(settings);
        this.transportService = transportService;
        this.clusterStateSupplier = clusterStateSupplier;
        this.newPendingClusterStatelistener = listener;
        this.discoverySettings = discoverySettings;
        this.clusterName = clusterName;
        this.pendingStatesQueue = new PendingClusterStatesQueue(logger, settings.getAsInt(SETTINGS_MAX_PENDING_CLUSTER_STATES, 25));
        transportService.registerRequestHandler(SEND_ACTION_NAME, BytesTransportRequest::new, ThreadPool.Names.SAME, new SendClusterStateRequestHandler());
        transportService.registerRequestHandler(COMMIT_ACTION_NAME, CommitClusterStateRequest::new, ThreadPool.Names.SAME, new CommitClusterStateRequestHandler());
    }

    public void close() {
        transportService.removeHandler(SEND_ACTION_NAME);
        transportService.removeHandler(COMMIT_ACTION_NAME);
    }

    public PendingClusterStatesQueue pendingStatesQueue() {
        return pendingStatesQueue;
    }

    /**
     * publishes a cluster change event to other nodes. if at least minMasterNodes acknowledge the change it is committed and will
     * be processed by the master and the other nodes.
     * <p>
     * The method is guaranteed to throw a {@link org.elasticsearch.discovery.Discovery.FailedToCommitClusterStateException} if the change is not committed and should be rejected.
     * Any other exception signals the something wrong happened but the change is committed.
     */
    public void publish(final ClusterChangedEvent clusterChangedEvent, final int minMasterNodes, final Discovery.AckListener ackListener) throws Discovery.FailedToCommitClusterStateException {
        final DiscoveryNodes nodes;
        final SendingController sendingController;
        final Set<DiscoveryNode> nodesToPublishTo;
        final Map<Version, BytesReference> serializedStates;
        final Map<Version, BytesReference> serializedDiffs;
        final boolean sendFullVersion;
        try {
            nodes = clusterChangedEvent.state().nodes();
            nodesToPublishTo = new HashSet<>(nodes.getSize());
            DiscoveryNode localNode = nodes.getLocalNode();
            final int totalMasterNodes = nodes.getMasterNodes().size();
            for (final DiscoveryNode node : nodes) {
                if (node.equals(localNode) == false) {
                    nodesToPublishTo.add(node);
                }
            }
            sendFullVersion = !discoverySettings.getPublishDiff() || clusterChangedEvent.previousState() == null;
            serializedStates = new HashMap<>();
            serializedDiffs = new HashMap<>();

            // we build these early as a best effort not to commit in the case of error.
            // sadly this is not water tight as it may that a failed diff based publishing to a node
            // will cause a full serialization based on an older version, which may fail after the
            // change has been committed.
            buildDiffAndSerializeStates(clusterChangedEvent.state(), clusterChangedEvent.previousState(),
                    nodesToPublishTo, sendFullVersion, serializedStates, serializedDiffs);

            final BlockingClusterStatePublishResponseHandler publishResponseHandler = new AckClusterStatePublishResponseHandler(nodesToPublishTo, ackListener);
            sendingController = new SendingController(clusterChangedEvent.state(), minMasterNodes, totalMasterNodes, publishResponseHandler);
        } catch (Throwable t) {
            throw new Discovery.FailedToCommitClusterStateException("unexpected error while preparing to publish", t);
        }

        try {
            innerPublish(clusterChangedEvent, nodesToPublishTo, sendingController, sendFullVersion, serializedStates, serializedDiffs);
        } catch (Discovery.FailedToCommitClusterStateException t) {
            throw t;
        } catch (Throwable t) {
            // try to fail committing, in cause it's still on going
            if (sendingController.markAsFailed("unexpected error", t)) {
                // signal the change should be rejected
                throw new Discovery.FailedToCommitClusterStateException("unexpected error", t);
            } else {
                throw t;
            }
        }
    }

    private void innerPublish(final ClusterChangedEvent clusterChangedEvent, final Set<DiscoveryNode> nodesToPublishTo,
                              final SendingController sendingController, final boolean sendFullVersion,
                              final Map<Version, BytesReference> serializedStates, final Map<Version, BytesReference> serializedDiffs) {

        final ClusterState clusterState = clusterChangedEvent.state();
        final ClusterState previousState = clusterChangedEvent.previousState();
        final TimeValue publishTimeout = discoverySettings.getPublishTimeout();

        final long publishingStartInNanos = System.nanoTime();

        for (final DiscoveryNode node : nodesToPublishTo) {
            // try and serialize the cluster state once (or per version), so we don't serialize it
            // per node when we send it over the wire, compress it while we are at it...
            // we don't send full version if node didn't exist in the previous version of cluster state
            if (sendFullVersion || !previousState.nodes().nodeExists(node.getId())) {
                sendFullClusterState(clusterState, serializedStates, node, publishTimeout, sendingController);
            } else {
                sendClusterStateDiff(clusterState, serializedDiffs, serializedStates, node, publishTimeout, sendingController);
            }
        }

        sendingController.waitForCommit(discoverySettings.getCommitTimeout());

        try {
            long timeLeftInNanos = Math.max(0, publishTimeout.nanos() - (System.nanoTime() - publishingStartInNanos));
            final BlockingClusterStatePublishResponseHandler publishResponseHandler = sendingController.getPublishResponseHandler();
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
                if (sendFullVersion || !previousState.nodes().nodeExists(node.getId())) {
                    // will send a full reference
                    if (serializedStates.containsKey(node.getVersion()) == false) {
                        serializedStates.put(node.getVersion(), serializeFullClusterState(clusterState, node.getVersion()));
                    }
                } else {
                    // will send a diff
                    if (diff == null) {
                        diff = clusterState.diff(previousState);
                    }
                    if (serializedDiffs.containsKey(node.getVersion()) == false) {
                        serializedDiffs.put(node.getVersion(), serializeDiffClusterState(diff, node.getVersion()));
                    }
                }
            } catch (IOException e) {
                throw new ElasticsearchException("failed to serialize cluster_state for publishing to node {}", e, node);
            }
        }
    }

    private void sendFullClusterState(ClusterState clusterState, Map<Version, BytesReference> serializedStates,
                                      DiscoveryNode node, TimeValue publishTimeout, SendingController sendingController) {
        BytesReference bytes = serializedStates.get(node.getVersion());
        if (bytes == null) {
            try {
                bytes = serializeFullClusterState(clusterState, node.getVersion());
                serializedStates.put(node.getVersion(), bytes);
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
        BytesReference bytes = serializedDiffs.get(node.getVersion());
        assert bytes != null : "failed to find serialized diff for node " + node + " of version [" + node.getVersion() + "]";
        sendClusterStateToNode(clusterState, bytes, node, publishTimeout, sendingController, true, serializedStates);
    }

    private void sendClusterStateToNode(final ClusterState clusterState, BytesReference bytes,
                                        final DiscoveryNode node,
                                        final TimeValue publishTimeout,
                                        final SendingController sendingController,
                                        final boolean sendDiffs, final Map<Version, BytesReference> serializedStates) {
        try {

            // -> no need to put a timeout on the options here, because we want the response to eventually be received
            //  and not log an error if it arrives after the timeout
            // -> no need to compress, we already compressed the bytes
            TransportRequestOptions options = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STATE).withCompress(false).build();
            transportService.sendRequest(node, SEND_ACTION_NAME,
                    new BytesTransportRequest(bytes, node.getVersion()),
                    options,
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
            TransportRequestOptions options = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STATE).build();
            // no need to put a timeout on the options here, because we want the response to eventually be received
            // and not log an error if it arrives after the timeout
            transportService.sendRequest(node, COMMIT_ACTION_NAME,
                    new CommitClusterStateRequest(clusterState.stateUUID()),
                    options,
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
                incomingState = ClusterState.Builder.readFrom(in, clusterStateSupplier.get().nodes().getLocalNode());
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
            validateIncomingState(incomingState, lastSeenClusterState);

            pendingStatesQueue.addPending(incomingState);
            lastSeenClusterState = incomingState;
            lastSeenClusterState.status(ClusterState.ClusterStateStatus.RECEIVED);
        }
        channel.sendResponse(TransportResponse.Empty.INSTANCE);
    }

    // package private for testing

    /**
     * does simple sanity check of the incoming cluster state. Throws an exception on rejections.
     */
    void validateIncomingState(ClusterState incomingState, ClusterState lastSeenClusterState) {
        final ClusterName incomingClusterName = incomingState.getClusterName();
        if (!incomingClusterName.equals(this.clusterName)) {
            logger.warn("received cluster state from [{}] which is also master but with a different cluster name [{}]", incomingState.nodes().getMasterNode(), incomingClusterName);
            throw new IllegalStateException("received state from a node that is not part of the cluster");
        }
        final ClusterState clusterState = clusterStateSupplier.get();

        if (clusterState.nodes().getLocalNode().equals(incomingState.nodes().getLocalNode()) == false) {
            logger.warn("received a cluster state from [{}] and not part of the cluster, should not happen", incomingState.nodes().getMasterNode());
            throw new IllegalStateException("received state with a local node that does not match the current local node");
        }

        if (ZenDiscovery.shouldIgnoreOrRejectNewClusterState(logger, clusterState, incomingState)) {
            String message = String.format(
                    Locale.ROOT,
                    "rejecting cluster state version [%d] uuid [%s] received from [%s]",
                    incomingState.version(),
                    incomingState.stateUUID(),
                    incomingState.nodes().getMasterNodeId()
            );
            logger.warn(message);
            throw new IllegalStateException(message);
        }

    }

    protected void handleCommitRequest(CommitClusterStateRequest request, final TransportChannel channel) {
        final ClusterState state = pendingStatesQueue.markAsCommitted(request.stateUUID, new PendingClusterStatesQueue.StateProcessedListener() {
            @Override
            public void onNewClusterStateProcessed() {
                try {
                    // send a response to the master to indicate that this cluster state has been processed post committing it.
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
        if (state != null) {
            newPendingClusterStatelistener.onNewClusterState("master " + state.nodes().getMasterNode() + " committed version [" + state.version() + "]");
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


    /**
     * Coordinates acknowledgments of the sent cluster state from the different nodes. Commits the change
     * after `minimum_master_nodes` have successfully responded or fails the entire change. After committing
     * the cluster state, will trigger a commit message to all nodes that responded previously and responds immediately
     * to all future acknowledgments.
     */
    class SendingController {

        private final ClusterState clusterState;

        public BlockingClusterStatePublishResponseHandler getPublishResponseHandler() {
            return publishResponseHandler;
        }

        private final BlockingClusterStatePublishResponseHandler publishResponseHandler;
        final ArrayList<DiscoveryNode> sendAckedBeforeCommit = new ArrayList<>();

        // writes and reads of these are protected under synchronization
        final CountDownLatch committedOrFailedLatch; // 0 count indicates that a decision was made w.r.t committing or failing
        boolean committed;  // true if cluster state was committed
        int neededMastersToCommit; // number of master nodes acks still needed before committing
        int pendingMasterNodes; // how many master node still need to respond

        // an external marker to note that the publishing process is timed out. This is useful for proper logging.
        final AtomicBoolean publishingTimedOut = new AtomicBoolean();

        private SendingController(ClusterState clusterState, int minMasterNodes, int totalMasterNodes, BlockingClusterStatePublishResponseHandler publishResponseHandler) {
            this.clusterState = clusterState;
            this.publishResponseHandler = publishResponseHandler;
            this.neededMastersToCommit = Math.max(0, minMasterNodes - 1); // we are one of the master nodes
            this.pendingMasterNodes = totalMasterNodes - 1;
            if (this.neededMastersToCommit > this.pendingMasterNodes) {
                throw new Discovery.FailedToCommitClusterStateException("not enough masters to ack sent cluster state. [{}] needed , have [{}]", neededMastersToCommit, pendingMasterNodes);
            }
            this.committed = neededMastersToCommit == 0;
            this.committedOrFailedLatch = new CountDownLatch(committed ? 0 : 1);
        }

        public void waitForCommit(TimeValue commitTimeout) {
            boolean timedout = false;
            try {
                timedout = committedOrFailedLatch.await(commitTimeout.millis(), TimeUnit.MILLISECONDS) == false;
            } catch (InterruptedException e) {
                // the commit check bellow will either translate to an exception or we are committed and we can safely continue
            }

            if (timedout) {
                markAsFailed("timed out waiting for commit (commit timeout [" + commitTimeout + "])");
            }
            if (isCommitted() == false) {
                throw new Discovery.FailedToCommitClusterStateException("{} enough masters to ack sent cluster state. [{}] left",
                        timedout ? "timed out while waiting for" : "failed to get", neededMastersToCommit);
            }
        }

        synchronized public boolean isCommitted() {
            return committed;
        }

        synchronized public void onNodeSendAck(DiscoveryNode node) {
            if (committed) {
                assert sendAckedBeforeCommit.isEmpty();
                sendCommitToNode(node, clusterState, this);
            } else if (committedOrFailed()) {
                logger.trace("ignoring ack from [{}] for cluster state version [{}]. already failed", node, clusterState.version());
            } else {
                // we're still waiting
                sendAckedBeforeCommit.add(node);
                if (node.isMasterNode()) {
                    checkForCommitOrFailIfNoPending(node);
                }
            }
        }

        private synchronized boolean committedOrFailed() {
            return committedOrFailedLatch.getCount() == 0;
        }

        /**
         * check if enough master node responded to commit the change. fails the commit
         * if there are no more pending master nodes but not enough acks to commit.
         */
        synchronized private void checkForCommitOrFailIfNoPending(DiscoveryNode masterNode) {
            logger.trace("master node {} acked cluster state version [{}]. processing ... (current pending [{}], needed [{}])",
                    masterNode, clusterState.version(), pendingMasterNodes, neededMastersToCommit);
            neededMastersToCommit--;
            if (neededMastersToCommit == 0) {
                if (markAsCommitted()) {
                    for (DiscoveryNode nodeToCommit : sendAckedBeforeCommit) {
                        sendCommitToNode(nodeToCommit, clusterState, this);
                    }
                    sendAckedBeforeCommit.clear();
                }
            }
            decrementPendingMasterAcksAndChangeForFailure();
        }

        synchronized private void decrementPendingMasterAcksAndChangeForFailure() {
            pendingMasterNodes--;
            if (pendingMasterNodes == 0 && neededMastersToCommit > 0) {
                markAsFailed("no more pending master nodes, but failed to reach needed acks ([" + neededMastersToCommit + "] left)");
            }
        }

        synchronized public void onNodeSendFailed(DiscoveryNode node, Throwable t) {
            if (node.isMasterNode()) {
                logger.trace("master node {} failed to ack cluster state version [{}]. processing ... (current pending [{}], needed [{}])",
                        node, clusterState.version(), pendingMasterNodes, neededMastersToCommit);
                decrementPendingMasterAcksAndChangeForFailure();
            }
            publishResponseHandler.onFailure(node, t);
        }

        /**
         * tries and commit the current state, if a decision wasn't made yet
         *
         * @return true if successful
         */
        synchronized private boolean markAsCommitted() {
            if (committedOrFailed()) {
                return committed;
            }
            logger.trace("committing version [{}]", clusterState.version());
            committed = true;
            committedOrFailedLatch.countDown();
            return true;
        }

        /**
         * tries marking the publishing as failed, if a decision wasn't made yet
         *
         * @return true if the publishing was failed and the cluster state is *not* committed
         **/
        synchronized private boolean markAsFailed(String details, Throwable reason) {
            if (committedOrFailed()) {
                return committed == false;
            }
            logger.trace("failed to commit version [{}]. {}", reason, clusterState.version(), details);
            committed = false;
            committedOrFailedLatch.countDown();
            return true;
        }

        /**
         * tries marking the publishing as failed, if a decision wasn't made yet
         *
         * @return true if the publishing was failed and the cluster state is *not* committed
         **/
        synchronized private boolean markAsFailed(String reason) {
            if (committedOrFailed()) {
                return committed == false;
            }
            logger.trace("failed to commit version [{}]. {}", clusterState.version(), reason);
            committed = false;
            committedOrFailedLatch.countDown();
            return true;
        }

        public boolean getPublishingTimedOut() {
            return publishingTimedOut.get();
        }

        public void setPublishingTimedOut(boolean isTimedOut) {
            publishingTimedOut.set(isTimedOut);
        }
    }
}

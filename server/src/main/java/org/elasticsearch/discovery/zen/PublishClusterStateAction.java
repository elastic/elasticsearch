/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery.zen;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.IncompatibleClusterStateVersionException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.discovery.AckClusterStatePublishResponseHandler;
import org.elasticsearch.discovery.BlockingClusterStatePublishResponseHandler;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.tasks.Task;
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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class PublishClusterStateAction {

    private static final Logger logger = LogManager.getLogger(PublishClusterStateAction.class);

    public static final String SEND_ACTION_NAME = "internal:discovery/zen/publish/send";
    public static final String COMMIT_ACTION_NAME = "internal:discovery/zen/publish/commit";

    // -> no need to put a timeout on the options, because we want the state response to eventually be received
    //  and not log an error if it arrives after the timeout
    private static final TransportRequestOptions STATE_REQUEST_OPTIONS =
            TransportRequestOptions.of(null, TransportRequestOptions.Type.STATE);

    public interface IncomingClusterStateListener {

        /**
         * called when a new incoming cluster state has been received.
         * Should validate the incoming state and throw an exception if it's not a valid successor state.
         */
        void onIncomingClusterState(ClusterState incomingState);

        /**
         * called when a cluster state has been committed and is ready to be processed
         */
        void onClusterStateCommitted(String stateUUID, ActionListener<Void> processedListener);
    }

    private final TransportService transportService;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final IncomingClusterStateListener incomingClusterStateListener;
    private final DiscoverySettings discoverySettings;

    private final AtomicLong fullClusterStateReceivedCount = new AtomicLong();
    private final AtomicLong incompatibleClusterStateDiffReceivedCount = new AtomicLong();
    private final AtomicLong compatibleClusterStateDiffReceivedCount = new AtomicLong();

    public PublishClusterStateAction(
            TransportService transportService,
            NamedWriteableRegistry namedWriteableRegistry,
            IncomingClusterStateListener incomingClusterStateListener,
            DiscoverySettings discoverySettings) {
        this.transportService = transportService;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.incomingClusterStateListener = incomingClusterStateListener;
        this.discoverySettings = discoverySettings;
        transportService.registerRequestHandler(SEND_ACTION_NAME, ThreadPool.Names.SAME, false, false, BytesTransportRequest::new,
            new SendClusterStateRequestHandler());
        transportService.registerRequestHandler(COMMIT_ACTION_NAME, ThreadPool.Names.SAME, false, false, CommitClusterStateRequest::new,
            new CommitClusterStateRequestHandler());
    }

    /**
     * publishes a cluster change event to other nodes. if at least minMasterNodes acknowledge the change it is committed and will
     * be processed by the master and the other nodes.
     * <p>
     * The method is guaranteed to throw a {@link FailedToCommitClusterStateException}
     * if the change is not committed and should be rejected.
     * Any other exception signals the something wrong happened but the change is committed.
     */
    public void publish(final ClusterChangedEvent clusterChangedEvent, final int minMasterNodes,
                        final Discovery.AckListener ackListener) throws FailedToCommitClusterStateException {
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
            sendFullVersion = discoverySettings.getPublishDiff() == false || clusterChangedEvent.previousState() == null;
            serializedStates = new HashMap<>();
            serializedDiffs = new HashMap<>();

            // we build these early as a best effort not to commit in the case of error.
            // sadly this is not water tight as it may that a failed diff based publishing to a node
            // will cause a full serialization based on an older version, which may fail after the
            // change has been committed.
            buildDiffAndSerializeStates(clusterChangedEvent.state(), clusterChangedEvent.previousState(),
                    nodesToPublishTo, sendFullVersion, serializedStates, serializedDiffs);

            final BlockingClusterStatePublishResponseHandler publishResponseHandler =
                new AckClusterStatePublishResponseHandler(nodesToPublishTo, ackListener);
            sendingController = new SendingController(clusterChangedEvent.state(), minMasterNodes,
                totalMasterNodes, publishResponseHandler);
        } catch (Exception e) {
            throw new FailedToCommitClusterStateException("unexpected error while preparing to publish", e);
        }

        try {
            innerPublish(clusterChangedEvent, nodesToPublishTo, sendingController, ackListener, sendFullVersion, serializedStates,
                serializedDiffs);
        } catch (FailedToCommitClusterStateException t) {
            throw t;
        } catch (Exception e) {
            // try to fail committing, in cause it's still on going
            if (sendingController.markAsFailed("unexpected error", e)) {
                // signal the change should be rejected
                throw new FailedToCommitClusterStateException("unexpected error", e);
            } else {
                throw e;
            }
        }
    }

    private void innerPublish(final ClusterChangedEvent clusterChangedEvent, final Set<DiscoveryNode> nodesToPublishTo,
                              final SendingController sendingController, final Discovery.AckListener ackListener,
                              final boolean sendFullVersion, final Map<Version, BytesReference> serializedStates,
                              final Map<Version, BytesReference> serializedDiffs) {

        final ClusterState clusterState = clusterChangedEvent.state();
        final ClusterState previousState = clusterChangedEvent.previousState();
        final TimeValue publishTimeout = discoverySettings.getPublishTimeout();

        final long publishingStartInNanos = System.nanoTime();

        for (final DiscoveryNode node : nodesToPublishTo) {
            // try and serialize the cluster state once (or per version), so we don't serialize it
            // per node when we send it over the wire, compress it while we are at it...
            // we don't send full version if node didn't exist in the previous version of cluster state
            if (sendFullVersion || previousState.nodes().nodeExists(node) == false) {
                sendFullClusterState(clusterState, serializedStates, node, publishTimeout, sendingController);
            } else {
                sendClusterStateDiff(clusterState, serializedDiffs, serializedStates, node, publishTimeout, sendingController);
            }
        }

        sendingController.waitForCommit(discoverySettings.getCommitTimeout());

        final long commitTime = System.nanoTime() - publishingStartInNanos;

        ackListener.onCommit(TimeValue.timeValueNanos(commitTime));

        try {
            long timeLeftInNanos = Math.max(0, publishTimeout.nanos() - commitTime);
            final BlockingClusterStatePublishResponseHandler publishResponseHandler = sendingController.getPublishResponseHandler();
            sendingController.setPublishingTimedOut(
                    publishResponseHandler.awaitAllNodes(TimeValue.timeValueNanos(timeLeftInNanos)) == false);
            if (sendingController.getPublishingTimedOut()) {
                DiscoveryNode[] pendingNodes = publishResponseHandler.pendingNodes();
                // everyone may have just responded
                if (pendingNodes.length > 0) {
                    logger.warn("timed out waiting for all nodes to process published state [{}] (timeout [{}], pending nodes: {})",
                        clusterState.version(), publishTimeout, pendingNodes);
                }
            }
            // The failure is logged under debug when a sending failed. we now log a summary.
            Set<DiscoveryNode> failedNodes = publishResponseHandler.getFailedNodes();
            if (failedNodes.isEmpty() == false) {
                logger.warn("publishing cluster state with version [{}] failed for the following nodes: [{}]",
                    clusterChangedEvent.state().version(), failedNodes);
            }
        } catch (InterruptedException e) {
            // ignore & restore interrupt
            Thread.currentThread().interrupt();
        }
    }

    private void buildDiffAndSerializeStates(ClusterState clusterState, ClusterState previousState, Set<DiscoveryNode> nodesToPublishTo,
                                             boolean sendFullVersion, Map<Version, BytesReference> serializedStates,
                                             Map<Version, BytesReference> serializedDiffs) {
        Diff<ClusterState> diff = null;
        for (final DiscoveryNode node : nodesToPublishTo) {
            try {
                if (sendFullVersion || previousState.nodes().nodeExists(node) == false) {
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
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("failed to serialize cluster_state before publishing it to node {}", node), e);
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

            transportService.sendRequest(node, SEND_ACTION_NAME,
                    new BytesTransportRequest(bytes, node.getVersion()),
                    STATE_REQUEST_OPTIONS,
                    new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {

                        @Override
                        public void handleResponse(TransportResponse.Empty response) {
                            if (sendingController.getPublishingTimedOut()) {
                                logger.debug("node {} responded for cluster state [{}] (took longer than [{}])", node,
                                    clusterState.version(), publishTimeout);
                            }
                            sendingController.onNodeSendAck(node);
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            if (sendDiffs && exp.unwrapCause() instanceof IncompatibleClusterStateVersionException) {
                                logger.debug("resending full cluster state to node {} reason {}", node, exp.getDetailedMessage());
                                sendFullClusterState(clusterState, serializedStates, node, publishTimeout, sendingController);
                            } else {
                                logger.debug(() -> new ParameterizedMessage("failed to send cluster state to {}", node), exp);
                                sendingController.onNodeSendFailed(node, exp);
                            }
                        }
                    });
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("error sending cluster state to {}", node), e);
            sendingController.onNodeSendFailed(node, e);
        }
    }

    private void sendCommitToNode(final DiscoveryNode node, final ClusterState clusterState, final SendingController sendingController) {
        try {
            logger.trace("sending commit for cluster state (uuid: [{}], version [{}]) to [{}]",
                clusterState.stateUUID(), clusterState.version(), node);
            transportService.sendRequest(node, COMMIT_ACTION_NAME,
                    new CommitClusterStateRequest(clusterState.stateUUID()),
                    STATE_REQUEST_OPTIONS,
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
                            logger.debug(() -> new ParameterizedMessage("failed to commit cluster state (uuid [{}], version [{}]) to {}",
                                    clusterState.stateUUID(), clusterState.version(), node), exp);
                            sendingController.getPublishResponseHandler().onFailure(node, exp);
                        }
                    });
        } catch (Exception t) {
            logger.warn(() -> new ParameterizedMessage("error sending cluster state commit (uuid [{}], version [{}]) to {}",
                    clusterState.stateUUID(), clusterState.version(), node), t);
            sendingController.getPublishResponseHandler().onFailure(node, t);
        }
    }


    public static BytesReference serializeFullClusterState(ClusterState clusterState, Version nodeVersion) throws IOException {
        BytesStreamOutput bStream = new BytesStreamOutput();
        try (StreamOutput stream = new OutputStreamStreamOutput(CompressorFactory.COMPRESSOR.threadLocalOutputStream(bStream))) {
            stream.setVersion(nodeVersion);
            stream.writeBoolean(true);
            clusterState.writeTo(stream);
        }
        return bStream.bytes();
    }

    public static BytesReference serializeDiffClusterState(Diff<?> diff, Version nodeVersion) throws IOException {
        BytesStreamOutput bStream = new BytesStreamOutput();
        try (StreamOutput stream = new OutputStreamStreamOutput(CompressorFactory.COMPRESSOR.threadLocalOutputStream(bStream))) {
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
        StreamInput in = request.bytes().streamInput();
        final ClusterState incomingState;
        synchronized (lastSeenClusterStateMutex) {
            try {
                if (compressor != null) {
                    in = new InputStreamStreamInput(compressor.threadLocalInputStream(in));
                }
                in = new NamedWriteableAwareStreamInput(in, namedWriteableRegistry);
                in.setVersion(request.version());
                // If true we received full cluster state - otherwise diffs
                if (in.readBoolean()) {
                    incomingState = ClusterState.readFrom(in, transportService.getLocalNode());
                    fullClusterStateReceivedCount.incrementAndGet();
                    logger.debug("received full cluster state version [{}] with size [{}]", incomingState.version(),
                        request.bytes().length());
                } else if (lastSeenClusterState != null) {
                    Diff<ClusterState> diff = ClusterState.readDiffFrom(in, lastSeenClusterState.nodes().getLocalNode());
                    incomingState = diff.apply(lastSeenClusterState);
                    compatibleClusterStateDiffReceivedCount.incrementAndGet();
                    logger.debug("received diff cluster state version [{}] with uuid [{}], diff size [{}]",
                        incomingState.version(), incomingState.stateUUID(), request.bytes().length());
                } else {
                    logger.debug("received diff for but don't have any local cluster state - requesting full state");
                    throw new IncompatibleClusterStateVersionException("have no local cluster state");
                }
            } catch (IncompatibleClusterStateVersionException e) {
                incompatibleClusterStateDiffReceivedCount.incrementAndGet();
                throw e;
            } catch (Exception e) {
                logger.warn("unexpected error while deserializing an incoming cluster state", e);
                throw e;
            } finally {
                IOUtils.close(in);
            }
            incomingClusterStateListener.onIncomingClusterState(incomingState);
            lastSeenClusterState = incomingState;
        }
        channel.sendResponse(TransportResponse.Empty.INSTANCE);
    }

    protected void handleCommitRequest(CommitClusterStateRequest request, final TransportChannel channel) {
        incomingClusterStateListener.onClusterStateCommitted(request.stateUUID, new ActionListener<Void>() {

            @Override
            public void onResponse(Void ignore) {
                try {
                    // send a response to the master to indicate that this cluster state has been processed post committing it.
                    channel.sendResponse(TransportResponse.Empty.INSTANCE);
                } catch (Exception e) {
                    logger.debug("failed to send response on cluster state processed", e);
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    channel.sendResponse(e);
                } catch (Exception inner) {
                    inner.addSuppressed(e);
                    logger.debug("failed to send response on cluster state processed", inner);
                }
            }
        });
    }

    private class SendClusterStateRequestHandler implements TransportRequestHandler<BytesTransportRequest> {

        @Override
        public void messageReceived(BytesTransportRequest request, final TransportChannel channel, Task task) throws Exception {
            handleIncomingClusterStateRequest(request, channel);
        }
    }

    private class CommitClusterStateRequestHandler implements TransportRequestHandler<CommitClusterStateRequest> {
        @Override
        public void messageReceived(CommitClusterStateRequest request, final TransportChannel channel, Task task) throws Exception {
            handleCommitRequest(request, channel);
        }
    }

    public static class CommitClusterStateRequest extends TransportRequest {

        public String stateUUID;

        public CommitClusterStateRequest(StreamInput in) throws IOException {
            super(in);
            stateUUID = in.readString();
        }

        public CommitClusterStateRequest(String stateUUID) {
            this.stateUUID = stateUUID;
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

        private SendingController(ClusterState clusterState, int minMasterNodes, int totalMasterNodes,
                                  BlockingClusterStatePublishResponseHandler publishResponseHandler) {
            this.clusterState = clusterState;
            this.publishResponseHandler = publishResponseHandler;
            this.neededMastersToCommit = Math.max(0, minMasterNodes - 1); // we are one of the master nodes
            this.pendingMasterNodes = totalMasterNodes - 1;
            if (this.neededMastersToCommit > this.pendingMasterNodes) {
                throw new FailedToCommitClusterStateException("not enough masters to ack sent cluster state." +
                    "[{}] needed , have [{}]", neededMastersToCommit, pendingMasterNodes);
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
                throw new FailedToCommitClusterStateException("{} enough masters to ack sent cluster state. [{}] left",
                        timedout ? "timed out while waiting for" : "failed to get", neededMastersToCommit);
            }
        }

        public synchronized boolean isCommitted() {
            return committed;
        }

        public synchronized void onNodeSendAck(DiscoveryNode node) {
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
        private synchronized void checkForCommitOrFailIfNoPending(DiscoveryNode masterNode) {
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

        private synchronized void decrementPendingMasterAcksAndChangeForFailure() {
            pendingMasterNodes--;
            if (pendingMasterNodes == 0 && neededMastersToCommit > 0) {
                markAsFailed("no more pending master nodes, but failed to reach needed acks ([" + neededMastersToCommit + "] left)");
            }
        }

        public synchronized void onNodeSendFailed(DiscoveryNode node, Exception e) {
            if (node.isMasterNode()) {
                logger.trace("master node {} failed to ack cluster state version [{}]. " +
                        "processing ... (current pending [{}], needed [{}])",
                        node, clusterState.version(), pendingMasterNodes, neededMastersToCommit);
                decrementPendingMasterAcksAndChangeForFailure();
            }
            publishResponseHandler.onFailure(node, e);
        }

        /**
         * tries and commit the current state, if a decision wasn't made yet
         *
         * @return true if successful
         */
        private synchronized boolean markAsCommitted() {
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
        private synchronized boolean markAsFailed(String details, Exception reason) {
            if (committedOrFailed()) {
                return committed == false;
            }
            logger.trace(() -> new ParameterizedMessage("failed to commit version [{}]. {}",
                clusterState.version(), details), reason);
            committed = false;
            committedOrFailedLatch.countDown();
            return true;
        }

        /**
         * tries marking the publishing as failed, if a decision wasn't made yet
         *
         * @return true if the publishing was failed and the cluster state is *not* committed
         **/
        private synchronized boolean markAsFailed(String reason) {
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

    public PublishClusterStateStats stats() {
        return new PublishClusterStateStats(
            fullClusterStateReceivedCount.get(),
            incompatibleClusterStateDiffReceivedCount.get(),
            compatibleClusterStateDiffReceivedCount.get());
    }
}

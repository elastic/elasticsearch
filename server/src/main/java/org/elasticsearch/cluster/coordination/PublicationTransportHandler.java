/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStatePublicationEvent;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.IncompatibleClusterStateVersionException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.PositionTrackingOutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BytesTransportRequest;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.threadpool.ThreadPool.Names.GENERIC;

/**
 * Implements the low-level mechanics of sending a cluster state to other nodes in the cluster during a publication.
 * <p>
 * Cluster states can be quite large and expensive to serialize, but we (mostly) send the same serialized representation to every node in
 * the cluster. This class does the serialization work once, up-front, as part of {@link #newPublicationContext} and then just re-uses the
 * resulting bytes across transport messages.
 * <p>
 * It also uses the {@link Diff} mechanism to reduce the data to be transferred wherever possible. This is only a best-effort mechanism so
 * we fall back to sending a full cluster state if the diff cannot be applied for some reason.
 */
public class PublicationTransportHandler {

    private static final Logger logger = LogManager.getLogger(PublicationTransportHandler.class);

    public static final String PUBLISH_STATE_ACTION_NAME = "internal:cluster/coordination/publish_state";

    private final TransportService transportService;
    private final Executor clusterCoordinationExecutor;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final Function<PublishRequest, PublishWithJoinResponse> handlePublishRequest;

    private final AtomicReference<ClusterState> lastSeenClusterState = new AtomicReference<>();

    private final AtomicLong fullClusterStateReceivedCount = new AtomicLong();
    private final AtomicLong incompatibleClusterStateDiffReceivedCount = new AtomicLong();
    private final AtomicLong compatibleClusterStateDiffReceivedCount = new AtomicLong();
    // -> no need to put a timeout on the options here, because we want the response to eventually be received
    // and not log an error if it arrives after the timeout
    private static final TransportRequestOptions STATE_REQUEST_OPTIONS = TransportRequestOptions.of(
        null,
        TransportRequestOptions.Type.STATE
    );

    public static final TransportVersion INCLUDES_LAST_COMMITTED_DATA_VERSION = TransportVersions.V_8_6_0;

    private final SerializationStatsTracker serializationStatsTracker = new SerializationStatsTracker();

    public PublicationTransportHandler(
        TransportService transportService,
        NamedWriteableRegistry namedWriteableRegistry,
        Function<PublishRequest, PublishWithJoinResponse> handlePublishRequest
    ) {
        this.transportService = transportService;
        this.clusterCoordinationExecutor = transportService.getThreadPool().executor(ThreadPool.Names.CLUSTER_COORDINATION);
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.handlePublishRequest = handlePublishRequest;

        transportService.registerRequestHandler(
            PUBLISH_STATE_ACTION_NAME,
            transportService.getThreadPool().generic(),
            false,
            false,
            BytesTransportRequest::new,
            (request, channel, task) -> this.handleIncomingPublishRequest(request, new ChannelActionListener<>(channel))
        );
    }

    public PublishClusterStateStats stats() {
        return new PublishClusterStateStats(
            fullClusterStateReceivedCount.get(),
            incompatibleClusterStateDiffReceivedCount.get(),
            compatibleClusterStateDiffReceivedCount.get(),
            serializationStatsTracker.getSerializationStats()
        );
    }

    private void handleIncomingPublishRequest(
        BytesTransportRequest request,
        ActionListener<PublishWithJoinResponse> publishResponseListener
    ) throws IOException {
        assert ThreadPool.assertCurrentThreadPool(GENERIC);
        final Compressor compressor = CompressorFactory.compressor(request.bytes());
        StreamInput in = request.bytes().streamInput();
        try {
            if (compressor != null) {
                in = compressor.threadLocalStreamInput(in);
            }
            in = new NamedWriteableAwareStreamInput(in, namedWriteableRegistry);
            in.setTransportVersion(request.version());
            // If true we received full cluster state - otherwise diffs
            if (in.readBoolean()) {
                final ClusterState incomingState;
                // Close early to release resources used by the de-compression as early as possible
                try (StreamInput input = in) {
                    incomingState = ClusterState.readFrom(input, transportService.getLocalNode());
                    assert input.read() == -1;
                } catch (Exception e) {
                    logger.warn("unexpected error while deserializing an incoming cluster state", e);
                    assert false : e;
                    throw e;
                }
                fullClusterStateReceivedCount.incrementAndGet();
                logger.debug("received full cluster state version [{}] with size [{}]", incomingState.version(), request.bytes().length());
                acceptState(incomingState, publishResponseListener.map(response -> {
                    lastSeenClusterState.set(incomingState);
                    return response;
                }));
            } else {
                final ClusterState lastSeen = lastSeenClusterState.get();
                if (lastSeen == null) {
                    logger.debug("received diff for but don't have any local cluster state - requesting full state");
                    incompatibleClusterStateDiffReceivedCount.incrementAndGet();
                    throw new IncompatibleClusterStateVersionException("have no local cluster state");
                } else {
                    final ClusterState incomingState = deserializeAndApplyDiff(request, in, lastSeen);
                    compatibleClusterStateDiffReceivedCount.incrementAndGet();
                    logger.debug(
                        "received diff cluster state version [{}] with uuid [{}], diff size [{}]",
                        incomingState.version(),
                        incomingState.stateUUID(),
                        request.bytes().length()
                    );
                    acceptState(incomingState, publishResponseListener.map(response -> {
                        lastSeenClusterState.compareAndSet(lastSeen, incomingState);
                        return response;
                    }));
                }
            }
        } finally {
            IOUtils.close(in);
        }
    }

    private ClusterState deserializeAndApplyDiff(BytesTransportRequest request, StreamInput in, ClusterState currentState)
        throws IOException {
        ClusterState incomingState;
        try {
            final Diff<ClusterState> diff;
            final boolean includesLastCommittedData = request.version().onOrAfter(INCLUDES_LAST_COMMITTED_DATA_VERSION);
            final boolean clusterUuidCommitted;
            final CoordinationMetadata.VotingConfiguration lastCommittedConfiguration;

            // Close stream early to release resources used by the de-compression as early as possible
            try (StreamInput input = in) {
                diff = ClusterState.readDiffFrom(input, currentState.nodes().getLocalNode());
                if (includesLastCommittedData) {
                    clusterUuidCommitted = in.readBoolean();
                    lastCommittedConfiguration = new CoordinationMetadata.VotingConfiguration(in);
                } else {
                    clusterUuidCommitted = false;
                    lastCommittedConfiguration = null;
                }
                assert input.read() == -1;
            }
            incomingState = diff.apply(currentState); // might throw IncompatibleClusterStateVersionException
            if (includesLastCommittedData) {
                final var adjustedMetadata = incomingState.metadata()
                    .withLastCommittedValues(clusterUuidCommitted, lastCommittedConfiguration);
                if (adjustedMetadata != incomingState.metadata()) {
                    incomingState = ClusterState.builder(incomingState).metadata(adjustedMetadata).build();
                }
            }
        } catch (IncompatibleClusterStateVersionException e) {
            incompatibleClusterStateDiffReceivedCount.incrementAndGet();
            throw e;
        } catch (Exception e) {
            logger.warn("unexpected error while deserializing an incoming cluster state", e);
            assert false : e;
            throw e;
        }
        return incomingState;
    }

    /**
     * Delegate to cluster-coordination thread to apply received state
     *
     * @param incomingState The received cluster state
     * @param actionListener The action to perform once the publish call completes
     */
    private void acceptState(ClusterState incomingState, ActionListener<PublishWithJoinResponse> actionListener) {
        assert incomingState.nodes().isLocalNodeElectedMaster() == false
            : "should handle local publications locally, but got " + incomingState;
        clusterCoordinationExecutor.execute(ActionRunnable.supply(actionListener, new CheckedSupplier<>() {
            @Override
            public PublishWithJoinResponse get() {
                return handlePublishRequest.apply(new PublishRequest(incomingState));
            }

            @Override
            public String toString() {
                return "acceptState[term=" + incomingState.term() + ",version=" + incomingState.version() + "]";
            }
        }));
    }

    public PublicationContext newPublicationContext(ClusterStatePublicationEvent clusterStatePublicationEvent) {
        final PublicationContext publicationContext = new PublicationContext(clusterStatePublicationEvent);
        boolean success = false;
        try {
            // Build the serializations we expect to need now, early in the process, so that an error during serialization fails the
            // publication straight away. This isn't watertight since we send diffs on a best-effort basis and may fall back to sending a
            // full state (and therefore serializing it) if the diff-based publication fails.
            publicationContext.buildDiffAndSerializeStates();
            success = true;
            return publicationContext;
        } finally {
            if (success == false) {
                publicationContext.decRef();
            }
        }
    }

    private ReleasableBytesReference serializeFullClusterState(ClusterState clusterState, DiscoveryNode node, TransportVersion version) {
        try (RecyclerBytesStreamOutput bytesStream = transportService.newNetworkBytesStream()) {
            final long uncompressedBytes;
            try (
                StreamOutput stream = new PositionTrackingOutputStreamStreamOutput(
                    CompressorFactory.COMPRESSOR.threadLocalOutputStream(Streams.flushOnCloseStream(bytesStream))
                )
            ) {
                stream.setTransportVersion(version);
                stream.writeBoolean(true);
                clusterState.writeTo(stream);
                uncompressedBytes = stream.position();
            } catch (IOException e) {
                throw new ElasticsearchException("failed to serialize cluster state for publishing to node {}", e, node);
            }
            final int size = bytesStream.size();
            serializationStatsTracker.serializedFullState(uncompressedBytes, size);
            logger.trace(
                "serialized full cluster state version [{}] using transport version [{}] with size [{}]",
                clusterState.version(),
                version,
                size
            );
            return bytesStream.moveToBytesReference();
        }
    }

    private ReleasableBytesReference serializeDiffClusterState(
        ClusterState newState,
        Diff<ClusterState> diff,
        DiscoveryNode node,
        TransportVersion version
    ) {
        final long clusterStateVersion = newState.version();
        try (RecyclerBytesStreamOutput bytesStream = transportService.newNetworkBytesStream()) {
            final long uncompressedBytes;
            try (
                StreamOutput stream = new PositionTrackingOutputStreamStreamOutput(
                    CompressorFactory.COMPRESSOR.threadLocalOutputStream(Streams.flushOnCloseStream(bytesStream))
                )
            ) {
                stream.setTransportVersion(version);
                stream.writeBoolean(false);
                diff.writeTo(stream);
                if (version.onOrAfter(INCLUDES_LAST_COMMITTED_DATA_VERSION)) {
                    stream.writeBoolean(newState.metadata().clusterUUIDCommitted());
                    newState.getLastCommittedConfiguration().writeTo(stream);
                }
                uncompressedBytes = stream.position();
            } catch (IOException e) {
                throw new ElasticsearchException("failed to serialize cluster state diff for publishing to node {}", e, node);
            }
            final int size = bytesStream.size();
            serializationStatsTracker.serializedDiff(uncompressedBytes, size);
            logger.trace(
                "serialized cluster state diff for version [{}] using transport version [{}] with size [{}]",
                clusterStateVersion,
                version,
                size
            );
            return bytesStream.moveToBytesReference();
        }
    }

    /**
     * Publishing a cluster state typically involves sending the same cluster state (or diff) to every node, so the work of diffing,
     * serializing, and compressing the state can be done once and the results shared across publish requests. The
     * {@code PublicationContext} implements this sharing. It's ref-counted: the initial reference is released by the coordinator when
     * a state (or diff) has been sent to every node, every transmitted diff also holds a reference in case it needs to retry with a full
     * state.
     */
    public class PublicationContext extends AbstractRefCounted {

        private final DiscoveryNodes discoveryNodes;
        private final ClusterState newState;
        private final ClusterState previousState;
        private final Task task;
        private final boolean sendFullVersion;

        private final Map<DiscoveryNode, Transport.Connection> nodeConnections = new HashMap<>();
        // All the values of these maps have one ref for the context (while it's open) and one for each in-flight message.
        private final Map<TransportVersion, ReleasableBytesReference> serializedStates = new ConcurrentHashMap<>();
        private final Map<TransportVersion, ReleasableBytesReference> serializedDiffs = new HashMap<>();

        PublicationContext(ClusterStatePublicationEvent clusterStatePublicationEvent) {
            discoveryNodes = clusterStatePublicationEvent.getNewState().nodes();
            newState = clusterStatePublicationEvent.getNewState();
            previousState = clusterStatePublicationEvent.getOldState();
            task = clusterStatePublicationEvent.getTask();
            sendFullVersion = previousState.getBlocks().disableStatePersistence();
        }

        void buildDiffAndSerializeStates() {
            assert refCount() > 0;
            final LazyInitializable<Diff<ClusterState>, RuntimeException> diffSupplier = new LazyInitializable<>(
                () -> newState.diff(previousState)
            );
            for (DiscoveryNode node : discoveryNodes) {
                if (node.equals(transportService.getLocalNode())) {
                    // publication to local node bypasses any serialization
                    continue;
                }

                Transport.Connection connection;
                try {
                    connection = transportService.getConnection(node);
                } catch (NodeNotConnectedException e) {
                    // can't send to this node, don't need to serialize anything for it
                    logger.debug(() -> format("No connection to [%s] available, skipping serialization", node), e);
                    continue;
                }

                nodeConnections.put(node, connection);
                if (sendFullVersion || previousState.nodes().nodeExists(node) == false) {
                    serializedStates.computeIfAbsent(connection.getTransportVersion(), v -> serializeFullClusterState(newState, node, v));
                } else {
                    serializedDiffs.computeIfAbsent(
                        connection.getTransportVersion(),
                        v -> serializeDiffClusterState(newState, diffSupplier.getOrCompute(), node, v)
                    );
                }
            }
        }

        public void sendPublishRequest(
            DiscoveryNode destination,
            PublishRequest publishRequest,
            ActionListener<PublishWithJoinResponse> listener
        ) {
            assert refCount() > 0;
            assert publishRequest.getAcceptedState() == newState : "state got switched on us";
            assert transportService.getThreadPool().getThreadContext().isSystemContext();
            final var newStateVersion = newState.version();
            if (destination.equals(discoveryNodes.getLocalNode())) {

                // The transport service normally avoids serializing/deserializing requests to the local node but here we have special
                // handling to re-use the serialized representation of the cluster state across requests which means we must also handle
                // local requests differently to avoid having to decompress and deserialize the request on the master.
                //
                // Also, the master needs the original non-serialized state as it contains some transient information that isn't replicated
                // because it only makes sense on the local node (e.g. UnassignedInfo#unassignedTimeNanos).

                final boolean isVotingOnlyNode = discoveryNodes.getLocalNode().getRoles().contains(DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE);
                logger.trace("handling cluster state version [{}] locally on [{}]", newStateVersion, destination);
                transportService.getThreadPool()
                    .executor(ThreadPool.Names.CLUSTER_COORDINATION)
                    .execute(
                        transportService.getThreadPool()
                            .getThreadContext()
                            .preserveContext(ActionRunnable.supply(listener, new CheckedSupplier<>() {
                                @Override
                                public PublishWithJoinResponse get() {
                                    if (isVotingOnlyNode) {
                                        // Voting-only nodes publish their cluster state to other nodes in order to freshen the state held
                                        // on other full master nodes, but then fail the publication before committing. However there's no
                                        // need to freshen our local state so we can fail right away.
                                        throw new TransportException(
                                            new ElasticsearchException("voting-only node skipping local publication to " + destination)
                                        );
                                    } else {
                                        return handlePublishRequest.apply(publishRequest);
                                    }
                                }

                                @Override
                                public String toString() {
                                    return "handling cluster state version [" + newStateVersion + "] locally on [" + destination + "]";
                                }
                            }))
                    );
            } else if (sendFullVersion || previousState.nodes().nodeExists(destination) == false) {
                logger.trace("sending full cluster state version [{}] to [{}]", newStateVersion, destination);
                sendFullClusterState(destination, listener);
            } else {
                logger.trace("sending cluster state diff for version [{}] to [{}]", newStateVersion, destination);
                sendClusterStateDiff(destination, listener);
            }
        }

        private void sendFullClusterState(DiscoveryNode destination, ActionListener<PublishWithJoinResponse> listener) {
            assert refCount() > 0;
            Transport.Connection connection = nodeConnections.get(destination);
            if (connection == null) {
                logger.debug("No connection to [{}] available, skipping send", destination);
                listener.onFailure(new NodeNotConnectedException(destination, "No connection available"));
                return;
            }

            var version = connection.getTransportVersion();
            ReleasableBytesReference bytes = serializedStates.get(version);
            if (bytes == null) {
                try {
                    bytes = serializedStates.computeIfAbsent(version, v -> serializeFullClusterState(newState, destination, v));
                } catch (Exception e) {
                    logger.warn(() -> format("failed to serialize cluster state before publishing it to node %s", destination), e);
                    listener.onFailure(e);
                    return;
                }
            }
            sendClusterState(connection, bytes, listener);
        }

        private void sendClusterStateDiff(DiscoveryNode destination, ActionListener<PublishWithJoinResponse> listener) {
            Transport.Connection connection = nodeConnections.get(destination);
            if (connection == null) {
                logger.debug("No connection to [{}] available, skipping send", destination);
                listener.onFailure(new NodeNotConnectedException(destination, "No connection available"));
                return;
            }

            final ReleasableBytesReference bytes = serializedDiffs.get(connection.getTransportVersion());
            assert bytes != null
                : "failed to find serialized diff for node "
                    + destination
                    + " of version ["
                    + connection.getTransportVersion().toReleaseVersion()
                    + "]";

            // acquire a ref to the context just in case we need to try again with the full cluster state
            if (tryIncRef() == false) {
                assert false;
                listener.onFailure(new IllegalStateException("publication context released before transmission"));
                return;
            }
            sendClusterState(connection, bytes, ActionListener.runAfter(listener.delegateResponse((delegate, e) -> {
                if (e instanceof final TransportException transportException) {
                    if (transportException.unwrapCause() instanceof IncompatibleClusterStateVersionException) {
                        logger.debug(
                            () -> format(
                                "resending full cluster state to node %s reason %s",
                                destination,
                                transportException.getDetailedMessage()
                            )
                        );
                        sendFullClusterState(destination, delegate);
                        return;
                    }
                }

                logger.debug(() -> format("failed to send cluster state to %s", destination), e);
                delegate.onFailure(e);
            }), this::decRef));
        }

        private void sendClusterState(
            Transport.Connection connection,
            ReleasableBytesReference bytes,
            ActionListener<PublishWithJoinResponse> listener
        ) {
            assert refCount() > 0;
            if (bytes.tryIncRef() == false) {
                assert false;
                listener.onFailure(new IllegalStateException("serialized cluster state released before transmission"));
                return;
            }
            transportService.sendChildRequest(
                connection,
                PUBLISH_STATE_ACTION_NAME,
                new BytesTransportRequest(bytes, connection.getTransportVersion()),
                task,
                STATE_REQUEST_OPTIONS,
                new CleanableResponseHandler<>(listener, PublishWithJoinResponse::new, clusterCoordinationExecutor, bytes::decRef)
            );
        }

        @Override
        protected void closeInternal() {
            serializedDiffs.values().forEach(Releasables::closeExpectNoException);
            serializedStates.values().forEach(Releasables::closeExpectNoException);
        }
    }

    private static class SerializationStatsTracker {

        private long fullStateCount;
        private long totalUncompressedFullStateBytes;
        private long totalCompressedFullStateBytes;

        private long diffCount;
        private long totalUncompressedDiffBytes;
        private long totalCompressedDiffBytes;

        public synchronized void serializedFullState(long uncompressedBytes, int compressedBytes) {
            fullStateCount += 1;
            totalUncompressedFullStateBytes += uncompressedBytes;
            totalCompressedFullStateBytes += compressedBytes;
        }

        public synchronized void serializedDiff(long uncompressedBytes, int compressedBytes) {
            diffCount += 1;
            totalUncompressedDiffBytes += uncompressedBytes;
            totalCompressedDiffBytes += compressedBytes;
        }

        public synchronized ClusterStateSerializationStats getSerializationStats() {
            return new ClusterStateSerializationStats(
                fullStateCount,
                totalUncompressedFullStateBytes,
                totalCompressedFullStateBytes,
                diffCount,
                totalUncompressedDiffBytes,
                totalCompressedDiffBytes
            );
        }
    }

}

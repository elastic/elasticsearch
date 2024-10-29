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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BytesTransportRequest;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * Coordinates the join validation process.
 * <p>
 * When a node requests to join an existing cluster, the master first sends it a copy of a recent cluster state to ensure that the new node
 * can make sense of it (e.g. it has all the plugins it needs to even deserialize the state). Cluster states can be expensive to serialize:
 * they are large, so we compress them, but the compression takes extra CPU. Also there may be many nodes all joining at once (e.g. after a
 * full cluster restart or the healing of a large network partition). This component caches the serialized and compressed state that was
 * sent to one joining node and reuses it to validate other join requests that arrive within the cache timeout, avoiding the need to
 * allocate memory for each request and repeat all that serialization and compression work each time.
 */
public class JoinValidationService {

    /*
     * IMPLEMENTATION NOTES
     *
     * This component is based around a queue of actions which are processed in a single-threaded fashion on the CLUSTER_COORDINATION
     * threadpool. The actions are either:
     *
     * - send a join validation request to a particular node, or
     * - clear the cache
     *
     * The single-threadedness is arranged by tracking (a lower bound on) the size of the queue in a separate AtomicInteger, and only
     * spawning a new processor when the tracked queue size changes from 0 to 1.
     *
     * The executeRefs ref counter is necessary to handle the possibility of a concurrent shutdown, ensuring that the cache is always
     * cleared even if validateJoin is called concurrently to the shutdown.
     */

    private static final Logger logger = LogManager.getLogger(JoinValidationService.class);

    public static final String JOIN_VALIDATE_ACTION_NAME = "internal:cluster/coordination/join/validate";

    // the timeout for each cached value
    public static final Setting<TimeValue> JOIN_VALIDATION_CACHE_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.join_validation.cache_timeout",
        TimeValue.timeValueSeconds(60),
        TimeValue.timeValueMillis(1),
        Setting.Property.NodeScope
    );

    private static final TransportRequestOptions REQUEST_OPTIONS = TransportRequestOptions.of(null, TransportRequestOptions.Type.STATE);

    private final TimeValue cacheTimeout;
    private final TransportService transportService;
    private final Supplier<ClusterState> clusterStateSupplier;
    private final AtomicInteger queueSize = new AtomicInteger();
    private final Queue<AbstractRunnable> queue = new ConcurrentLinkedQueue<>();
    private final Map<TransportVersion, ReleasableBytesReference> statesByVersion = new HashMap<>();
    private final RefCounted executeRefs;
    private final Executor responseExecutor;

    public JoinValidationService(
        Settings settings,
        TransportService transportService,
        Supplier<ClusterState> clusterStateSupplier,
        Supplier<Metadata> metadataSupplier,
        Collection<BiConsumer<DiscoveryNode, ClusterState>> joinValidators
    ) {
        this.cacheTimeout = JOIN_VALIDATION_CACHE_TIMEOUT_SETTING.get(settings);
        this.transportService = transportService;
        this.clusterStateSupplier = clusterStateSupplier;
        this.executeRefs = AbstractRefCounted.of(() -> execute(cacheClearer));
        this.responseExecutor = transportService.getThreadPool().executor(ThreadPool.Names.CLUSTER_COORDINATION);

        final var dataPaths = Environment.PATH_DATA_SETTING.get(settings);
        transportService.registerRequestHandler(
            JoinValidationService.JOIN_VALIDATE_ACTION_NAME,
            this.responseExecutor,
            ValidateJoinRequest::new,
            (request, channel, task) -> {
                final var remoteState = request.getOrReadState();
                final var remoteMetadata = remoteState.metadata();
                final var localMetadata = metadataSupplier.get();
                if (localMetadata.clusterUUIDCommitted() && localMetadata.clusterUUID().equals(remoteMetadata.clusterUUID()) == false) {
                    throw new CoordinationStateRejectedException(
                        "This node previously joined a cluster with UUID ["
                            + localMetadata.clusterUUID()
                            + "] and is now trying to join a different cluster with UUID ["
                            + remoteMetadata.clusterUUID()
                            + "]. This is forbidden and usually indicates an incorrect "
                            + "discovery or cluster bootstrapping configuration. Note that the cluster UUID persists across restarts and "
                            + "can only be changed by deleting the contents of the node's data "
                            + (dataPaths.size() == 1 ? "path " : "paths ")
                            + dataPaths
                            + " which will also remove any data held by this node."
                    );
                }
                joinValidators.forEach(joinValidator -> joinValidator.accept(transportService.getLocalNode(), remoteState));
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            }
        );
    }

    public void validateJoin(DiscoveryNode discoveryNode, ActionListener<Void> listener) {
        // This node isn't in the cluster yet so ClusterState#getMinTransportVersion() doesn't apply, we must obtain a specific connection
        // so we can check its transport version to decide how to proceed.

        final Transport.Connection connection;
        try {
            connection = transportService.getConnection(discoveryNode);
            assert connection != null;
        } catch (Exception e) {
            assert e instanceof NodeNotConnectedException : e;
            listener.onFailure(e);
            return;
        }

        if (executeRefs.tryIncRef()) {
            try {
                execute(new JoinValidation(discoveryNode, connection, listener));
            } finally {
                executeRefs.decRef();
            }
        } else {
            listener.onFailure(new NodeClosedException(transportService.getLocalNode()));
        }
    }

    public void stop() {
        executeRefs.decRef();
    }

    boolean isIdle() {
        // this is for single-threaded tests to assert that the service becomes idle, so it is not properly synchronized
        return queue.isEmpty() && queueSize.get() == 0 && statesByVersion.isEmpty();
    }

    private void execute(AbstractRunnable task) {
        assert task == cacheClearer || executeRefs.hasReferences();
        queue.add(task);
        if (queueSize.getAndIncrement() == 0) {
            runProcessor();
        }
    }

    private void runProcessor() {
        transportService.getThreadPool().executor(ThreadPool.Names.CLUSTER_COORDINATION).execute(processor);
    }

    private final AbstractRunnable processor = new AbstractRunnable() {
        @Override
        protected void doRun() {
            processNextItem();
        }

        @Override
        public void onRejection(Exception e) {
            assert e instanceof EsRejectedExecutionException esre && esre.isExecutorShutdown();
            onShutdown();
        }

        @Override
        public void onFailure(Exception e) {
            logger.error("unexpectedly failed to process queue item", e);
            assert false : e;
        }

        @Override
        public String toString() {
            return "process next task of join validation service";
        }
    };

    private void processNextItem() {
        if (executeRefs.hasReferences() == false) {
            onShutdown();
            return;
        }

        final var nextItem = queue.poll();
        assert nextItem != null;
        try {
            nextItem.run();
        } finally {
            try {
                final var remaining = queueSize.decrementAndGet();
                assert remaining >= 0;
                if (remaining > 0) {
                    runProcessor();
                }
            } catch (Exception e) {
                assert false : e;
                /* we only catch so we can assert false, so throwing is ok */
                // noinspection ThrowFromFinallyBlock
                throw e;
            }
        }
    }

    private void onShutdown() {
        try {
            // shutting down when enqueueing the next processor run which means there is no active processor so it's safe to clear out the
            // cache ...
            cacheClearer.run();

            // ... and drain the queue
            do {
                final var nextItem = queue.poll();
                assert nextItem != null;
                if (nextItem != cacheClearer) {
                    nextItem.onFailure(new NodeClosedException(transportService.getLocalNode()));
                }
            } while (queueSize.decrementAndGet() > 0);
        } catch (Exception e) {
            assert false : e;
            throw e;
        }
    }

    private final AbstractRunnable cacheClearer = new AbstractRunnable() {
        @Override
        public void onFailure(Exception e) {
            logger.error("unexpectedly failed to clear cache", e);
            assert false : e;
        }

        @Override
        protected void doRun() {
            // NB this never runs concurrently to JoinValidation actions, nor to itself, (see IMPLEMENTATION NOTES above) so it is safe
            // to do these (non-atomic) things to the (unsynchronized) statesByVersion map.
            for (final var bytes : statesByVersion.values()) {
                bytes.decRef();
            }
            statesByVersion.clear();
            logger.trace("join validation cache cleared");
        }

        @Override
        public String toString() {
            return "clear join validation cache";
        }
    };

    private class JoinValidation extends ActionRunnable<Void> {
        private final DiscoveryNode discoveryNode;
        private final Transport.Connection connection;

        JoinValidation(DiscoveryNode discoveryNode, Transport.Connection connection, ActionListener<Void> listener) {
            super(listener);
            this.discoveryNode = discoveryNode;
            this.connection = connection;
        }

        @Override
        protected void doRun() {
            // NB these things never run concurrently to each other, or to the cache cleaner (see IMPLEMENTATION NOTES above) so it is safe
            // to do these (non-atomic) things to the (unsynchronized) statesByVersion map.
            var transportVersion = connection.getTransportVersion();
            var cachedBytes = statesByVersion.get(transportVersion);
            var bytes = maybeSerializeClusterState(cachedBytes, discoveryNode, transportVersion);
            if (bytes == null) {
                // Normally if we're not the master then the Coordinator sends a ping message just to validate connectivity instead of
                // getting here. But if we were the master when the Coordinator checked then we might not be the master any more, so we
                // get a null and fall back to a ping here too.

                // noinspection ConstantConditions
                assert cachedBytes == null;
                transportService.sendRequest(
                    connection,
                    JoinHelper.JOIN_PING_ACTION_NAME,
                    new JoinHelper.JoinPingRequest(),
                    REQUEST_OPTIONS,
                    TransportResponseHandler.empty(responseExecutor, listener)
                );
                return;
            }
            bytes.mustIncRef();
            transportService.sendRequest(
                connection,
                JOIN_VALIDATE_ACTION_NAME,
                new BytesTransportRequest(bytes, transportVersion),
                REQUEST_OPTIONS,
                new CleanableResponseHandler<>(
                    listener.map(ignored -> null),
                    in -> TransportResponse.Empty.INSTANCE,
                    responseExecutor,
                    bytes::decRef
                )
            );
            try {
                if (cachedBytes == null) {
                    transportService.getThreadPool().schedule(new Runnable() {
                        @Override
                        public void run() {
                            execute(cacheClearer);
                        }

                        @Override
                        public String toString() {
                            return cacheClearer + " after timeout";
                        }
                    }, cacheTimeout, responseExecutor);
                }
            } catch (Exception e) {
                assert e instanceof EsRejectedExecutionException esre && esre.isExecutorShutdown() : e;
                // we're shutting down, so clear the cache (and handle the shutdown) right away
                execute(cacheClearer);
            }
        }

        @Override
        public String toString() {
            return "send cached join validation request to " + discoveryNode;
        }
    }

    @Nullable // if we are not the master according to the current cluster state
    private ReleasableBytesReference maybeSerializeClusterState(
        ReleasableBytesReference cachedBytes,
        DiscoveryNode discoveryNode,
        TransportVersion version
    ) {
        if (cachedBytes != null) {
            return cachedBytes;
        }

        final var clusterState = clusterStateSupplier.get();
        if (clusterState == null) {
            return null;
        }
        assert clusterState.nodes().isLocalNodeElectedMaster();

        final var bytesStream = transportService.newNetworkBytesStream();
        var success = false;
        try {
            try (
                var stream = new OutputStreamStreamOutput(
                    CompressorFactory.COMPRESSOR.threadLocalOutputStream(Streams.flushOnCloseStream(bytesStream))
                )
            ) {
                stream.setTransportVersion(version);
                clusterState.writeTo(stream);
            } catch (IOException e) {
                throw new ElasticsearchException("failed to serialize cluster state for publishing to node {}", e, discoveryNode);
            }
            final var newBytes = new ReleasableBytesReference(bytesStream.bytes(), bytesStream);
            logger.trace(
                "serialized join validation cluster state version [{}] for transport version [{}] with size [{}]",
                clusterState.version(),
                version,
                newBytes.length()
            );
            final var previousBytes = statesByVersion.put(version, newBytes);
            assert previousBytes == null;
            success = true;
            return newBytes;
        } finally {
            if (success == false) {
                bytesStream.close();
                assert false;
            }
        }
    }
}

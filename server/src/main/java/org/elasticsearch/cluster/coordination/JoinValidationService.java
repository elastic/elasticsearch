/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.cluster.ClusterState;
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
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
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

    public JoinValidationService(
        Settings settings,
        TransportService transportService,
        Supplier<ClusterState> clusterStateSupplier,
        Collection<BiConsumer<DiscoveryNode, ClusterState>> joinValidators
    ) {
        this.cacheTimeout = JOIN_VALIDATION_CACHE_TIMEOUT_SETTING.get(settings);
        this.transportService = transportService;
        this.clusterStateSupplier = clusterStateSupplier;
        this.executeRefs = AbstractRefCounted.of(() -> execute(cacheClearer));

        final var dataPaths = Environment.PATH_DATA_SETTING.get(settings);
        transportService.registerRequestHandler(
            JoinValidationService.JOIN_VALIDATE_ACTION_NAME,
            ThreadPool.Names.CLUSTER_COORDINATION,
            ValidateJoinRequest::new,
            (request, channel, task) -> {
                final var remoteState = request.getOrReadState();
                final var localState = clusterStateSupplier.get();
                if (localState.metadata().clusterUUIDCommitted()
                    && localState.metadata().clusterUUID().equals(remoteState.metadata().clusterUUID()) == false) {
                    throw new CoordinationStateRejectedException(
                        "This node previously joined a cluster with UUID ["
                            + localState.metadata().clusterUUID()
                            + "] and is now trying to join a different cluster with UUID ["
                            + remoteState.metadata().clusterUUID()
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

    public void validateJoin(DiscoveryNode discoveryNode, ActionListener<TransportResponse.Empty> listener) {
        if (discoveryNode.getVersion().onOrAfter(Version.V_8_3_0)) {
            if (executeRefs.tryIncRef()) {
                try {
                    execute(new JoinValidation(discoveryNode, listener));
                } finally {
                    executeRefs.decRef();
                }
            } else {
                listener.onFailure(new NodeClosedException(transportService.getLocalNode()));
            }
        } else {
            transportService.sendRequest(
                discoveryNode,
                JOIN_VALIDATE_ACTION_NAME,
                new ValidateJoinRequest(clusterStateSupplier.get()),
                REQUEST_OPTIONS,
                new ActionListenerResponseHandler<>(listener.delegateResponse((l, e) -> {
                    logger.warn(() -> "failed to validate incoming join request from node [" + discoveryNode + "]", e);
                    listener.onFailure(
                        new IllegalStateException(
                            String.format(
                                Locale.ROOT,
                                "failure when sending a join validation request from [%s] to [%s]",
                                transportService.getLocalNode().descriptionWithoutAttributes(),
                                discoveryNode.descriptionWithoutAttributes()
                            ),
                            e
                        )
                    );
                }), i -> TransportResponse.Empty.INSTANCE, ThreadPool.Names.CLUSTER_COORDINATION)
            );
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

    private class JoinValidation extends ActionRunnable<TransportResponse.Empty> {
        private final DiscoveryNode discoveryNode;

        JoinValidation(DiscoveryNode discoveryNode, ActionListener<TransportResponse.Empty> listener) {
            super(listener);
            this.discoveryNode = discoveryNode;
        }

        @Override
        protected void doRun() throws Exception {
            assert discoveryNode.getVersion().onOrAfter(Version.V_8_3_0) : discoveryNode.getVersion();
            // NB these things never run concurrently to each other, or to the cache cleaner (see IMPLEMENTATION NOTES above) so it is safe
            // to do these (non-atomic) things to the (unsynchronized) statesByVersion map.
            Transport.Connection connection;
            try {
                connection = transportService.getConnection(discoveryNode);
            } catch (NodeNotConnectedException e) {
                listener.onFailure(e);
                return;
            }
            var version = connection.getTransportVersion();
            var cachedBytes = statesByVersion.get(version);
            var bytes = Objects.requireNonNullElseGet(cachedBytes, () -> serializeClusterState(discoveryNode, version));
            assert bytes.hasReferences() : "already closed";
            bytes.incRef();
            transportService.sendRequest(
                connection,
                JOIN_VALIDATE_ACTION_NAME,
                new BytesTransportRequest(bytes, version),
                REQUEST_OPTIONS,
                new CleanableResponseHandler<>(
                    listener,
                    in -> TransportResponse.Empty.INSTANCE,
                    ThreadPool.Names.CLUSTER_COORDINATION,
                    bytes::decRef
                )
            );
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
                }, cacheTimeout, ThreadPool.Names.CLUSTER_COORDINATION);
            }
        }

        @Override
        public String toString() {
            return "send cached join validation request to " + discoveryNode;
        }
    }

    private ReleasableBytesReference serializeClusterState(DiscoveryNode discoveryNode, TransportVersion version) {
        final var bytesStream = transportService.newNetworkBytesStream();
        var success = false;
        try {
            final var clusterState = clusterStateSupplier.get();
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

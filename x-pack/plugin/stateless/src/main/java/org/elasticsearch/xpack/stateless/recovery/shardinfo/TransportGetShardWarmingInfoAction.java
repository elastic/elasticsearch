/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.shardinfo;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.stateless.cache.WarmingRatioProvider;
import org.elasticsearch.xpack.stateless.cache.WarmingRatioProviderFactory;
import org.elasticsearch.xpack.stateless.commits.BlobFileRanges;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.lucene.SearchDirectory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.function.LongSupplier;
import java.util.function.ToLongFunction;

/// Asks the shutting-down source of a search shard relocation for that shard's estimated bytes to warm and the sum of the estimated bytes
/// to warm across all shards on the source that may relocate to the same target. Sent by the relocation target.
///
/// The sum is recomputed on every request from the source's routing table, so shards that finished relocating drop out of it. The
/// per-shard estimates are memoized for the source's current shutdown signal.
public final class TransportGetShardWarmingInfoAction extends TransportAction<
    TransportGetShardWarmingInfoAction.Request,
    TransportGetShardWarmingInfoAction.Response> {

    public static final ActionType<Response> TYPE = new ActionType<>("internal:admin/stateless/search/shard_warming_info");
    static final String NODE_ACTION_NAME = TYPE.name() + "[n]";

    /// The cluster must support this before a target sends the request: a node receiving an unknown action trips an assertion.
    public static final TransportVersion SHARD_WARMING_INFO = TransportVersion.fromName("shard_warming_info");

    private static final Logger logger = LogManager.getLogger(TransportGetShardWarmingInfoAction.class);
    private static final TimeValue REQUEST_TIMEOUT = TimeValue.THIRTY_SECONDS;

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final WarmingRatioProviderFactory warmingRatioProviderFactory;
    private final LongSupplier nowSupplier;

    private final Object memoLock = new Object();
    // Shutdown signal timestamp the memoized estimates were computed under, and the estimates. Guarded by memoLock.
    // These are never cleared, because the source node is shutting down.
    private long memoShutdownSignalTimestamp = -1L;
    private Map<ShardId, Long> memo = new HashMap<>();

    @Inject
    public TransportGetShardWarmingInfoAction(
        ThreadPool threadPool,
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        IndicesService indicesService,
        WarmingRatioProviderFactory warmingRatioProviderFactory
    ) {
        super(TYPE.name(), actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.warmingRatioProviderFactory = warmingRatioProviderFactory;
        this.nowSupplier = threadPool::absoluteTimeInMillis;
        transportService.registerRequestHandler(
            NODE_ACTION_NAME,
            threadPool.generic(),
            NodeRequest::new,
            (request, channel, task) -> ActionListener.completeWith(new ChannelActionListener<>(channel), () -> computeWarmingInfo(request))
        );
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        transportService.sendRequest(
            request.sourceNode(),
            NODE_ACTION_NAME,
            request.nodeRequest(),
            TransportRequestOptions.timeout(REQUEST_TIMEOUT),
            new ActionListenerResponseHandler<>(listener, Response::new, TransportResponseHandler.TRANSPORT_WORKER)
        );
    }

    /// Runs on the source. Responds [Response#NONE] when this shard is absent, or when a shard that may relocate to the target has no
    /// commit yet.
    // visible for testing
    Response computeWarmingInfo(NodeRequest request) {
        final ClusterState state = clusterService.state();
        final String localNodeId = state.nodes().getLocalNodeId();
        final RoutingNode localRoutingNode = state.getRoutingNodes().node(localNodeId);
        if (localRoutingNode == null) {
            logger.debug("no routing node for local node [{}], no warming info for [{}]", localNodeId, request.shardId());
            return Response.NONE;
        }
        final long nowMillis = nowSupplier.getAsLong();
        synchronized (memoLock) {
            final Map<ShardId, Long> estimates = estimatesForSignal(state, localNodeId, request.shutdownSignalTimestamp());
            long sourceBytesToWarm = 0L;
            long shardBytesToWarm = -1L;
            for (ShardRouting routing : localRoutingNode) {
                if (routing.relocating() && request.targetNodeId().equals(routing.relocatingNodeId()) == false) {
                    // already relocating to another target
                    continue;
                }
                final IndexShard shard = indicesService.getShardOrNull(routing.shardId());
                if (shard == null) {
                    // An initializing copy is not created yet, so it has no commit and may still relocate to the target. Any other missing
                    // copy is gone.
                    if (routing.initializing()) {
                        return Response.NONE;
                    }
                    continue;
                }
                if (shard.store().isClosing()) {
                    continue;
                }
                final Long memoized = estimates.get(routing.shardId());
                final long bytesToWarm;
                if (memoized != null) {
                    bytesToWarm = memoized;
                } else {
                    final OptionalLong estimate;
                    try {
                        estimate = estimateShardBytesToWarm(shard, nowMillis);
                    } catch (AlreadyClosedException e) {
                        // Store closed concurrently between isClosing() and directory(); it will not relocate to the target.
                        continue;
                    } catch (Exception e) {
                        logger.warn(() -> "failed to estimate bytes to warm for " + routing.shardId(), e);
                        return Response.NONE;
                    }
                    if (estimate.isEmpty()) {
                        // No commit yet. Counting this as 0 would understate the sum: a ratio of 0 is a present commit instead.
                        return Response.NONE;
                    }
                    bytesToWarm = estimate.getAsLong();
                    estimates.put(routing.shardId(), bytesToWarm);
                }
                sourceBytesToWarm += bytesToWarm;
                if (routing.shardId().equals(request.shardId())) {
                    shardBytesToWarm = bytesToWarm;
                }
            }
            if (shardBytesToWarm < 0L) {
                logger.debug("[{}] is not on local node [{}], no warming info", request.shardId(), localNodeId);
                return Response.NONE;
            }
            return new Response(shardBytesToWarm, sourceBytesToWarm);
        }
    }

    /// Returns the memoized map when the request matches this node's active shutdown signal, or a throwaway map otherwise.
    private Map<ShardId, Long> estimatesForSignal(ClusterState state, String localNodeId, long requestSignalTimestamp) {
        assert Thread.holdsLock(memoLock);
        final var localShutdown = state.metadata().nodeShutdowns().get(localNodeId);
        if (localShutdown == null || localShutdown.getStartedAtMillis() != requestSignalTimestamp) {
            return new HashMap<>();
        }
        if (memoShutdownSignalTimestamp != requestSignalTimestamp) {
            memo = new HashMap<>();
            memoShutdownSignalTimestamp = requestSignalTimestamp;
        }
        return memo;
    }

    /// Empty when the shard has no commit yet. A present 0 is a commit whose warming ratio is 0.
    // visible for testing
    OptionalLong estimateShardBytesToWarm(IndexShard shard, long nowMillis) {
        final SearchDirectory directory = SearchDirectory.unwrapDirectory(shard.store().directory());
        if (directory.getCurrentCommit() == null) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(
            estimateBytesToWarm(
                directory.getCurrentCommitBlobFileRanges(),
                warmingRatioProviderFactory.create(shard.shardId()),
                directory::resolveRegionTimestampMillis,
                nowMillis
            )
        );
    }

    /// Estimated bytes offline warming would fetch, repeating
    /// [org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService#byteRangeToWarmForCC] per compound commit:
    /// each blob is warmed from 0 up to the furthest `ccStart + round(ccSize * ratio)` among its commits.
    /// Files carry no commit identity, so a commit is approximated by the files of one blob sharing a timestamp range,
    /// and its extent by those files' offsets (header bytes and no-longer-referenced files are not counted).
    static long estimateBytesToWarm(
        Collection<BlobFileRanges> ranges,
        WarmingRatioProvider ratioProvider,
        ToLongFunction<StatelessCompoundCommit.TimestampFieldValueRange> resolveTimestamp,
        long nowMillis
    ) {
        // Use timestamp range to identify commits. It's the best we have, and it's acceptable if there is a clash.
        record CommitInBlob(String blobName, @Nullable StatelessCompoundCommit.TimestampFieldValueRange timestampRange) {}
        record Extent(long start, long end) {
            Extent union(Extent other) {
                return new Extent(Math.min(start, other.start), Math.max(end, other.end));
            }
        }
        Map<CommitInBlob, Extent> commits = new HashMap<>();
        for (BlobFileRanges range : ranges) {
            commits.merge(
                new CommitInBlob(range.blobName(), range.timestampRange()),
                new Extent(range.fileOffset(), range.fileOffset() + range.fileLength()),
                Extent::union
            );
        }
        Map<String, Long> warmEndPerBlob = new HashMap<>();
        for (var commit : commits.entrySet()) {
            var timestampRange = commit.getKey().timestampRange();
            double ratio = ratioProvider.getWarmingRatio(timestampRange, resolveTimestamp.applyAsLong(timestampRange), nowMillis);
            if (ratio <= 0) {
                continue;
            }
            Extent extent = commit.getValue();
            long size = extent.end() - extent.start();
            long warmEnd = extent.start() + Math.min(size, Math.round(size * ratio));
            warmEndPerBlob.merge(commit.getKey().blobName(), warmEnd, Math::max);
        }
        return warmEndPerBlob.values().stream().mapToLong(Long::longValue).sum();
    }

    /// Local-only request carrying the source node to send the [NodeRequest] to.
    public static class Request extends ActionRequest {

        private final DiscoveryNode sourceNode;
        private final NodeRequest nodeRequest;

        public Request(DiscoveryNode sourceNode, NodeRequest nodeRequest) {
            this.sourceNode = Objects.requireNonNull(sourceNode);
            this.nodeRequest = Objects.requireNonNull(nodeRequest);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public DiscoveryNode sourceNode() {
            return sourceNode;
        }

        public NodeRequest nodeRequest() {
            return nodeRequest;
        }
    }

    /// Sent to the source node: the warming info for `shardId` and `targetNodeId` under the source's shutdown signal started at
    /// `shutdownSignalTimestamp`.
    public static class NodeRequest extends AbstractTransportRequest {

        private final String targetNodeId;
        private final ShardId shardId;
        private final long shutdownSignalTimestamp;

        public NodeRequest(String targetNodeId, ShardId shardId, long shutdownSignalTimestamp) {
            this.targetNodeId = Objects.requireNonNull(targetNodeId);
            this.shardId = Objects.requireNonNull(shardId);
            this.shutdownSignalTimestamp = shutdownSignalTimestamp;
        }

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            targetNodeId = in.readString();
            shardId = new ShardId(in);
            shutdownSignalTimestamp = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(targetNodeId);
            shardId.writeTo(out);
            out.writeVLong(shutdownSignalTimestamp);
        }

        public String targetNodeId() {
            return targetNodeId;
        }

        public ShardId shardId() {
            return shardId;
        }

        public long shutdownSignalTimestamp() {
            return shutdownSignalTimestamp;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            NodeRequest that = (NodeRequest) o;
            return shutdownSignalTimestamp == that.shutdownSignalTimestamp
                && targetNodeId.equals(that.targetNodeId)
                && shardId.equals(that.shardId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(targetNodeId, shardId, shutdownSignalTimestamp);
        }
    }

    /// `sourceBytesToWarm == 0` means there is no usable warming info: the source could not compute it, or every shard on the source that
    /// may relocate to the target estimates to 0.
    public static final class Response extends ActionResponse {

        static final Response NONE = new Response(0L, 0L);

        private final long shardBytesToWarm;
        private final long sourceBytesToWarm;

        public Response(long shardBytesToWarm, long sourceBytesToWarm) {
            assert shardBytesToWarm >= 0L && sourceBytesToWarm >= shardBytesToWarm : shardBytesToWarm + " / " + sourceBytesToWarm;
            this.shardBytesToWarm = shardBytesToWarm;
            this.sourceBytesToWarm = sourceBytesToWarm;
        }

        public Response(StreamInput in) throws IOException {
            shardBytesToWarm = in.readVLong();
            sourceBytesToWarm = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(shardBytesToWarm);
            out.writeVLong(sourceBytesToWarm);
        }

        public long shardBytesToWarm() {
            return shardBytesToWarm;
        }

        public long sourceBytesToWarm() {
            return sourceBytesToWarm;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            Response that = (Response) o;
            return shardBytesToWarm == that.shardBytesToWarm && sourceBytesToWarm == that.sourceBytesToWarm;
        }

        @Override
        public int hashCode() {
            return Objects.hash(shardBytesToWarm, sourceBytesToWarm);
        }

        @Override
        public String toString() {
            return "Response{shardBytesToWarm=" + shardBytesToWarm + ", sourceBytesToWarm=" + sourceBytesToWarm + '}';
        }
    }
}

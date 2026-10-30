/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.shardinfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
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

/// Asks the shutting-down source of a search shard relocation for that shard's estimated warm amount and the sum of the estimated warm
/// amounts of all its shards competing for the same target. Sent by the target, see `StatelessSearchNodeRecoveryListener`, and used by
/// the warm-amount heuristic of [org.elasticsearch.xpack.stateless.cache.SearchRecoveryTimeoutCalculationService].
///
/// The sum is recomputed on every request from the source's routing table, so shards that finished relocating drop out of it. The
/// per-shard estimates are memoized for the source's current shutdown signal.
public final class TransportGetShardWarmAmountShareAction extends TransportAction<
    TransportGetShardWarmAmountShareAction.Request,
    TransportGetShardWarmAmountShareAction.Response> {

    public static final ActionType<Response> TYPE = new ActionType<>("internal:admin/stateless/search/warm_amount_share");
    static final String NODE_ACTION_NAME = TYPE.name() + "[n]";

    /// The cluster must support this before a target sends the request: a node receiving an unknown action trips an assertion.
    public static final TransportVersion SHARD_WARM_AMOUNT_SHARE = TransportVersion.fromName("shard_warm_amount_share");

    private static final Logger logger = LogManager.getLogger(TransportGetShardWarmAmountShareAction.class);
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
    public TransportGetShardWarmAmountShareAction(
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
            (request, channel, task) -> ActionListener.completeWith(new ChannelActionListener<>(channel), () -> computeShare(request))
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

    /// Runs on the source. Responds [Response#NO_SHARE] when this shard or any shard competing with it has no estimate.
    // visible for testing
    Response computeShare(NodeRequest request) {
        final ClusterState state = clusterService.state();
        final String localNodeId = state.nodes().getLocalNodeId();
        final RoutingNode localRoutingNode = state.getRoutingNodes().node(localNodeId);
        if (localRoutingNode == null) {
            logger.debug("no routing node for local node [{}], no warm amount share for [{}]", localNodeId, request.shardId());
            return Response.NO_SHARE;
        }
        final var localShutdown = state.metadata().nodeShutdowns().get(localNodeId);
        // A target may still act on a shutdown signal this node has already replaced, or one it has not applied yet. Answer it, but only
        // keep estimates computed under this node's own current signal.
        final boolean memoizable = localShutdown != null && localShutdown.getStartedAtMillis() == request.shutdownSignalTimestamp();
        final long nowMillis = nowSupplier.getAsLong();
        synchronized (memoLock) {
            if (memoizable && memoShutdownSignalTimestamp != request.shutdownSignalTimestamp()) {
                memo = new HashMap<>();
                memoShutdownSignalTimestamp = request.shutdownSignalTimestamp();
            }
            final Map<ShardId, Long> estimates = memoizable ? memo : new HashMap<>();
            long competingSum = 0L;
            long shardAmount = -1L;
            for (ShardRouting routing : localRoutingNode) {
                if (routing.relocating() && request.targetNodeId().equals(routing.relocatingNodeId()) == false) {
                    // already relocating to another target, so not competing for this one
                    continue;
                }
                final OptionalLong estimate = memoizedShardWarmAmount(routing, estimates, nowMillis);
                if (estimate.isEmpty()) {
                    logger.debug("no warm amount estimate for [{}], no warm amount share for [{}]", routing.shardId(), request.shardId());
                    return Response.NO_SHARE;
                }
                final long amount = estimate.getAsLong();
                competingSum += amount;
                if (routing.shardId().equals(request.shardId())) {
                    shardAmount = amount;
                }
            }
            if (shardAmount < 0L) {
                logger.debug("[{}] is not on local node [{}], no warm amount share", request.shardId(), localNodeId);
                return Response.NO_SHARE;
            }
            return new Response(shardAmount, competingSum);
        }
    }

    private OptionalLong memoizedShardWarmAmount(ShardRouting routing, Map<ShardId, Long> estimates, long nowMillis) {
        final Long memoized = estimates.get(routing.shardId());
        if (memoized != null) {
            return OptionalLong.of(memoized);
        }
        final OptionalLong estimate = estimateShardWarmAmount(routing.shardId(), nowMillis);
        // A copy recovering onto this node estimates to 0 until its recovery registers the first commit, just before warming starts.
        // Don't memoize that 0, so later requests count its real amount.
        if (estimate.isPresent() && (routing.initializing() == false || estimate.getAsLong() > 0L)) {
            estimates.put(routing.shardId(), estimate.getAsLong());
        }
        return estimate;
    }

    private OptionalLong estimateShardWarmAmount(ShardId shardId, long nowMillis) {
        final IndexShard shard = indicesService.getShardOrNull(shardId);
        if (shard == null) {
            return OptionalLong.empty();
        }
        return tryEstimateShardWarmAmount(shard, nowMillis);
    }

    // visible for testing
    OptionalLong tryEstimateShardWarmAmount(IndexShard shard, long nowMillis) {
        Store store = null;
        boolean acquired = false;
        try {
            store = shard.store();
            acquired = store.tryIncRef();
            if (acquired == false) {
                return OptionalLong.empty();
            }
            SearchDirectory directory = SearchDirectory.unwrapDirectory(store.directory());
            return OptionalLong.of(
                estimateWarmAmount(
                    directory.getCurrentCommitBlobFileRanges(),
                    warmingRatioProviderFactory.create(shard.shardId()),
                    directory::resolveRegionTimestampMillis,
                    nowMillis
                )
            );
        } catch (Exception e) {
            logger.warn(() -> "failed to estimate warm amount for " + shard.shardId(), e);
            return OptionalLong.empty();
        } finally {
            if (acquired) {
                store.decRef();
            }
        }
    }

    /// Estimated bytes offline warming would fetch, repeating
    /// [org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService#byteRangeToWarmForCC] per compound commit:
    /// each blob is warmed from 0 up to the furthest `ccStart + round(ccSize * ratio)` among its commits.
    /// Files carry no commit identity, so a commit is approximated by the files of one blob sharing a timestamp range,
    /// and its extent by those files' offsets (header bytes and no-longer-referenced files are not counted).
    static long estimateWarmAmount(
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

    /// Sent to the source node: the share of `shardId` among the shards competing for `targetNodeId`, under the source's shutdown
    /// signal started at `shutdownSignalTimestamp`.
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

    /// `competingSum == 0` means there is no share: the source could not compute it, or every competing shard estimates to 0.
    public static final class Response extends ActionResponse {

        static final Response NO_SHARE = new Response(0L, 0L);

        private final long shardAmount;
        private final long competingSum;

        public Response(long shardAmount, long competingSum) {
            assert shardAmount >= 0L && competingSum >= shardAmount : shardAmount + " / " + competingSum;
            this.shardAmount = shardAmount;
            this.competingSum = competingSum;
        }

        public Response(StreamInput in) throws IOException {
            shardAmount = in.readVLong();
            competingSum = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(shardAmount);
            out.writeVLong(competingSum);
        }

        public long shardAmount() {
            return shardAmount;
        }

        public long competingSum() {
            return competingSum;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            Response that = (Response) o;
            return shardAmount == that.shardAmount && competingSum == that.competingSum;
        }

        @Override
        public int hashCode() {
            return Objects.hash(shardAmount, competingSum);
        }

        @Override
        public String toString() {
            return "Response{shardAmount=" + shardAmount + ", competingSum=" + competingSum + '}';
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.shardinfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.NoSuchNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.stateless.cache.WarmingRatioProvider;
import org.elasticsearch.xpack.stateless.commits.BlobFileRanges;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.lucene.SearchDirectory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.ToLongFunction;

/**
 * Fetches per-shard warm-volume estimates from a search node that is shutting down.
 * <p>
 * This action is unversioned on purpose so a QA image that contains it can be replaced by an older image.
 * An older node does not register the action and rejects the call before reading the body. Do not ship it this way:
 * a release that must stay in a cluster needs a transport version.
 */
public class TransportFetchShardWarmVolumesAction extends HandledTransportAction<
    TransportFetchShardWarmVolumesAction.Request,
    TransportFetchShardWarmVolumesAction.Response> {

    public static final ActionType<Response> TYPE = new ActionType<>("internal:admin/stateless/indices/shard_warm_volumes");

    private static final Logger logger = LogManager.getLogger(TransportFetchShardWarmVolumesAction.class);

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final TransportService transportService;
    private final String nodeActionName;
    private final Executor genericExecutor;
    private final LongSupplier nowSupplier;
    private final WarmingRatioProvider warmingRatioProvider;
    private final Object sourceMemoLock = new Object();
    private VolumesSnapshot sourceMemo;

    @SuppressWarnings("this-escape")
    @Inject
    public TransportFetchShardWarmVolumesAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndicesService indicesService,
        WarmingRatioProvider warmingRatioProvider
    ) {
        super(TYPE.name(), transportService, actionFilters, Request::new, threadPool.generic());
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.transportService = transportService;
        this.genericExecutor = threadPool.generic();
        this.nodeActionName = actionName + "[n]";
        this.nowSupplier = threadPool::absoluteTimeInMillis;
        this.warmingRatioProvider = warmingRatioProvider;
        transportService.registerRequestHandler(
            nodeActionName,
            genericExecutor,
            Request::new,
            (request, channel, task) -> nodeOperation(new ChannelActionListener<>(channel))
        );
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        DiscoveryNode node = clusterService.state().nodes().get(request.sourceNodeId());
        if (node == null) {
            listener.onFailure(new NoSuchNodeException(request.sourceNodeId()));
            return;
        }
        transportService.sendChildRequest(
            node,
            nodeActionName,
            request,
            task,
            TransportRequestOptions.timeout(TimeValue.THIRTY_SECONDS),
            new ActionListenerResponseHandler<>(listener, Response::new, genericExecutor)
        );
    }

    // visible for testing
    void nodeOperation(ActionListener<Response> listener) {
        ActionListener.completeWith(listener, () -> {
            final var state = clusterService.state();
            VolumesSnapshot snapshot = cachedOrCollect(state);
            return new Response(state.nodes().getLocalNodeId(), snapshot.generation(), snapshot.volumes());
        });
    }

    // Shards still recovering onto the source when shutdown began are recorded with 0 or partial volumes.
    private VolumesSnapshot cachedOrCollect(ClusterState state) {
        final var shutdown = state.metadata().nodeShutdowns().get(state.nodes().getLocalNodeId());
        final long generation = shutdown == null ? Long.MIN_VALUE : shutdown.getStartedAtMillis();
        synchronized (sourceMemoLock) {
            if (sourceMemo != null && sourceMemo.generation() == generation) {
                return sourceMemo;
            }
            final long nowMillis = nowSupplier.getAsLong();
            sourceMemo = new VolumesSnapshot(
                generation,
                collectWarmVolumes(snapshotSearchableShards(indicesService), shard -> tryEstimateShardWarmVolume(shard, nowMillis))
            );
            return sourceMemo;
        }
    }

    static List<IndexShard> snapshotSearchableShards(IndicesService indicesService) {
        List<IndexShard> shards = new ArrayList<>();
        for (var indexService : indicesService) {
            for (IndexShard shard : indexService) {
                var routing = shard.routingEntry();
                if (routing != null && routing.isSearchable()) {
                    shards.add(shard);
                }
            }
        }
        return shards;
    }

    // visible for testing
    static Map<ShardId, Long> collectWarmVolumes(List<IndexShard> shards, Function<IndexShard, OptionalLong> estimator) {
        Map<ShardId, Long> volumes = new HashMap<>();
        for (IndexShard shard : shards) {
            OptionalLong volume = estimator.apply(shard);
            if (volume.isEmpty()) {
                continue;
            }
            volumes.put(shard.shardId(), volume.getAsLong());
        }
        return Map.copyOf(volumes);
    }

    OptionalLong tryEstimateShardWarmVolume(IndexShard shard, long nowMillis) {
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
                estimateWarmVolume(
                    directory.getCurrentCommitBlobFileRanges(),
                    warmingRatioProvider,
                    directory::resolveRegionTimestampMillis,
                    nowMillis
                )
            );
        } catch (Exception e) {
            logger.debug(() -> "failed to estimate warm volume for " + shard.shardId(), e);
            return OptionalLong.empty();
        } finally {
            if (acquired) {
                store.decRef();
            }
        }
    }

    /**
     * Estimated bytes offline warming would fetch, repeating
     * {@link org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService#byteRangeToWarmForCC} per compound commit:
     * each blob is warmed from 0 up to the furthest {@code ccStart + round(ccSize * ratio)} among its commits.
     * Files carry no commit identity, so a commit is approximated by the files of one blob sharing a timestamp range,
     * and its extent by those files' offsets (header bytes and no-longer-referenced files are not counted).
     */
    static long estimateWarmVolume(
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

    private record VolumesSnapshot(long generation, Map<ShardId, Long> volumes) {}

    public static class Request extends ActionRequest {

        private final String sourceNodeId;

        public Request(String sourceNodeId) {
            this.sourceNodeId = Objects.requireNonNull(sourceNodeId);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            sourceNodeId = in.readString();
        }

        public String sourceNodeId() {
            return sourceNodeId;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(sourceNodeId);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(sourceNodeId, request.sourceNodeId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceNodeId);
        }
    }

    public static final class Response extends ActionResponse {

        private final String respondingNodeId;
        private final long volumesGeneration;
        private final Map<ShardId, Long> volumes;

        public Response(String respondingNodeId, long volumesGeneration, Map<ShardId, Long> volumes) {
            this.respondingNodeId = Objects.requireNonNull(respondingNodeId);
            this.volumesGeneration = volumesGeneration;
            this.volumes = Map.copyOf(volumes);
        }

        public Response(StreamInput in) throws IOException {
            respondingNodeId = in.readString();
            volumesGeneration = in.readZLong();
            volumes = in.readImmutableMap(ShardId::new, StreamInput::readVLong);
        }

        public String respondingNodeId() {
            return respondingNodeId;
        }

        public long volumesGeneration() {
            return volumesGeneration;
        }

        public Map<ShardId, Long> volumes() {
            return volumes;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(respondingNodeId);
            out.writeZLong(volumesGeneration);
            out.writeMap(volumes, StreamOutput::writeWriteable, StreamOutput::writeVLong);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return volumesGeneration == response.volumesGeneration
                && Objects.equals(respondingNodeId, response.respondingNodeId)
                && Objects.equals(volumes, response.volumes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(respondingNodeId, volumesGeneration, volumes);
        }
    }
}

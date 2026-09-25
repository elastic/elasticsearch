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
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
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
import org.elasticsearch.xpack.stateless.commits.BlobFileRanges;
import org.elasticsearch.xpack.stateless.engine.SearchEngine;
import org.elasticsearch.xpack.stateless.lucene.SearchDirectory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * Transport Action to share the state of the last acquired index searcher across shards.
 *
 * In order to figure out if data should be prefetched when a shard is moving in the cluster,
 * this action allows to ask other nodes for the last updated timestamp of a ShardId.
 *
 * A Request to a node contains a shardId, the node to be sent to, and optionally a request for
 * per-shard warm-volume estimates of all searchable shards on the responding node.
 *
 * A NodeResponse contains the time of the last acquired searcher in milliseconds since the epoch,
 * and when requested a snapshot of warm volumes keyed by the responding node.
 *
 * The transport action contains the node operation, which queries the SearchEngine instance.
 */
public class TransportFetchSearchShardInformationAction extends HandledTransportAction<
    TransportFetchSearchShardInformationAction.Request,
    TransportFetchSearchShardInformationAction.Response> {

    public static final ActionType<TransportFetchSearchShardInformationAction.Response> TYPE = new ActionType<>(
        "internal:admin/stateless/indices/shard_information"
    );

    public static final TransportVersion FETCH_SHARD_WARM_VOLUMES = TransportVersion.fromName("fetch_shard_warm_volumes");

    private static final Logger logger = LogManager.getLogger(TransportFetchSearchShardInformationAction.class);
    static final long NO_OTHER_SHARDS_FOUND = -1L;
    static final long SHARD_HAS_MOVED = -2L;
    static final Response NO_OTHER_SHARDS_FOUND_RESPONSE = new Response(NO_OTHER_SHARDS_FOUND);
    static final Response SHARD_HAS_MOVED_RESPONSE = new Response(SHARD_HAS_MOVED);

    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final IndicesService indicesService;
    private final TransportService transportService;
    private final String shardActionName;
    private final Executor genericExecutor;
    private final AtomicInteger seed = new AtomicInteger(0);
    private final LongSupplier nowSupplier;
    private final Object sourceMemoLock = new Object();
    private volatile long sourceMemoGeneration = Long.MIN_VALUE;
    private volatile Map<ShardId, Long> sourceMemoVolumes;

    @SuppressWarnings("this-escape")
    @Inject
    public TransportFetchSearchShardInformationAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        ProjectResolver projectResolver,
        TransportService transportService,
        ActionFilters actionFilters,
        IndicesService indicesService
    ) {
        super(TYPE.name(), transportService, actionFilters, TransportFetchSearchShardInformationAction.Request::new, threadPool.generic());
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.indicesService = indicesService;
        this.transportService = transportService;
        this.genericExecutor = threadPool.generic();
        this.shardActionName = actionName + "[s]";
        this.nowSupplier = threadPool::absoluteTimeInMillis;
        transportService.registerRequestHandler(
            shardActionName,
            genericExecutor,
            TransportFetchSearchShardInformationAction.Request::new,
            (request, channel, task) -> shardOperation(request, new ChannelActionListener<>(channel))
        );
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        ProjectState projectState = projectResolver.getProjectState(clusterService.state());

        // specific data stream handling
        // if this is the latest write index of a data stream, shortcut and not even send the request to another node
        // Our basic assumption here is, that latest datastreams backing indices are always eligible
        // for caching with the prefetcher, no need to query around in the cluster
        Optional<ProjectMetadata> projectMetadataOptional = clusterService.state()
            .metadata()
            .lookupProject(request.getShardId().getIndex());
        if (request.wantVolumes() == false && projectMetadataOptional.isPresent()) {
            ProjectMetadata projectMetadata = projectMetadataOptional.get();
            String indexName = request.getShardId().getIndex().getName();

            // ensure old indices that are not written anymore into are accidentally considered new
            long now = nowSupplier.getAsLong();
            long indexAgeMilliSeconds = now - projectMetadata.index(indexName).getCreationDate();
            boolean isIndexAgeWithinRange = TimeValue.timeValueDays(30).millis() > indexAgeMilliSeconds;
            IndexAbstraction index = projectMetadata.getIndicesLookup().get(indexName);
            boolean isWriteIndex = index.getParentDataStream() != null
                && indexName.equals(index.getParentDataStream().getWriteIndex().getName());
            if (isIndexAgeWithinRange && isWriteIndex) {
                listener.onResponse(new Response(now));
                return;
            }
        }

        Optional<ShardRouting> searchShard = findSearchShard(projectState, request);
        if (searchShard.isEmpty()) {
            listener.onResponse(NO_OTHER_SHARDS_FOUND_RESPONSE);
            return;
        }

        ShardRouting shardRouting = searchShard.get();
        DiscoveryNode node = clusterService.state().nodes().resolveNode(shardRouting.currentNodeId());

        logger.trace("requesting shard information from shard {}", shardRouting);
        transportService.sendChildRequest(
            node,
            shardActionName,
            requestForResolvedShard(request, shardRouting),
            task,
            TransportRequestOptions.timeout(TimeValue.THIRTY_SECONDS),
            new ActionListenerResponseHandler<>(listener, TransportFetchSearchShardInformationAction.Response::new, genericExecutor)
        );
    }

    // visible for testing
    static Request requestForResolvedShard(Request request, ShardRouting resolved) {
        if (request.wantVolumes() && resolved.currentNodeId().equals(request.getNodeId()) == false) {
            return new Request(request.getNodeId(), request.getShardId(), false);
        }
        return request;
    }

    // visible for testing
    void shardOperation(Request request, ActionListener<Response> listener) {
        ActionListener.completeWith(listener, () -> {
            IndexShard indexShard = indicesService.getShardOrNull(request.getShardId());
            final long lastSearcherAcquired;
            if (indexShard == null) {
                lastSearcherAcquired = SHARD_HAS_MOVED;
            } else {
                lastSearcherAcquired = indexShard.tryWithEngineOrNull(engine -> {
                    if (engine instanceof SearchEngine searchEngine) {
                        return searchEngine.getLastSearcherAcquiredTime();
                    }

                    return 0L;
                });
            }

            if (request.wantVolumes() == false) {
                return lastSearcherAcquired == SHARD_HAS_MOVED ? SHARD_HAS_MOVED_RESPONSE : new Response(lastSearcherAcquired);
            }
            VolumesSnapshot snapshot = cachedOrCollect();
            return new Response(
                lastSearcherAcquired,
                clusterService.state().nodes().getLocalNodeId(),
                snapshot.generation(),
                snapshot.volumes()
            );
        });
    }

    private VolumesSnapshot cachedOrCollect() {
        final var state = clusterService.state();
        final var shutdown = state.metadata().nodeShutdowns().get(state.nodes().getLocalNodeId());
        final long generation = shutdown == null ? Long.MIN_VALUE : shutdown.getStartedAtMillis();
        synchronized (sourceMemoLock) {
            if (sourceMemoVolumes != null && sourceMemoGeneration == generation) {
                return new VolumesSnapshot(generation, sourceMemoVolumes);
            }
            Map<ShardId, Long> collected = collectWarmVolumes(snapshotSearchableShards(indicesService));
            sourceMemoGeneration = generation;
            sourceMemoVolumes = collected;
            return new VolumesSnapshot(generation, collected);
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

    static Map<ShardId, Long> collectWarmVolumes(List<IndexShard> shards) {
        return collectWarmVolumes(shards, TransportFetchSearchShardInformationAction::tryEstimateShardWarmVolume);
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

    static OptionalLong tryEstimateShardWarmVolume(IndexShard shard) {
        Store store = null;
        boolean acquired = false;
        try {
            store = shard.store();
            acquired = store.tryIncRef();
            if (acquired == false) {
                return OptionalLong.empty();
            }
            SearchDirectory directory = SearchDirectory.unwrapDirectory(store.directory());
            return OptionalLong.of(estimateWarmVolume(List.copyOf(directory.getCurrentCommitBlobFileRanges())));
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
     * Sum of per-blob prefix ends: {@code max(fileOffset + fileLength)} for each blob name.
     */
    public static long estimateWarmVolume(Collection<BlobFileRanges> ranges) {
        Map<String, Long> prefixEnd = new HashMap<>();
        for (BlobFileRanges range : ranges) {
            prefixEnd.merge(range.blobName(), range.fileOffset() + range.fileLength(), Math::max);
        }
        long total = 0L;
        for (long end : prefixEnd.values()) {
            total += end;
        }
        return total;
    }

    // visible for testing
    Optional<ShardRouting> findSearchShard(ProjectState projectState, Request request) {
        ClusterState state = projectState.cluster();
        RoutingTable routingTable = state.routingTable(projectState.projectId());

        List<ShardRouting> activeShardRouting = routingTable.shardRoutingTable(request.getShardId()).activeShards();
        String localNodeId = state.nodes().getLocalNodeId();

        List<ShardRouting> shardRoutings = new ArrayList<>();
        for (ShardRouting shardRouting : activeShardRouting) {
            // find other nodes, this shard resides on, ensure they are active, on other search only nodes and not this node
            if (shardRouting.isSearchable() && localNodeId.equals(shardRouting.currentNodeId()) == false) {
                // if we found the requested node, break out of the loop and use only that
                if (shardRouting.currentNodeId().equals(request.getNodeId())) {
                    shardRoutings = List.of(shardRouting);
                    break;
                } else {
                    shardRoutings.add(shardRouting);
                }
            }
        }

        if (shardRoutings.isEmpty()) {
            logger.trace("found no copies for shard {} to request shard state information", request.getShardId());
            return Optional.empty();
        } else {
            // most simple randomizing
            if (shardRoutings.size() > 1) {
                Collections.rotate(shardRoutings, seed.incrementAndGet());
            }

            ShardRouting candidate = shardRoutings.get(0);
            return Optional.of(candidate);
        }
    }

    private record VolumesSnapshot(long generation, Map<ShardId, Long> volumes) {}

    public static class Request extends ActionRequest {

        private final String nodeId;
        private final ShardId shardId;
        private final boolean wantVolumes;

        public Request(@Nullable String nodeId, ShardId shardId) {
            this(nodeId, shardId, false);
        }

        public Request(@Nullable String nodeId, ShardId shardId, boolean wantVolumes) {
            this.shardId = shardId;
            this.nodeId = nodeId;
            this.wantVolumes = wantVolumes;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            shardId = new ShardId(in);
            nodeId = in.readOptionalString();
            wantVolumes = in.getTransportVersion().supports(FETCH_SHARD_WARM_VOLUMES) && in.readBoolean();
        }

        public String getNodeId() {
            return nodeId;
        }

        public ShardId getShardId() {
            return shardId;
        }

        public boolean wantVolumes() {
            return wantVolumes;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            out.writeOptionalString(nodeId);
            if (out.getTransportVersion().supports(FETCH_SHARD_WARM_VOLUMES)) {
                out.writeBoolean(wantVolumes);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return wantVolumes == request.wantVolumes && Objects.equals(nodeId, request.nodeId) && Objects.equals(shardId, request.shardId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeId, shardId, wantVolumes);
        }
    }

    public static final class Response extends ActionResponse {

        private final long lastSearcherAcquiredTime;
        private final boolean volumesCollected;
        @Nullable
        private final String respondingNodeId;
        private final long volumesGeneration;
        private final Map<ShardId, Long> volumes;

        public Response(long lastSearcherAcquiredTime) {
            this.lastSearcherAcquiredTime = lastSearcherAcquiredTime;
            this.volumesCollected = false;
            this.respondingNodeId = null;
            this.volumesGeneration = Long.MIN_VALUE;
            this.volumes = Map.of();
        }

        public Response(long lastSearcherAcquiredTime, String respondingNodeId, long volumesGeneration, Map<ShardId, Long> volumes) {
            this.lastSearcherAcquiredTime = lastSearcherAcquiredTime;
            this.volumesCollected = true;
            this.respondingNodeId = Objects.requireNonNull(respondingNodeId);
            this.volumesGeneration = volumesGeneration;
            this.volumes = Map.copyOf(volumes);
        }

        public Response(StreamInput in) throws IOException {
            lastSearcherAcquiredTime = in.readZLong();
            if (in.getTransportVersion().supports(FETCH_SHARD_WARM_VOLUMES) && in.readBoolean()) {
                volumesCollected = true;
                respondingNodeId = in.readString();
                volumesGeneration = in.readZLong();
                volumes = in.readImmutableMap(ShardId::new, StreamInput::readVLong);
            } else {
                volumesCollected = false;
                respondingNodeId = null;
                volumesGeneration = Long.MIN_VALUE;
                volumes = Map.of();
            }
        }

        public long getLastSearcherAcquiredTime() {
            return lastSearcherAcquiredTime;
        }

        public boolean volumesCollected() {
            return volumesCollected;
        }

        @Nullable
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
            out.writeZLong(lastSearcherAcquiredTime);
            if (out.getTransportVersion().supports(FETCH_SHARD_WARM_VOLUMES)) {
                out.writeBoolean(volumesCollected);
                if (volumesCollected) {
                    out.writeString(respondingNodeId);
                    out.writeZLong(volumesGeneration);
                    out.writeMap(volumes, StreamOutput::writeWriteable, StreamOutput::writeVLong);
                }
            }
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return lastSearcherAcquiredTime == response.lastSearcherAcquiredTime
                && volumesCollected == response.volumesCollected
                && volumesGeneration == response.volumesGeneration
                && Objects.equals(respondingNodeId, response.respondingNodeId)
                && Objects.equals(volumes, response.volumes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(lastSearcherAcquiredTime, volumesCollected, respondingNodeId, volumesGeneration, volumes);
        }
    }
}

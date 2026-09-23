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
import org.elasticsearch.action.NoSuchNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ActionNotFoundTransportException;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.stateless.cache.ShardWarmVolumes;
import org.elasticsearch.xpack.stateless.commits.BlobFileRanges;
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

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Fetches per-shard warm-volume estimates from a single search node that is shutting down.
 * The node operation walks local searchable shards and memoizes the result by shutdown generation.
 */
public class TransportFetchShardWarmVolumesAction extends HandledTransportAction<
    TransportFetchShardWarmVolumesAction.Request,
    TransportFetchShardWarmVolumesAction.Response> {

    public static final ActionType<Response> TYPE = new ActionType<>("internal:admin/stateless/indices/shard_warm_volumes");
    public static final TransportVersion FETCH_SHARD_WARM_VOLUMES = TransportVersion.fromName("fetch_shard_warm_volumes");

    private static final Logger logger = LogManager.getLogger(TransportFetchShardWarmVolumesAction.class);

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final TransportService transportService;
    private final String nodeActionName;
    private final Executor genericExecutor;
    private final Object sourceMemoLock = new Object();
    private volatile long sourceMemoGeneration = Long.MIN_VALUE;
    private volatile Response sourceMemo;

    @SuppressWarnings("this-escape")
    @Inject
    public TransportFetchShardWarmVolumesAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndicesService indicesService
    ) {
        super(TYPE.name(), transportService, actionFilters, Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.transportService = transportService;
        this.genericExecutor = threadPool.generic();
        this.nodeActionName = actionName + "[n]";
        transportService.registerRequestHandler(
            nodeActionName,
            genericExecutor,
            Request::new,
            (request, channel, task) -> nodeOperation(new ChannelActionListener<>(channel))
        );
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final var state = clusterService.state();
        DiscoveryNode node = state.nodes().get(request.sourceNodeId());
        if (node == null) {
            listener.onFailure(new NoSuchNodeException(request.sourceNodeId()));
            return;
        }
        final Transport.Connection connection;
        try {
            connection = transportService.getConnection(node);
        } catch (NodeNotConnectedException e) {
            listener.onFailure(e);
            return;
        }
        if (connection.getTransportVersion().supports(FETCH_SHARD_WARM_VOLUMES) == false) {
            listener.onFailure(new ActionNotFoundTransportException(nodeActionName));
            return;
        }
        transportService.sendRequest(
            node,
            nodeActionName,
            request,
            TransportRequestOptions.timeout(TimeValue.THIRTY_SECONDS),
            new ActionListenerResponseHandler<>(listener, Response::new, genericExecutor)
        );
    }

    void nodeOperation(ActionListener<Response> listener) {
        try {
            listener.onResponse(cachedOrCollect());
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private Response cachedOrCollect() {
        final var state = clusterService.state();
        final var shutdown = state.metadata().nodeShutdowns().get(state.nodes().getLocalNodeId());
        final long generation = shutdown == null ? Long.MIN_VALUE : shutdown.getStartedAtMillis();
        synchronized (sourceMemoLock) {
            if (sourceMemo != null && sourceMemoGeneration == generation) {
                return sourceMemo;
            }
            Response collected = new Response(generation, collectWarmVolumes(snapshotSearchableShards(indicesService)));
            sourceMemoGeneration = generation;
            sourceMemo = collected;
            return collected;
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

    static Map<Index, Map<Integer, Long>> collectWarmVolumes(List<IndexShard> shards) {
        return collectWarmVolumes(shards, TransportFetchShardWarmVolumesAction::tryEstimateShardWarmVolume);
    }

    // visible for testing
    static Map<Index, Map<Integer, Long>> collectWarmVolumes(List<IndexShard> shards, Function<IndexShard, OptionalLong> estimator) {
        Map<Index, Map<Integer, Long>> volumes = new HashMap<>();
        for (IndexShard shard : shards) {
            OptionalLong volume = estimator.apply(shard);
            if (volume.isEmpty()) {
                continue;
            }
            volumes.computeIfAbsent(shard.shardId().getIndex(), ignored -> new HashMap<>()).put(shard.shardId().id(), volume.getAsLong());
        }
        return volumes;
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

    public static class Request extends ActionRequest {

        private final String sourceNodeId;

        public Request(String sourceNodeId) {
            this.sourceNodeId = Objects.requireNonNull(sourceNodeId);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.sourceNodeId = in.readString();
        }

        public String sourceNodeId() {
            return sourceNodeId;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (sourceNodeId == null || sourceNodeId.isEmpty()) {
                validationException = addValidationError("sourceNodeId is missing", validationException);
            }
            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(sourceNodeId);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Request request = (Request) o;
            return Objects.equals(sourceNodeId, request.sourceNodeId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceNodeId);
        }
    }

    public static final class Response extends ActionResponse {

        private final long generationStartedAtMillis;
        private final Map<Index, Map<Integer, Long>> volumesByIndex;

        public Response(long generationStartedAtMillis, Map<Index, Map<Integer, Long>> volumesByIndex) {
            this.generationStartedAtMillis = generationStartedAtMillis;
            Map<Index, Map<Integer, Long>> copy = new HashMap<>(volumesByIndex.size());
            for (var entry : volumesByIndex.entrySet()) {
                copy.put(entry.getKey(), Map.copyOf(entry.getValue()));
            }
            this.volumesByIndex = Map.copyOf(copy);
        }

        public Response(StreamInput in) throws IOException {
            this.generationStartedAtMillis = in.readZLong();
            this.volumesByIndex = in.readMap(Index::new, i -> i.readMap(StreamInput::readVInt, StreamInput::readVLong));
        }

        public long generationStartedAtMillis() {
            return generationStartedAtMillis;
        }

        public Map<Index, Map<Integer, Long>> volumesByIndex() {
            return volumesByIndex;
        }

        public ShardWarmVolumes.Entry toEntry() {
            Map<ShardId, Long> volumes = new HashMap<>();
            for (var indexEntry : volumesByIndex.entrySet()) {
                for (var shardEntry : indexEntry.getValue().entrySet()) {
                    volumes.put(new ShardId(indexEntry.getKey(), shardEntry.getKey()), shardEntry.getValue());
                }
            }
            return new ShardWarmVolumes.Entry(generationStartedAtMillis, volumes);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeZLong(generationStartedAtMillis);
            out.writeMap(
                volumesByIndex,
                StreamOutput::writeWriteable,
                (o, shardMap) -> o.writeMap(shardMap, StreamOutput::writeVInt, StreamOutput::writeVLong)
            );
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Response response = (Response) o;
            return generationStartedAtMillis == response.generationStartedAtMillis
                && Objects.equals(volumesByIndex, response.volumesByIndex);
        }

        @Override
        public int hashCode() {
            return Objects.hash(generationStartedAtMillis, volumesByIndex);
        }
    }
}

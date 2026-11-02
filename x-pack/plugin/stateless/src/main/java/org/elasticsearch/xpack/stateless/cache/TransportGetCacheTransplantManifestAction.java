/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Returns the source node's resident blob-cache regions for one shard, highest LFU frequency first.
 * A missing or closed shard returns an empty manifest so the caller can fall back to object-store warming.
 * The source shard is relocating, so it is active without being in the started state.
 */
public class TransportGetCacheTransplantManifestAction extends HandledTransportAction<
    TransportGetCacheTransplantManifestAction.Request,
    TransportGetCacheTransplantManifestAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportGetCacheTransplantManifestAction.class);

    public static final String NAME = "internal:admin/stateless/cache_transplant/manifest";
    public static final ActionType<Response> TYPE = new ActionType<>(NAME);

    private final IndicesService indicesService;
    private final StatelessPlugin.SharedBlobCacheServiceSupplier cacheServiceSupplier;

    @Inject
    public TransportGetCacheTransplantManifestAction(
        TransportService transportService,
        ActionFilters actionFilters,
        IndicesService indicesService,
        StatelessPlugin.SharedBlobCacheServiceSupplier cacheServiceSupplier
    ) {
        super(
            NAME,
            false,
            transportService,
            actionFilters,
            Request::new,
            transportService.getThreadPool().executor(ThreadPool.Names.GENERIC)
        );
        this.indicesService = indicesService;
        this.cacheServiceSupplier = cacheServiceSupplier;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        if (shardStillLocal(request.shardId()) == false) {
            listener.onResponse(new Response(List.of()));
            return;
        }
        try {
            List<SharedBlobCacheService.ResidentRegion<FileCacheKey>> regions = cacheServiceSupplier.get()
                .snapshotResidentRegions(request.shardId());
            List<CacheTransplantRegionInfo> infos = new ArrayList<>(regions.size());
            for (SharedBlobCacheService.ResidentRegion<FileCacheKey> region : regions) {
                FileCacheKey key = region.key();
                infos.add(
                    new CacheTransplantRegionInfo(
                        key.primaryTerm(),
                        key.fileName(),
                        region.region(),
                        region.length(),
                        region.timestampMillis(),
                        region.frequency()
                    )
                );
            }
            listener.onResponse(new Response(infos));
        } catch (Exception e) {
            logger.debug("failed to snapshot resident cache regions for [{}]", request.shardId(), e);
            listener.onResponse(new Response(List.of()));
        }
    }

    private boolean shardStillLocal(ShardId shardId) {
        IndexService indexService = indicesService.indexService(shardId.getIndex());
        if (indexService == null) {
            return false;
        }
        IndexShard shard = indexService.getShardOrNull(shardId.id());
        return shard != null && shard.routingEntry().active();
    }

    public static final class Request extends ActionRequest {

        private final ShardId shardId;

        public Request(ShardId shardId) {
            this.shardId = shardId;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.shardId = new ShardId(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
        }

        public ShardId shardId() {
            return shardId;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static final class Response extends ActionResponse {

        private final List<CacheTransplantRegionInfo> regions;

        public Response(List<CacheTransplantRegionInfo> regions) {
            this.regions = regions;
        }

        public Response(StreamInput in) throws IOException {
            this.regions = in.readCollectionAsList(CacheTransplantRegionInfo::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(regions);
        }

        public List<CacheTransplantRegionInfo> regions() {
            return regions;
        }
    }
}

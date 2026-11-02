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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Copies the requested resident cache regions off the source node in one message. The source shard is relocating, so it stays active
 * until the target starts. A closed shard or a region evicted during the read is absent in the response, and the caller skips that region.
 */
public class TransportGetCacheTransplantRegionAction extends HandledTransportAction<
    TransportGetCacheTransplantRegionAction.Request,
    TransportGetCacheTransplantRegionAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportGetCacheTransplantRegionAction.class);

    public static final String NAME = "internal:admin/stateless/cache_transplant/region";
    public static final ActionType<Response> TYPE = new ActionType<>(NAME);

    /**
     * Most regions one message may carry. Each region is copied into its own buffer on the source, so an unbounded batch can exhaust the
     * heap. At the default 16MiB region size this is 256MiB.
     */
    static final int MAX_REGIONS_PER_REQUEST = 16;

    private final IndicesService indicesService;
    private final StatelessPlugin.SharedBlobCacheServiceSupplier cacheServiceSupplier;

    @Inject
    public TransportGetCacheTransplantRegionAction(
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
        if (request.regions().size() > MAX_REGIONS_PER_REQUEST) {
            listener.onFailure(
                new IllegalArgumentException(
                    "cache transplant region batch for ["
                        + request.shardId()
                        + "] has ["
                        + request.regions().size()
                        + "] regions, above the limit of ["
                        + MAX_REGIONS_PER_REQUEST
                        + "]"
                )
            );
            return;
        }
        List<CopiedRegion> copied = new ArrayList<>(request.regions().size());
        if (shardStillLocal(request.shardId()) == false) {
            for (int i = 0; i < request.regions().size(); i++) {
                copied.add(CopiedRegion.absent());
            }
            listener.onResponse(new Response(copied));
            return;
        }
        var cache = cacheServiceSupplier.get();
        int regionSize = cache.getRegionSize();
        for (RegionRef region : request.regions()) {
            copied.add(copyRegion(cache, regionSize, request.shardId(), region));
        }
        listener.onResponse(new Response(copied));
    }

    private CopiedRegion copyRegion(SharedBlobCacheService<FileCacheKey> cache, int regionSize, ShardId shardId, RegionRef region) {
        if (regionSize <= 0) {
            return CopiedRegion.absent();
        }
        try {
            byte[] raw = new byte[regionSize];
            ByteBuffer buffer = ByteBuffer.wrap(raw);
            FileCacheKey key = new FileCacheKey(shardId, region.primaryTerm(), region.blobName());
            SharedBlobCacheService.CopiedResidentRegion copied = cache.copyResidentRegion(key, region.region(), buffer);
            if (copied == null || copied.length() <= 0 || copied.length() > regionSize) {
                return CopiedRegion.absent();
            }
            return new CopiedRegion(true, copied.timestampMillis(), new BytesArray(raw, 0, copied.length()));
        } catch (Exception e) {
            logger.debug("failed to copy resident cache region [{}] for [{}]", region.region(), shardId, e);
            return CopiedRegion.absent();
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

    /**
     * One region the target wants copied. The source answers each entry in order.
     */
    public static final class RegionRef implements Writeable {

        private final long primaryTerm;
        private final String blobName;
        private final int region;

        public RegionRef(long primaryTerm, String blobName, int region) {
            this.primaryTerm = primaryTerm;
            this.blobName = blobName;
            this.region = region;
        }

        public RegionRef(StreamInput in) throws IOException {
            this.primaryTerm = in.readVLong();
            this.blobName = in.readString();
            this.region = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(primaryTerm);
            out.writeString(blobName);
            out.writeVInt(region);
        }

        public long primaryTerm() {
            return primaryTerm;
        }

        public String blobName() {
            return blobName;
        }

        public int region() {
            return region;
        }
    }

    public static final class Request extends ActionRequest {

        private final ShardId shardId;
        private final List<RegionRef> regions;

        public Request(ShardId shardId, List<RegionRef> regions) {
            this.shardId = shardId;
            this.regions = regions;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.shardId = new ShardId(in);
            this.regions = in.readCollectionAsList(RegionRef::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            out.writeCollection(regions);
        }

        public ShardId shardId() {
            return shardId;
        }

        public List<RegionRef> regions() {
            return regions;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    /**
     * Bytes of one requested region. {@link #present} is false when the source no longer has the region.
     */
    public static final class CopiedRegion implements Writeable {

        private final boolean present;
        private final long timestampMillis;
        @Nullable
        private final BytesReference bytes;

        public CopiedRegion(boolean present, long timestampMillis, @Nullable BytesReference bytes) {
            this.present = present;
            this.timestampMillis = timestampMillis;
            this.bytes = bytes;
        }

        public CopiedRegion(StreamInput in) throws IOException {
            this.present = in.readBoolean();
            if (present) {
                this.timestampMillis = in.readLong();
                this.bytes = in.readBytesReference();
            } else {
                this.timestampMillis = 0;
                this.bytes = null;
            }
        }

        public static CopiedRegion absent() {
            return new CopiedRegion(false, 0, null);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(present);
            if (present) {
                out.writeLong(timestampMillis);
                out.writeBytesReference(bytes);
            }
        }

        public boolean present() {
            return present;
        }

        public long timestampMillis() {
            return timestampMillis;
        }

        @Nullable
        public BytesReference bytes() {
            return bytes;
        }
    }

    public static final class Response extends ActionResponse {

        private final List<CopiedRegion> regions;

        public Response(List<CopiedRegion> regions) {
            this.regions = regions;
        }

        public Response(StreamInput in) throws IOException {
            this.regions = in.readCollectionAsList(CopiedRegion::new);
        }

        public static Response none() {
            return new Response(List.of());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(regions);
        }

        public List<CopiedRegion> regions() {
            return regions;
        }
    }
}

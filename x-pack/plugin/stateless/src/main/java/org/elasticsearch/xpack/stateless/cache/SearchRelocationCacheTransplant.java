/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.CachePopulationSource;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.RemoteTransportClient;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Copies a relocation source's resident blob-cache regions onto the target while the target shard is recovering.
 * Regions are requested highest LFU frequency first. Each transport message carries up to {@link #MAX_REGIONS_IN_FLIGHT_SETTING}
 * regions, and only one message is outstanding. The bytes are written with {@link SharedBlobCacheService#maybeFetchRegion} so the
 * first claim of a region wins. Once the recovery warming wait finishes, no further message is sent. A response already in flight
 * is still written into the cache. A source that cannot serve a region is skipped.
 */
public class SearchRelocationCacheTransplant {

    private static final Logger logger = LogManager.getLogger(SearchRelocationCacheTransplant.class);

    /**
     * Counts object-store warming starts that follow a transplant. Internal cluster tests read this; production code only increments it.
     */
    static final AtomicInteger objectStoreWarmingStarted = new AtomicInteger();

    public static final Setting<Boolean> ENABLED_SETTING = Setting.boolSetting(
        "stateless.search.relocation.cache_transplant.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public enum Ordering {
        PARALLEL,
        SERIAL
    }

    public static final Setting<Ordering> ORDERING_SETTING = Setting.enumSetting(
        Ordering.class,
        "stateless.search.relocation.cache_transplant.ordering",
        Ordering.PARALLEL,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Integer> MAX_REGIONS_IN_FLIGHT_SETTING = Setting.intSetting(
        "stateless.search.relocation.cache_transplant.max_regions_in_flight",
        1,
        1,
        TransportGetCacheTransplantRegionAction.MAX_REGIONS_PER_REQUEST,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Sends manifest and region requests to one source node. Implementations call the listener exactly once.
     */
    public interface Client {
        void manifest(DiscoveryNode node, ShardId shardId, ActionListener<TransportGetCacheTransplantManifestAction.Response> listener);

        void regions(
            DiscoveryNode node,
            TransportGetCacheTransplantRegionAction.Request request,
            ActionListener<TransportGetCacheTransplantRegionAction.Response> listener
        );
    }

    /**
     * Forwards requests on the cluster transport.
     */
    public static Client remoteClient(RemoteTransportClient remoteTransportClient, Executor responseExecutor) {
        return new Client() {
            @Override
            public void manifest(
                DiscoveryNode node,
                ShardId shardId,
                ActionListener<TransportGetCacheTransplantManifestAction.Response> listener
            ) {
                send(
                    remoteTransportClient,
                    responseExecutor,
                    node,
                    TransportGetCacheTransplantManifestAction.NAME,
                    new TransportGetCacheTransplantManifestAction.Request(shardId),
                    TransportGetCacheTransplantManifestAction.Response::new,
                    listener
                );
            }

            @Override
            public void regions(
                DiscoveryNode node,
                TransportGetCacheTransplantRegionAction.Request request,
                ActionListener<TransportGetCacheTransplantRegionAction.Response> listener
            ) {
                send(
                    remoteTransportClient,
                    responseExecutor,
                    node,
                    TransportGetCacheTransplantRegionAction.NAME,
                    request,
                    TransportGetCacheTransplantRegionAction.Response::new,
                    listener
                );
            }
        };
    }

    private static <R extends TransportResponse> void send(
        RemoteTransportClient remoteTransportClient,
        Executor responseExecutor,
        DiscoveryNode node,
        String action,
        TransportRequest request,
        Writeable.Reader<R> reader,
        ActionListener<R> listener
    ) {
        try {
            remoteTransportClient.sendRequest(
                node,
                action,
                request,
                new ActionListenerResponseHandler<>(listener, reader, responseExecutor)
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private final Client client;
    private final Function<String, DiscoveryNode> nodeResolver;
    private final StatelessSharedBlobCacheService cacheService;
    private final Executor fetchExecutor;

    private volatile boolean enabled;
    private volatile Ordering ordering;
    private volatile int maxRegionsInFlight;

    public SearchRelocationCacheTransplant(
        Client client,
        Function<String, DiscoveryNode> nodeResolver,
        StatelessSharedBlobCacheService cacheService,
        Executor fetchExecutor,
        ClusterSettings clusterSettings
    ) {
        this.client = client;
        this.nodeResolver = nodeResolver;
        this.cacheService = cacheService;
        this.fetchExecutor = fetchExecutor;
        clusterSettings.initializeAndWatch(ENABLED_SETTING, value -> this.enabled = value);
        clusterSettings.initializeAndWatch(ORDERING_SETTING, value -> this.ordering = value);
        clusterSettings.initializeAndWatch(MAX_REGIONS_IN_FLIGHT_SETTING, value -> this.maxRegionsInFlight = value);
    }

    public boolean isEnabled() {
        return enabled;
    }

    public Ordering ordering() {
        return ordering;
    }

    public int maxRegionsInFlight() {
        return maxRegionsInFlight;
    }

    /**
     * Chooses whether object-store warming runs beside the transplant or after it. {@code recoveryWaitFinished} is true once the
     * relocation warming wait has completed, including when it timed out. In {@link Ordering#SERIAL}, object-store warming starts only
     * when the transplant finishes while that wait is still open.
     */
    static void coordinate(
        ShardId shardId,
        Ordering ordering,
        BooleanSupplier recoveryWaitFinished,
        BiConsumer<BooleanSupplier, ActionListener<Void>> startTransplant,
        Consumer<ActionListener<Void>> startObjectStoreWarm,
        ActionListener<Void> warmCompletion
    ) {
        if (ordering == Ordering.SERIAL) {
            startTransplant.accept(recoveryWaitFinished, ActionListener.wrap(ignored -> {
                if (recoveryWaitFinished.getAsBoolean()) {
                    logger.debug("skipping object store warming after cache transplant for {}", shardId);
                    return;
                }
                startObjectStoreWarming(shardId, startObjectStoreWarm, warmCompletion);
            }, e -> {
                logger.debug("cache transplant failed for [{}], continuing with object store warming", shardId, e);
                if (recoveryWaitFinished.getAsBoolean() == false) {
                    startObjectStoreWarming(shardId, startObjectStoreWarm, warmCompletion);
                }
            }));
        } else {
            startTransplant.accept(recoveryWaitFinished, ActionListener.noop());
            startObjectStoreWarming(shardId, startObjectStoreWarm, warmCompletion);
        }
    }

    private static void startObjectStoreWarming(
        ShardId shardId,
        Consumer<ActionListener<Void>> startObjectStoreWarm,
        ActionListener<Void> warmCompletion
    ) {
        objectStoreWarmingStarted.incrementAndGet();
        logger.debug("starting object store warming after cache transplant for {}", shardId);
        startObjectStoreWarm.accept(warmCompletion);
    }

    /**
     * Copies resident regions from {@code sourceNodeId}. Completes {@code listener} when no further region requests will be issued.
     * A response already in flight is written even after {@code stopped} becomes true. Failures are logged and completed
     * successfully so recovery can continue with object-store warming.
     */
    public void run(ShardId shardId, String sourceNodeId, BooleanSupplier stopped, ActionListener<Void> listener) {
        if (enabled == false) {
            listener.onResponse(null);
            return;
        }
        DiscoveryNode source = nodeResolver.apply(sourceNodeId);
        if (source == null) {
            logger.debug("cache transplant skipped for {}: source node [{}] is not in the cluster state", shardId, sourceNodeId);
            listener.onResponse(null);
            return;
        }
        ActionListener<Void> failOpen = listener.delegateResponse((delegate, e) -> {
            logger.debug("cache transplant failed for [{}]", shardId, e);
            delegate.onResponse(null);
        });
        client.manifest(source, shardId, failOpen.delegateFailure((l, response) -> {
            try {
                if (stopped.getAsBoolean() || enabled == false) {
                    l.onResponse(null);
                    return;
                }
                fetchRegions(source, shardId, response.regions(), stopped, l);
            } catch (Exception e) {
                l.onFailure(e);
            }
        }));
    }

    private void fetchRegions(
        DiscoveryNode source,
        ShardId shardId,
        List<CacheTransplantRegionInfo> regions,
        BooleanSupplier stopped,
        ActionListener<Void> listener
    ) {
        if (regions.isEmpty() || enabled == false || stopped.getAsBoolean()) {
            listener.onResponse(null);
            return;
        }
        new Fetch(source, shardId, regions, stopped, listener).pump();
    }

    private void writeRegion(ShardId shardId, CacheTransplantRegionInfo info, BytesReference bytes, long timestampMillis) {
        int length = bytes.length();
        if (length <= 0) {
            return;
        }
        long blobLength = (long) info.region() * cacheService.getRegionSize() + length;
        FileCacheKey key = new FileCacheKey(shardId, info.primaryTerm(), info.blobName());
        cacheService.maybeFetchRegion(
            key,
            info.region(),
            blobLength,
            (channel, channelPos, streamFactory, relativePos, gapLength, progressUpdater, completionListener) -> {
                try {
                    if (relativePos < 0 || gapLength < 0 || relativePos > bytes.length() || gapLength > bytes.length() - relativePos) {
                        throw new IOException(
                            "cache transplant region "
                                + info.region()
                                + " for "
                                + shardId
                                + " has ["
                                + bytes.length()
                                + "] bytes, cannot fill ["
                                + gapLength
                                + "] from ["
                                + relativePos
                                + "]"
                        );
                    }
                    long started = System.nanoTime();
                    BytesReference slice = bytes.slice(relativePos, gapLength);
                    BytesRefIterator iterator = slice.iterator();
                    int position = channelPos;
                    BytesRef ref;
                    while ((ref = iterator.next()) != null) {
                        ByteBuffer src = ByteBuffer.wrap(ref.bytes, ref.offset, ref.length);
                        while (src.hasRemaining()) {
                            int written = channel.write(src, position);
                            if (written <= 0) {
                                throw new IOException("cache transplant wrote no bytes for " + shardId + " region " + info.region());
                            }
                            position += written;
                            progressUpdater.accept(written);
                        }
                    }
                    cacheService.getBlobCacheMetrics()
                        .recordCachePopulationMetrics(
                            info.blobName(),
                            gapLength,
                            System.nanoTime() - started,
                            BlobCacheMetrics.CachePopulationReason.Warming,
                            CachePopulationSource.Peer
                        );
                    completionListener.onResponse(null);
                } catch (Exception e) {
                    completionListener.onFailure(e instanceof IOException ioe ? ioe : new IOException(e));
                }
            },
            fetchExecutor,
            timestampMillis,
            ActionListener.wrap(fetched -> {
                if (fetched) {
                    logger.trace("transplanted region {} of {} for {}", info.region(), info.blobName(), shardId);
                }
            }, e -> logger.debug("failed to write transplanted region [{}] for [{}]", info.region(), shardId, e))
        );
    }

    private final class Fetch {
        private final DiscoveryNode source;
        private final ShardId shardId;
        private final List<CacheTransplantRegionInfo> regions;
        private final BooleanSupplier stopped;
        private final ActionListener<Void> listener;
        private final AtomicBoolean done = new AtomicBoolean();
        private int next;
        private boolean inFlight;

        private Fetch(
            DiscoveryNode source,
            ShardId shardId,
            List<CacheTransplantRegionInfo> regions,
            BooleanSupplier stopped,
            ActionListener<Void> listener
        ) {
            this.source = source;
            this.shardId = shardId;
            this.regions = regions;
            this.stopped = stopped;
            this.listener = listener;
        }

        private void pump() {
            final List<CacheTransplantRegionInfo> batch;
            synchronized (this) {
                if (done.get() || inFlight) {
                    return;
                }
                boolean stopIssuing = stopped.getAsBoolean() || enabled == false || next >= regions.size();
                if (stopIssuing) {
                    if (done.compareAndSet(false, true)) {
                        batch = null;
                    } else {
                        return;
                    }
                } else {
                    int count = Math.min(maxRegionsInFlight, regions.size() - next);
                    batch = List.copyOf(regions.subList(next, next + count));
                    next += count;
                    inFlight = true;
                }
            }
            if (batch == null) {
                listener.onResponse(null);
                return;
            }
            List<TransportGetCacheTransplantRegionAction.RegionRef> refs = new ArrayList<>(batch.size());
            for (CacheTransplantRegionInfo region : batch) {
                refs.add(new TransportGetCacheTransplantRegionAction.RegionRef(region.primaryTerm(), region.blobName(), region.region()));
            }
            try {
                client.regions(source, new TransportGetCacheTransplantRegionAction.Request(shardId, refs), ActionListener.wrap(response -> {
                    try {
                        applyResponse(batch, response);
                    } finally {
                        onBatchDone();
                    }
                }, e -> {
                    logger.debug("cache transplant region batch failed for [{}]", shardId, e);
                    onBatchDone();
                }));
            } catch (Exception e) {
                logger.debug("failed to request cache transplant regions for [{}]", shardId, e);
                onBatchDone();
            }
        }

        private void applyResponse(List<CacheTransplantRegionInfo> batch, TransportGetCacheTransplantRegionAction.Response response) {
            if (response.regions() == null) {
                return;
            }
            int count = Math.min(batch.size(), response.regions().size());
            for (int i = 0; i < count; i++) {
                TransportGetCacheTransplantRegionAction.CopiedRegion copied = response.regions().get(i);
                if (copied.present() && copied.bytes() != null) {
                    writeRegion(shardId, batch.get(i), copied.bytes(), copied.timestampMillis());
                }
            }
        }

        private void onBatchDone() {
            synchronized (this) {
                inFlight = false;
            }
            pump();
        }
    }
}

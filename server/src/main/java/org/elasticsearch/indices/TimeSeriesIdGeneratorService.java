/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import com.carrotsearch.hppc.cursors.ObjectCursor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.TimeSeriesIdGenerator;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.node.Node;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

/**
 * Looks up the {@link TimeSeriesIdGenerator} for an index. If the index is
 * local we read from the local metadata. If the index isn't local we parse
 * the mapping, read it, and cache it.
 */
class TimeSeriesIdGeneratorService extends AbstractLifecycleComponent
    implements
        ClusterStateApplier,
        Function<IndexMetadata, TimeSeriesIdGenerator> {
    private static final Logger logger = LogManager.getLogger(TimeSeriesIdGeneratorService.class);

    public interface LocalIndex {
        long metadataVersion();

        TimeSeriesIdGenerator generator();
    }

    private final Function<Index, LocalIndex> lookupLocalIndex;
    private final Function<IndexMetadata, TimeSeriesIdGenerator> buildTimeSeriedIdGenerator;
    private final ExecutorService executor; // single thread to construct mapper services async as needed
    private final Map<Index, Value> byIndex = ConcurrentCollections.newConcurrentMap();

    static TimeSeriesIdGeneratorService build(Settings nodeSettings, ThreadPool threadPool, IndicesService indicesService) {
        String nodeName = Objects.requireNonNull(Node.NODE_NAME_SETTING.get(nodeSettings));
        String threadName = String.format(Locale.ROOT, "%s/%s#updateTask", nodeName, TimeSeriesIdGeneratorService.class.getSimpleName());
        ExecutorService executor = EsExecutors.newScaling(
            threadName,
            0,
            1,
            0,
            TimeUnit.MILLISECONDS,
            daemonThreadFactory(nodeName, threadName),
            threadPool.getThreadContext()
        );

        Function<Index, LocalIndex> lookupLocalIndex = index -> {
            IndexService local = indicesService.indexService(index);
            return local == null ? null : new LocalIndex() {
                @Override
                public long metadataVersion() {
                    return local.getMetadata().getVersion();
                }

                @Override
                public TimeSeriesIdGenerator generator() {
                    return local.mapperService().mappingLookup().getMapping().getTimeSeriesIdGenerator();
                }
            };
        };

        Function<IndexMetadata, TimeSeriesIdGenerator> buildTimeSeriedIdGenerator = indexMetadata -> {
            ClusterApplierService.assertNotClusterStateUpdateThread("decompressed the mapping of many indices");
            try {
                try (MapperService tmp = indicesService.createIndexMapperService(indexMetadata)) {
                    tmp.merge(indexMetadata, MapperService.MergeReason.MAPPING_RECOVERY);
                    TimeSeriesIdGenerator gen = tmp.mappingLookup().getMapping().getTimeSeriesIdGenerator();
                    logger.trace("computed timeseries id generator for {}", indexMetadata.getIndex());
                    return gen;
                }
            } catch (IOException e) {
                // Whatever happened here is unrecoverable and likely a bug so IllegalStateException which'll turn into HTTP 500
                throw new IllegalStateException("error building time series id generator: " + e.getMessage(), e);
            }
        };

        return new TimeSeriesIdGeneratorService(executor, lookupLocalIndex, buildTimeSeriedIdGenerator);
    }

    TimeSeriesIdGeneratorService(
        ExecutorService executor,
        Function<Index, LocalIndex> lookupLocalIndex,
        Function<IndexMetadata, TimeSeriesIdGenerator> buildTimeSeriedIdGenerator
    ) {
        this.executor = executor;
        this.lookupLocalIndex = lookupLocalIndex;
        this.buildTimeSeriedIdGenerator = buildTimeSeriedIdGenerator;
    }

    @Override
    protected void doStart() {}

    @Override
    protected void doStop() {
        ThreadPool.terminate(executor, 10, TimeUnit.SECONDS);
    }

    @Override
    protected void doClose() {}

    @Override
    public TimeSeriesIdGenerator apply(IndexMetadata meta) {
        if (false == meta.inTimeSeriesMode()) {
            return null;
        }
        Value v = byIndex.get(meta.getIndex());
        /*
         * v is rebuilt in applyClusterState which should have happened-before
         * whatever made meta available to the rest of the system. So the if
         * statement below really shouldn't fail.
         */
        if (meta.getMappingVersion() > v.mappingVersion) {
            throw new IllegalStateException(
                "Got a newer version of the index than the time series id generator ["
                    + meta.getMappingVersion()
                    + "] vs ["
                    + v.mappingVersion
                    + "]"
            );
        }
        /*
         * Because TimeSeriesIdGenerators only "get bigger" it should be safe
         * to use whatever is in the map, even if it is for a newer version of
         * index.
         */
        return v.generator();
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        applyClusterState(event.state().metadata());
    }

    void applyClusterState(Metadata metadata) {
        /*
         * Update the "byIndex" map containing the generators in three phases:
         * 1. Remove any deletes indices.
         * 2. Update any indices hosted on this node or who's mapping hasn't
         *    changed.
         * 3. Update remaining indices. These are slower but we can reuse any
         *    generators built for indices with the same mapping.
         */
        byIndex.keySet().removeIf(index -> metadata.index(index) == null);

        Map<DedupeKey, Value> dedupe = new HashMap<>();

        for (ObjectCursor<IndexMetadata> cursor : metadata.indices().values()) {
            IndexMetadata indexMetadata = cursor.value;
            if (false == indexMetadata.inTimeSeriesMode()) {
                continue;
            }
            Index index = indexMetadata.getIndex();

            if (indexMetadata.mapping() == null) {
                byIndex.put(index, new PreBuiltValue(indexMetadata.getMappingVersion(), TimeSeriesIdGenerator.EMPTY));
                continue;
            }

            DedupeKey key = new DedupeKey(indexMetadata);

            /*
             * Find indices who's mapping hasn't changed.
             */
            Value old = byIndex.get(index);
            if (old != null && old.mappingVersion == indexMetadata.getMappingVersion()) {
                logger.trace("reusing previous timeseries id generator for {}", index);
                dedupe.put(key, old);
                continue;
            }

            /*
             * Check if the mapping is the same as something we've already seen.
             */
            Value value = dedupe.get(key);
            if (value != null) {
                logger.trace("reusing timeseries id from another index for {}", index);
                byIndex.put(index, value.withMappingVersion(indexMetadata.getMappingVersion()));
                continue;
            }

            /*
             * Find indices that we're hosting locally. In production this
             * looks up against IndicesService which is a "high priority"
             * update consumer so it's cluster state updates
             * "happen-before" this one.
             */
            LocalIndex localIndex = lookupLocalIndex.apply(index);
            if (localIndex == null) {
                logger.trace("timeseries id for {} is not available locally", index);
                continue;
            }
            logger.trace("computing timeseries id generator for {} using local index service", index);
            if (localIndex.metadataVersion() < indexMetadata.getVersion()) {
                throw new IllegalStateException(
                    "Trying to update timeseries id with an older version of the metadata ["
                        + localIndex.metadataVersion()
                        + "] vs ["
                        + indexMetadata.getVersion()
                        + "]"
                );
            }
            value = new PreBuiltValue(indexMetadata.getMappingVersion(), localIndex.generator());
            byIndex.put(index, value);
            dedupe.put(key, value);
        }

        /*
         * Update the remaining indices.
         */
        for (ObjectCursor<IndexMetadata> cursor : metadata.indices().values()) {
            IndexMetadata indexMetadata = cursor.value;
            if (false == indexMetadata.inTimeSeriesMode()) {
                continue;
            }
            Index index = indexMetadata.getIndex();

            Value old = byIndex.get(index);
            if (old != null && old.mappingVersion == indexMetadata.getMappingVersion()) {
                // We already updated the generator in the first pass
                continue;
            }

            DedupeKey key = new DedupeKey(indexMetadata);
            Value value = dedupe.get(key);
            if (value == null) {
                logger.trace("computing timeseries id generator for {} async", index);
                value = new AsyncValue(indexMetadata.getMappingVersion(), buildTimeSeriedIdGenerator, executor, indexMetadata);
            } else {
                logger.trace("reusing timeseries id from another index for {}", index);
                value = value.withMappingVersion(indexMetadata.getMappingVersion());
            }
            byIndex.put(index, value);
        }
    }

    private abstract static class Value {
        private final long mappingVersion;

        protected Value(long mappingVersion) {
            this.mappingVersion = mappingVersion;
        }

        abstract TimeSeriesIdGenerator generator();

        abstract Value withMappingVersion(long newMappingVersion);
    }

    private static class PreBuiltValue extends Value {
        private final TimeSeriesIdGenerator generator;

        PreBuiltValue(long mappingVersion, TimeSeriesIdGenerator generator) {
            super(mappingVersion);
            this.generator = generator;
        }

        @Override
        TimeSeriesIdGenerator generator() {
            return generator;
        }

        @Override
        Value withMappingVersion(long newMappingVersion) {
            return new PreBuiltValue(newMappingVersion, generator);
        }
    }

    /**
     * Build the {@link TimeSeriesIdGenerator} async from the cluster state
     * update thread. Creating this will queue a task to build the generator
     * on the separate thread but return immediately. Callers to
     * {@link #generator()} race that queued task. If they win they will
     * build the {@link TimeSeriesIdGenerator} and if they lose they'll return
     * a cached copy.
     */
    private static class AsyncValue extends Value {
        private final LazyInitializable<TimeSeriesIdGenerator, RuntimeException> lazy;

        private AsyncValue(long mappingVersion, LazyInitializable<TimeSeriesIdGenerator, RuntimeException> lazy) {
            super(mappingVersion);
            this.lazy = lazy;
        }

        AsyncValue(
            long mappingVersion,
            Function<IndexMetadata, TimeSeriesIdGenerator> buildTimeSeriesIdGenerator,
            ExecutorService executor,
            IndexMetadata indexMetadata
        ) {
            /*
             * This closes over indexMetadata and keeps a reference to it
             * for as long as the AsyncValue lives which is ok. It isn't the
             * only thing with such a reference.
             */
            this(mappingVersion, new LazyInitializable<>(() -> buildTimeSeriesIdGenerator.apply(indexMetadata)));
            executor.execute(new AbstractRunnable() {
                @Override
                protected void doRun() throws Exception {
                    generator();
                }

                @Override
                public void onFailure(Exception e) {
                    /*
                     * We failed to build the time series id generator which sad,
                     * but we don't have to make a ton of noise about it because
                     * when someone goes to use it they'll attempt to build it
                     * again. If *they* fail then it'll throw an exception to
                     * the caller which'll get reported back over http.
                     */
                    logger.debug(
                        new ParameterizedMessage("error building timeseries id generator for {} async", indexMetadata.getIndex()),
                        e
                    );
                }
            });
        }

        @Override
        TimeSeriesIdGenerator generator() {
            return lazy.getOrCompute();
        }

        @Override
        Value withMappingVersion(long newMappingVersion) {
            return new AsyncValue(newMappingVersion, lazy);
        }
    }

    /**
     * Key for deduplicating mappings. In an ideal world we'd just use the
     * mapping's {@link CompressedXContent} but {@link CompressedXContent#equals(Object)}
     * will try to decompress the mapping if the crc matches but the compressed bytes
     * don't. That's wasteful for us - probably for everyone. If the crc and compressed
     * bytes match that's a match.
     */
    private static class DedupeKey {  // TODO Just use CompressedXContent and remove unzipping
        private final CompressedXContent mapping;

        DedupeKey(IndexMetadata meta) {
            this.mapping = meta.mapping().source();
        }

        @Override
        public int hashCode() {
            return mapping.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            DedupeKey other = (DedupeKey) obj;
            return mapping.hashCode() == other.mapping.hashCode() && Arrays.equals(mapping.compressed(), other.mapping.compressed());
        }
    }
}

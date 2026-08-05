/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.index.DirectoryReader;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.AbstractScriptFieldType;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.xpack.esql.plugin.ShardResultCacheKey.QueryPart;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * The ES|QL side of the shard request cache: turns a node request into a per-shard key, probes, replays a hit, and
 * stores a miss that is eligible.
 * <p>
 * The cache itself is {@link org.elasticsearch.indices.IndicesRequestCache}, reused rather than rebuilt. It already
 * provides reader-close invalidation, shard-clear, periodic cleanup, size accounting and eviction, and the
 * {@code request_cache} statistics. ES|QL and DSL entries therefore share one memory budget and one set of counters,
 * which is deliberate: they serve the same purpose, and {@code _cache/clear?request=true} clearing both is the
 * behavior an operator expects.
 */
final class ShardResultCache {

    private static final Logger logger = LogManager.getLogger(ShardResultCache.class);

    private final IndicesService indicesService;
    private final ClusterSettings clusterSettings;
    private final BlockFactory blockFactory;

    ShardResultCache(IndicesService indicesService, ClusterSettings clusterSettings, BlockFactory blockFactory) {
        this.indicesService = indicesService;
        this.clusterSettings = clusterSettings;
        this.blockFactory = blockFactory;
    }

    /**
     * Everything one shard needs to be served from, or stored into, the cache. The reader and the mapping key are
     * captured at probe time rather than looked up again at store time: a refresh during execution would otherwise let
     * a value computed against one reader be stored under a key naming another.
     */
    record ShardProbe(
        BytesReference key,
        IndexShard shard,
        MappingLookup.CacheKey mappingCacheKey,
        DirectoryReader reader,
        @Nullable BytesReference hit
    ) {
        boolean isHit() {
            return hit != null;
        }
    }

    /**
     * The per-request settings snapshot. Read once per node request so that a dynamic update cannot change the answer
     * halfway through one query.
     */
    ShardResultCacheSettings settings() {
        return new ShardResultCacheSettings(clusterSettings);
    }

    /**
     * The part of the key shared by every shard of this request, or {@code null} when the request is not cacheable at
     * all. Logs the reason at debug so a disappointing hit rate can be explained.
     */
    @Nullable
    QueryPart queryPart(DataNodeRequest request, EsqlFlags flags) {
        String reason = ShardResultCacheVerifier.notCacheableReason(request);
        if (reason != null) {
            logger.debug("query is not cacheable: {}", reason);
            return null;
        }
        try {
            return ShardResultCacheKey.queryPart(request, flags);
        } catch (Exception e) {
            logger.debug("failed to compute a shard result cache key", e);
            return null;
        }
    }

    /**
     * Looks one shard up.
     *
     * @return the probe, or {@code null} when this shard cannot participate - the key could not be completed, or the
     *         shard has no usable reader
     */
    @Nullable
    ShardProbe probe(QueryPart queryPart, DataNodeRequest.Shard shard, SearchContext searchContext) {
        // The same per-index opt-out the DSL path honors in IndicesService.canCache. An operator who turned the request
        // cache off for an index means it for every reader of that index.
        if (searchContext.indexShard().indexSettings().isRequestCacheEnabled() == false) {
            return null;
        }
        String nonDeterministicField = nonDeterministicField(queryPart.fieldNames(), searchContext.getSearchExecutionContext());
        if (nonDeterministicField != null) {
            logger.debug("query is not cacheable on shard [{}]: non-deterministic field [{}]", shard.shardId(), nonDeterministicField);
            return null;
        }
        try {
            CheckedBiConsumer<ShardSearchRequest, StreamOutput, IOException> differentiator = indicesService
                .requestCacheKeyDifferentiator();
            BytesReference key = ShardResultCacheKey.forShard(queryPart, shard, searchContext, differentiator);
            if (key == null) {
                return null;
            }
            IndexShard indexShard = searchContext.indexShard();
            MappingLookup.CacheKey mappingCacheKey = searchContext.getSearchExecutionContext().mappingCacheKey();
            DirectoryReader reader = searchContext.searcher().getDirectoryReader();
            BytesReference hit = indicesService.getCachedShardLevelResult(indexShard, mappingCacheKey, reader, key);
            logger.trace(
                () -> Strings.format(
                    "shard result cache %s on shard [%s] key [%s]",
                    hit == null ? "miss" : "hit",
                    indexShard.shardId(),
                    MessageDigests.toHexString(BytesReference.toBytes(key))
                )
            );
            return new ShardProbe(key, indexShard, mappingCacheKey, reader, hit);
        } catch (Exception e) {
            logger.debug("failed to probe the shard result cache", e);
            return null;
        }
    }

    /**
     * A runtime field whose script does not return the same value twice makes a shard's rows a function of when they
     * were read. {@link SearchExecutionContext#isCacheable()} reports that too, but only once something has built a
     * query against the field; ES|QL routinely reads a field's values without ever querying it, so the mapping is
     * asked directly here.
     *
     * @return the name of the first such field, or {@code null} when every field the plan reads is stable on this shard
     */
    @Nullable
    private static String nonDeterministicField(Set<String> fieldNames, SearchExecutionContext context) {
        for (String fieldName : fieldNames) {
            if (context.isFieldMapped(fieldName) == false) {
                continue;
            }
            if (context.getFieldType(fieldName) instanceof AbstractScriptFieldType<?> scriptField
                && scriptField.isResultDeterministic() == false) {
                return fieldName;
            }
        }
        return null;
    }

    /**
     * Deserializes a hit. The blocks are accounted against the query's circuit breaker exactly as blocks arriving over
     * the exchange are, while the cached bytes themselves stay accounted by the cache's own weigher.
     */
    List<Page> replay(BytesReference value) throws IOException {
        List<Page> pages = new ArrayList<>();
        boolean success = false;
        try (StreamInput in = value.streamInput()) {
            in.setTransportVersion(TransportVersion.current());
            try (BlockStreamInput blockStreamInput = new BlockStreamInput(in, blockFactory)) {
                int pageCount = blockStreamInput.readVInt();
                for (int i = 0; i < pageCount; i++) {
                    pages.add(new Page(blockStreamInput));
                }
            }
            success = true;
            return pages;
        } finally {
            if (success == false) {
                for (Page page : pages) {
                    page.releaseBlocks();
                }
            }
        }
    }

    /**
     * Whether this shard looks static enough to be worth an entry. Invalidation is reader-close driven and readers only
     * turn over when there is something new to expose, so a shard that has not been written to recently is one whose
     * entries survive. A hot write shard would pay the serialization and evict useful entries to hold something that
     * dies before it is read again.
     * <p>
     * The check uses two mechanisms. First, if the local checkpoint has not advanced past the committed
     * {@code max_seq_no} the shard has had no writes since the last flush and is admitted immediately regardless of
     * {@code Engine.lastWriteNanos}. This matters because {@code lastWriteNanos} is initialized to
     * {@code System.nanoTime()} at engine construction, so a shard just closed and reopened (for example by a
     * {@code _cache/clear} operation) looks "just written" for up to {@code minIdleNanos} even with no indexing
     * activity. Second, when uncommitted writes do exist, {@code lastWriteNanos} gates on the configured idle window.
     * <p>
     * Advisory, and separately switchable, because it does not hold on a stateless search-tier shard: there
     * {@code index()} throws, so the last write time never advances even while the index tier is busy. That wastes work
     * rather than returning wrong rows.
     */
    boolean admits(IndexShard shard, ShardResultCacheSettings settings) {
        long minIdleNanos = settings.minShardIdleTimeNanos();
        if (minIdleNanos == 0) {
            return true;
        }
        // Read the local checkpoint before entering the engine block; it comes from the replication
        // tracker which is independent of the engine lifecycle.
        long localCheckpoint = shard.getLocalCheckpoint();
        return Boolean.TRUE.equals(shard.tryWithEngineOrNull(engine -> {
            if (engine == null) {
                return false;
            }
            // If the local checkpoint has not advanced past the committed max_seq_no, no real writes
            // have occurred since the last flush — admit immediately rather than relying on
            // lastWriteNanos, which is reset to now at engine construction.
            String committedMaxSeqNoStr = engine.commitStats().getUserData().get(SequenceNumbers.MAX_SEQ_NO);
            if (committedMaxSeqNoStr != null && localCheckpoint <= Long.parseLong(committedMaxSeqNoStr)) {
                return true;
            }
            return System.nanoTime() - engine.getLastWriteNanos() >= minIdleNanos;
        }));
    }

    void store(ShardProbe probe, BytesReference value) {
        try {
            indicesService.putShardLevelResult(probe.shard(), probe.mappingCacheKey(), probe.reader(), probe.key(), value);
            logger.trace(
                () -> Strings.format(
                    "stored [%d] bytes for shard [%s] key [%s]",
                    value.length(),
                    probe.shard().shardId(),
                    MessageDigests.toHexString(BytesReference.toBytes(probe.key()))
                )
            );
        } catch (Exception e) {
            logger.debug("failed to store a shard result cache entry", e);
        }
    }
}

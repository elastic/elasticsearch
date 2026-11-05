/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.index;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponse;
import org.elasticsearch.action.fieldcaps.IndexFieldStats;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.IndexMode;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Per-concrete-index properties collected at field-caps / index-resolution time.
 * <p>
 * {@link #numberOfShards} is the total primary shard count for the index.
 * A value of {@code 0} means the count was not available (e.g. the coordinating
 * node is on a transport version that pre-dates shard-count propagation, or the
 * index type does not report shard counts). Consumers must treat {@code 0} as
 * "unknown".
 * </p>
 * <p>
 * {@link #docCount} and {@link #sizeInBytes} estimate the live documents and segment bytes of the whole index, or are
 * {@code -1} when unknown. Field caps samples one shard copy per index, so they are that shard's values times
 * {@link #numberOfShards}: skewed routing makes them rough. Field caps may also answer from its coordinator cache, which
 * is only invalidated by mapping or settings changes, so they can lag behind recent indexing. They are planning hints
 * for the cost-based optimizer, never exact counts.
 * </p>
 * <p>
 * {@link #fieldStats} are the per-field stats of that sampled shard, extrapolated the same way and keyed by field name.
 * Only the coordinator plans with them, so they are not serialized: a deserialized instance has none. For the same
 * reason they are left out of {@link #equals} and {@link #hashCode}, so a plan equals its serialization round trip.
 * </p>
 */
public record IndexProperties(
    IndexMode indexMode,
    int numberOfShards,
    long docCount,
    long sizeInBytes,
    Map<String, IndexFieldStats> fieldStats
) implements Writeable {

    /**
     * Transport version that introduced per-index shard-count propagation via {@code _field_caps}.
     * Gating point for both {@link #IndexProperties(StreamInput)} and {@link #writeTo(StreamOutput)}.
     */
    public static final TransportVersion SHARD_COUNTS = TransportVersion.fromName("field_caps_number_of_shards");

    /**
     * Transport version that introduced per-index doc stats via {@code _field_caps}.
     */
    public static final TransportVersion DOC_STATS = FieldCapabilitiesIndexResponse.SHARD_DOC_STATS_VERSION;

    public IndexProperties(IndexMode indexMode, int numberOfShards) {
        this(indexMode, numberOfShards, -1, -1);
    }

    public IndexProperties(IndexMode indexMode, int numberOfShards, long docCount, long sizeInBytes) {
        this(indexMode, numberOfShards, docCount, sizeInBytes, Map.of());
    }

    public IndexProperties {
        fieldStats = Map.copyOf(fieldStats);
    }

    /**
     * Builds the properties from the stats of the single shard copy that field caps sampled, extrapolating them to the
     * whole index. Stays unknown if the shard did not report stats or the shard count is unknown.
     */
    public static IndexProperties fromShardSample(
        IndexMode indexMode,
        int numberOfShards,
        long shardDocCount,
        long shardSizeInBytes,
        Map<String, IndexFieldStats> shardFieldStats
    ) {
        Map<String, IndexFieldStats> fieldStats = Map.of();
        if (numberOfShards > 0 && shardFieldStats.isEmpty() == false) {
            fieldStats = new HashMap<>(shardFieldStats.size());
            for (Map.Entry<String, IndexFieldStats> e : shardFieldStats.entrySet()) {
                fieldStats.put(e.getKey(), e.getValue().scale(numberOfShards));
            }
        }
        return new IndexProperties(
            indexMode,
            numberOfShards,
            extrapolate(shardDocCount, numberOfShards),
            extrapolate(shardSizeInBytes, numberOfShards),
            fieldStats
        );
    }

    private static long extrapolate(long shardValue, int numberOfShards) {
        if (shardValue < 0 || numberOfShards <= 0) {
            return -1;
        }
        try {
            return Math.multiplyExact(shardValue, numberOfShards);
        } catch (ArithmeticException e) {
            return -1;
        }
    }

    public IndexProperties(StreamInput in) throws IOException {
        this(
            IndexMode.readFrom(in),
            in.getTransportVersion().supports(SHARD_COUNTS) ? in.readVInt() : 0,
            in.getTransportVersion().supports(DOC_STATS) ? in.readZLong() : -1,
            in.getTransportVersion().supports(DOC_STATS) ? in.readZLong() : -1
        );
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof IndexProperties that
            && indexMode == that.indexMode
            && numberOfShards == that.numberOfShards
            && docCount == that.docCount
            && sizeInBytes == that.sizeInBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexMode, numberOfShards, docCount, sizeInBytes);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        IndexMode.writeTo(indexMode, out);
        if (out.getTransportVersion().supports(SHARD_COUNTS)) {
            out.writeVInt(numberOfShards);
        }
        if (out.getTransportVersion().supports(DOC_STATS)) {
            out.writeZLong(docCount);
            out.writeZLong(sizeInBytes);
        }
    }
}

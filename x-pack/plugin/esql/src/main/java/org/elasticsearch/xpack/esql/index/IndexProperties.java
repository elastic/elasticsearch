/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.index;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.IndexMode;

import java.io.IOException;

/**
 * Per-concrete-index properties collected at field-caps / index-resolution time.
 * <p>
 * {@link #numberOfShards} is the total primary shard count for the index.
 * A value of {@code 0} means the count was not available (e.g. the coordinating
 * node is on a transport version that pre-dates shard-count propagation, or the
 * index type does not report shard counts). Consumers must treat {@code 0} as
 * "unknown".
 * </p>
 */
public record IndexProperties(IndexMode indexMode, int numberOfShards) implements Writeable {

    /**
     * Transport version that introduced per-index shard-count propagation via {@code _field_caps}.
     * Gating point for both {@link #IndexProperties(StreamInput)} and {@link #writeTo(StreamOutput)}.
     */
    public static final TransportVersion SHARD_COUNTS = TransportVersion.fromName("field_caps_number_of_shards");

    public IndexProperties(StreamInput in) throws IOException {
        this(IndexMode.readFrom(in), in.getTransportVersion().supports(SHARD_COUNTS) ? in.readVInt() : 0);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        IndexMode.writeTo(indexMode, out);
        if (out.getTransportVersion().supports(SHARD_COUNTS)) {
            out.writeVInt(numberOfShards);
        }
    }
}

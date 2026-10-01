/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.dedup;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * Identity of a query for the purpose of counting repeats: the same vector, searched on the same field,
 * with the same filters. Runtime parameters such as k or num_candidates are deliberately left out, they
 * describe how the query was run and not which query it is.
 * <p>
 * The identity is a 128-bit hash, wide enough that a collision among the queries seen in a day is not a
 * practical concern.
 */
public record QueryFingerprint(long high, long low) {

    private static final long SEED = 0;

    public static QueryFingerprint of(CapturedQuery query) {
        byte[] field = query.field().getBytes(StandardCharsets.UTF_8);
        // This is considering that filters are combined with AND, so their order is not part of the query's identity
        List<byte[]> filters = query.filters().stream().map(QueryFingerprint::canonical).sorted(Arrays::compareUnsigned).toList();

        int size = Integer.BYTES + field.length + Integer.BYTES + Float.BYTES * query.queryVector().length;
        for (byte[] filter : filters) {
            size += Integer.BYTES + filter.length;
        }
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.putInt(field.length).put(field);
        buffer.putInt(query.queryVector().length);
        for (float value : query.queryVector()) {
            buffer.putFloat(value);
        }
        for (byte[] filter : filters) {
            buffer.putInt(filter.length).put(filter);
        }

        MurmurHash3.Hash128 hash = MurmurHash3.hash128(buffer.array(), 0, size, SEED, new MurmurHash3.Hash128());
        return new QueryFingerprint(hash.h2, hash.h1);
    }

    private static byte[] canonical(QueryBuilder filter) {
        return Strings.toString(filter).getBytes(StandardCharsets.UTF_8);
    }
}

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
import java.util.HexFormat;
import java.util.List;

/**
 * Identity of a query for counting repeats: vector, field, indices, filters and the parameters that decide result quality
 * (k, candidates, visit percentage, oversample), since other values give another recall. A 128-bit hash.
 */
public record QueryFingerprint(long high, long low) {

    private static final long SEED = 0;

    public static QueryFingerprint of(CapturedQuery query) {
        // the same vector searched in other indices has other results and other ground truth
        List<byte[]> indices = Arrays.stream(query.indices()).sorted().map(index -> index.getBytes(StandardCharsets.UTF_8)).toList();
        byte[] field = query.field().getBytes(StandardCharsets.UTF_8);
        // filters are ANDed, so their order is not part of the identity
        List<byte[]> filters = query.filters().stream().map(QueryFingerprint::canonical).sorted(Arrays::compareUnsigned).toList();

        int size = Integer.BYTES + Integer.BYTES + field.length + Integer.BYTES + Float.BYTES * query.queryVector().length;
        size += Integer.BYTES + Integer.BYTES + (1 + Float.BYTES) + (1 + Float.BYTES);
        for (byte[] index : indices) {
            size += Integer.BYTES + index.length;
        }
        for (byte[] filter : filters) {
            size += Integer.BYTES + filter.length;
        }
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.putInt(indices.size());
        for (byte[] index : indices) {
            buffer.putInt(index.length).put(index);
        }
        buffer.putInt(field.length).put(field);
        buffer.putInt(query.queryVector().length);
        for (float value : query.queryVector()) {
            buffer.putFloat(value);
        }
        for (byte[] filter : filters) {
            buffer.putInt(filter.length).put(filter);
        }
        buffer.putInt(query.k()).putInt(query.numCandidates());
        putOptional(buffer, query.visitPercentage());
        putOptional(buffer, query.oversample());

        MurmurHash3.Hash128 hash = MurmurHash3.hash128(buffer.array(), 0, size, SEED, new MurmurHash3.Hash128());
        return new QueryFingerprint(hash.h2, hash.h1);
    }

    public String hex() {
        HexFormat hex = HexFormat.of();
        return hex.toHexDigits(high) + hex.toHexDigits(low);
    }

    private static void putOptional(ByteBuffer buffer, Float value) {
        buffer.put((byte) (value == null ? 0 : 1)).putFloat(value == null ? 0f : value);
    }

    private static byte[] canonical(QueryBuilder filter) {
        return Strings.toString(filter).getBytes(StandardCharsets.UTF_8);
    }
}

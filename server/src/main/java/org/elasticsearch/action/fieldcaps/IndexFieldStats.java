/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;

import java.io.IOException;

/**
 * Cheap per-field statistics read from index metadata (terms and points), returned by field caps when
 * {@link FieldCapabilitiesRequest#includeFieldStats()} is set. They are planning hints for query planners
 * such as ES|QL's cost-based optimizer, not exact values: counts include deleted documents that are not
 * merged away yet, and {@link #valueBytes} extrapolates from a sample of term lengths.
 *
 * @param docsWithValue number of documents with at least one value, or {@code -1} if unknown
 * @param valueCount    number of values over all documents, counting each distinct value of a multi-valued
 *                      document once, or {@code -1} if unknown
 * @param valueBytes    total byte length of the values, or {@code -1} if unknown; only set for string fields
 * @param min           smallest value of a numeric or date field, as a {@link Long} or {@link Double}
 * @param max           largest value of a numeric or date field, as a {@link Long} or {@link Double}
 */
public record IndexFieldStats(long docsWithValue, long valueCount, long valueBytes, @Nullable Number min, @Nullable Number max)
    implements
        Writeable {

    public IndexFieldStats(StreamInput in) throws IOException {
        this(in.readZLong(), in.readZLong(), in.readZLong(), (Number) in.readGenericValue(), (Number) in.readGenericValue());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeZLong(docsWithValue);
        out.writeZLong(valueCount);
        out.writeZLong(valueBytes);
        out.writeGenericValue(min);
        out.writeGenericValue(max);
    }

    /**
     * Extrapolates the stats of one shard to an index with {@code numberOfShards} shards of similar content.
     * The bounds stay those of the sampled shard.
     */
    public IndexFieldStats scale(int numberOfShards) {
        return new IndexFieldStats(
            scale(docsWithValue, numberOfShards),
            scale(valueCount, numberOfShards),
            scale(valueBytes, numberOfShards),
            min,
            max
        );
    }

    private static long scale(long value, int factor) {
        if (value < 0 || factor <= 0) {
            return -1;
        }
        try {
            return Math.multiplyExact(value, factor);
        } catch (ArithmeticException e) {
            return -1;
        }
    }
}

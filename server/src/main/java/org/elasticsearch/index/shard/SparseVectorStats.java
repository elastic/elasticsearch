/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Statistics about indexed sparse vector
 */
public class SparseVectorStats implements Writeable, ToXContentFragment {
    private long valueCount = 0;

    public SparseVectorStats() {}

    public SparseVectorStats(long count) {
        this.valueCount = count;
    }

    public SparseVectorStats(StreamInput in) throws IOException {
        this.valueCount = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(valueCount);
    }

    public void add(SparseVectorStats other) {
        if (other == null) {
            return;
        }
        this.valueCount += other.valueCount;
    }

    /**
     * Returns the total number of dense vectors added in the index.
     */
    public long getValueCount() {
        return valueCount;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.NAME);
        builder.field(Fields.VALUE_COUNT, valueCount);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SparseVectorStats that = (SparseVectorStats) o;
        return valueCount == that.valueCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(valueCount);
    }

    static final class Fields {
        static final String NAME = "sparse_vector";
        static final String VALUE_COUNT = "value_count";
    }
}

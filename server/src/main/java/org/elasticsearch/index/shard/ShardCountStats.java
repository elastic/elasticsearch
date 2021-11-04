/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class ShardCountStats implements Writeable, ToXContentFragment {

    private final long totalCount;

    public ShardCountStats() {
        totalCount = 0;
    }

    public ShardCountStats(StreamInput in) throws IOException {
        totalCount = in.readVLong();
    }

    public ShardCountStats(long totalCount) {
        this.totalCount = totalCount;
    }

    public long getTotalCount() {
        return this.totalCount;
    }

    public ShardCountStats add(@Nullable ShardCountStats other) {
        return new ShardCountStats(this.totalCount + (other == null ? 0 : other.totalCount));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.SHARDS);
        builder.field(Fields.TOTAL_COUNT, totalCount);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalCount);
    }

    static final class Fields {
        static final String SHARDS = "shard_stats";
        static final String TOTAL_COUNT = "total_count";
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof ShardCountStats) && totalCount == ((ShardCountStats) o).totalCount;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(totalCount);
    }
}

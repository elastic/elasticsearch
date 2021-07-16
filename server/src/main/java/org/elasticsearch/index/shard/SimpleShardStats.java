/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.core.Nullable;

import java.io.IOException;

public class SimpleShardStats implements Writeable, ToXContentFragment {

    private final long totalCount;

    public SimpleShardStats() {
        totalCount = 0;
    }

    public SimpleShardStats(StreamInput in) throws IOException {
        totalCount = in.readVLong();
    }

    public SimpleShardStats(long totalCount) {
        this.totalCount = totalCount;
    }

    public long getTotalCount() {
        return this.totalCount;
    }

    public SimpleShardStats add(@Nullable SimpleShardStats other) {
        return new SimpleShardStats(this.totalCount + (other == null ? 0 : other.totalCount));
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
        static final String SHARDS = "shards";
        static final String TOTAL_COUNT = "total_count";
    }
}

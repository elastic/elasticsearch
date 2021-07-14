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

import java.io.IOException;

public class SimpleShardStats  implements Writeable, ToXContentFragment {

    private long count = 0;

    public SimpleShardStats() {

    }

    public SimpleShardStats(StreamInput in) throws IOException {
        count = in.readVLong();
    }

    public SimpleShardStats(long count) {
        this.count = count;
    }

    public long getCount() {
        return this.count;
    }

    public SimpleShardStats add(SimpleShardStats other) {
        return new SimpleShardStats(this.count + (other == null ? 0 : other.count));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.SHARDS);
        builder.field(Fields.COUNT, count);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(count);
    }

    static final class Fields {
        static final String SHARDS = "shards";
        static final String COUNT = "count";
    }
}

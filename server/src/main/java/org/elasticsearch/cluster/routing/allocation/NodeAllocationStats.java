/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public record NodeAllocationStats(int shards, double forecastedIngestLoad, long forecastedDiskUsage)
    implements
        Writeable,
        ToXContentFragment {

    public NodeAllocationStats(StreamInput in) throws IOException {
        this(in.readVInt(), in.readDouble(), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(shards);
        out.writeDouble(forecastedIngestLoad);
        out.writeVLong(forecastedDiskUsage);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder
            .field("shards", shards)
            .field("forecasted_ingest_load", forecastedIngestLoad)
            .field("forecasted_disk_usage", forecastedDiskUsage);
    }
}

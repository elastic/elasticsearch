/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Point-in-time allocation stats for a particular node.
 *
 * @param shards count of shards on this node.
 * @param undesiredShards count of shards that we want to move off of this node.
 * @param forecastedIngestLoad the predicted near future total ingest load on this node.
 * @param forecastedDiskUsage the predicted near future total disk usage on this node.
 * @param currentDiskUsage the current total disk usage on this node.
 */
public record NodeAllocationStats(
    int shards,
    int undesiredShards,
    double forecastedIngestLoad,
    long forecastedDiskUsage,
    long currentDiskUsage
) implements Writeable, ToXContentFragment {

    public NodeAllocationStats(StreamInput in) throws IOException {
        this(in.readVInt(), in.readVInt(), in.readDouble(), in.readVLong(), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(shards);
        out.writeVInt(undesiredShards);
        out.writeDouble(forecastedIngestLoad);
        out.writeVLong(forecastedDiskUsage);
        out.writeVLong(currentDiskUsage);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject("allocations")
            .field("shards", shards)
            .field("undesired_shards", undesiredShards)
            .field("forecasted_ingest_load", forecastedIngestLoad)
            .humanReadableField("forecasted_disk_usage_in_bytes", "forecasted_disk_usage", ByteSizeValue.ofBytes(forecastedDiskUsage))
            .humanReadableField("current_disk_usage_in_bytes", "current_disk_usage", ByteSizeValue.ofBytes(currentDiskUsage))
            .endObject();
    }
}

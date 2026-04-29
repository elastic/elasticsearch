/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.datafeed;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Immutable snapshot of cross-cluster search statistics for a running CPS-enabled datafeed.
 * Serialized as part of the datafeed stats API response.
 */
public record CrossClusterSearchStatsSnapshot(
    int totalClusters,
    int availableClusters,
    int skippedClusters,
    Double availabilityRatio,
    Set<String> stabilizedClusterAliases,
    Map<String, Integer> perClusterConsecutiveSkips
) implements Writeable, ToXContentObject {

    public CrossClusterSearchStatsSnapshot(StreamInput in) throws IOException {
        this(
            in.readVInt(),
            in.readVInt(),
            in.readVInt(),
            in.readDouble(),
            in.readCollectionAsSet(StreamInput::readString),
            in.readMap(StreamInput::readVInt)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(totalClusters);
        out.writeVInt(availableClusters);
        out.writeVInt(skippedClusters);
        out.writeDouble(availabilityRatio);
        out.writeStringCollection(stabilizedClusterAliases);
        out.writeMap(perClusterConsecutiveSkips, StreamOutput::writeVInt);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("total_clusters", totalClusters);
        builder.field("available_clusters", availableClusters);
        builder.field("skipped_clusters", skippedClusters);
        builder.field("availability_ratio", availabilityRatio);
        builder.field("stabilized_cluster_aliases", new TreeSet<>(stabilizedClusterAliases));
        builder.field("per_cluster_consecutive_skips", new TreeMap<>(perClusterConsecutiveSkips));
        builder.endObject();
        return builder;
    }
}

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
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Immutable snapshot of cross-project search statistics for a running CPS-enabled datafeed.
 * Serialized as part of the datafeed stats API response.
 */
public record CrossProjectSearchStatsSnapshot(
    int totalProjects,
    int availableProjects,
    int skippedProjects,
    double availabilityRatio,
    Set<String> stabilizedProjectAliases,
    Map<String, Integer> perProjectConsecutiveSkips
) implements Writeable, ToXContentObject {

    public CrossProjectSearchStatsSnapshot(StreamInput in) throws IOException {
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
        out.writeVInt(totalProjects);
        out.writeVInt(availableProjects);
        out.writeVInt(skippedProjects);
        out.writeDouble(availabilityRatio);
        out.writeStringCollection(stabilizedProjectAliases);
        out.writeMap(perProjectConsecutiveSkips, StreamOutput::writeVInt);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("total_projects", totalProjects);
        builder.field("available_projects", availableProjects);
        builder.field("skipped_projects", skippedProjects);
        builder.field("availability_ratio", availabilityRatio);
        builder.field("stabilized_project_aliases", new TreeSet<>(stabilizedProjectAliases));
        builder.field("per_project_consecutive_skips", new TreeMap<>(perProjectConsecutiveSkips));
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CrossProjectSearchStatsSnapshot that = (CrossProjectSearchStatsSnapshot) o;
        return totalProjects == that.totalProjects
            && availableProjects == that.availableProjects
            && skippedProjects == that.skippedProjects
            && Double.compare(availabilityRatio, that.availabilityRatio) == 0
            && Objects.equals(stabilizedProjectAliases, that.stabilizedProjectAliases)
            && Objects.equals(perProjectConsecutiveSkips, that.perProjectConsecutiveSkips);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            totalProjects,
            availableProjects,
            skippedProjects,
            availabilityRatio,
            stabilizedProjectAliases,
            perProjectConsecutiveSkips
        );
    }
}

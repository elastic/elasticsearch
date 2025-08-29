/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.plugins.NodeStatsPlugin;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.chunk;

/**
 * Extra plugin-contributed node statistics.
 */
public class PluginNodeStats implements Writeable, ChunkedToXContent {

    private final Map<String, NodeStatsPlugin.Stats> pluginNameToStats;

    public PluginNodeStats(Map<String, NodeStatsPlugin.Stats> pluginNameToStats) {
        this.pluginNameToStats = Objects.requireNonNull(pluginNameToStats);
    }

    public PluginNodeStats(StreamInput in) throws IOException {
        this(in.readImmutableMap(StreamInput::readString, in2 -> in2.readNamedWriteable(NodeStatsPlugin.Stats.class)));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(pluginNameToStats, StreamOutput::writeString, StreamOutput::writeNamedWriteable);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return chunk((builder, p) -> {
            builder.startObject("plugins");
            for (Map.Entry<String, NodeStatsPlugin.Stats> entry : pluginNameToStats.entrySet()) {
                builder.field(entry.getKey(), entry.getValue(), p);
            }
            return builder.endObject();
        });
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof PluginNodeStats other && Objects.equals(pluginNameToStats, other.pluginNameToStats));
    }

    @Override
    public int hashCode() {
        return Objects.hash(pluginNameToStats);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.plugins.NodeStatsPlugin;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Security plugin node stats.
 */
public class SecurityNodeStatsPluginStats implements NodeStatsPlugin.Stats {

    public static final String WRITEABLE_NAME = "security_node_stats";

    private final Map<String, Object> dlsCacheStats;

    public SecurityNodeStatsPluginStats(Map<String, Object> dlsCacheStats) {
        this.dlsCacheStats = Objects.requireNonNull(dlsCacheStats);
    }

    public SecurityNodeStatsPluginStats(StreamInput in) throws IOException {
        this(in.readGenericMap());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericMap(dlsCacheStats);
    }

    @Override
    public String getWriteableName() {
        return WRITEABLE_NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("dls_cache");
        builder.map(dlsCacheStats);
        return builder.endObject();
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.collector;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.health.collector.NodeHealthCache.NODE_HEALTH_STATE_COLLECTOR;

class NodeHealthCacheTaskParams implements PersistentTaskParams {

    public static final ObjectParser<NodeHealthCacheTaskParams, Void> PARSER = new ObjectParser<>(
        NODE_HEALTH_STATE_COLLECTOR,
        true,
        NodeHealthCacheTaskParams::new
    );

    NodeHealthCacheTaskParams() {}

    NodeHealthCacheTaskParams(StreamInput in) {}

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NODE_HEALTH_STATE_COLLECTOR;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_8_3_0;
    }

    @Override
    public void writeTo(StreamOutput out) {}

    public static NodeHealthCacheTaskParams fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof NodeHealthCacheTaskParams;
    }
}

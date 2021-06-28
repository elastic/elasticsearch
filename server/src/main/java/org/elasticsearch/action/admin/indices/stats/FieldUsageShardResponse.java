/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.search.stats.FieldUsageStats;

import java.io.IOException;
import java.util.Objects;

public class FieldUsageShardResponse implements Writeable {

    final ShardRouting routing;

    final FieldUsageStats stats;

    FieldUsageShardResponse(StreamInput in) throws IOException {
        routing = new ShardRouting(in);
        stats = new FieldUsageStats(in);
    }

    FieldUsageShardResponse(ShardRouting routing, FieldUsageStats stats) {
        this.routing = Objects.requireNonNull(routing, "routing must be non null");
        this.stats = Objects.requireNonNull(stats, "stats must be non null");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        routing.writeTo(out);
        stats.writeTo(out);
    }
}

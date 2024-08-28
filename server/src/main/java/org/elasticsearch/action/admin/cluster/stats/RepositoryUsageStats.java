/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * Stats on repository feature usage exposed in cluster stats for telemetry.
 *
 * @param statsByType a count of the repositories using various named features, keyed by repository type and then by feature name.
 */
public record RepositoryUsageStats(Map<String, Map<String, Long>> statsByType) implements Writeable, ToXContentObject {

    public static final RepositoryUsageStats EMPTY = new RepositoryUsageStats(Map.of());

    public static RepositoryUsageStats readFrom(StreamInput in) throws IOException {
        final var statsByType = in.readMap(i -> i.readMap(StreamInput::readVLong));
        if (statsByType.isEmpty()) {
            return EMPTY;
        } else {
            return new RepositoryUsageStats(statsByType);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(statsByType, (o, m) -> o.writeMap(m, StreamOutput::writeVLong));
    }

    public boolean isEmpty() {
        return statsByType.isEmpty();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (Map.Entry<String, Map<String, Long>> typeAndStats : statsByType.entrySet()) {
            builder.startObject(typeAndStats.getKey());
            for (Map.Entry<String, Long> statAndValue : typeAndStats.getValue().entrySet()) {
                builder.field(statAndValue.getKey(), statAndValue.getValue());
            }
            builder.endObject();
        }
        return builder.endObject();
    }
}

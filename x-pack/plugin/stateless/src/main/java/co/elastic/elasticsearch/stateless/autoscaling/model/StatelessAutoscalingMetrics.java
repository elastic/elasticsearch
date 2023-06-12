/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.autoscaling.model;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

public class StatelessAutoscalingMetrics implements ToXContentObject, Writeable {

    private final Map<String, TierMetrics> tiers;

    public StatelessAutoscalingMetrics(Map<String, TierMetrics> tiers) {
        this.tiers = tiers;
    }

    public StatelessAutoscalingMetrics(final StreamInput input) throws IOException {
        this(input.readMap(StreamInput::readString, TierMetrics::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(tiers, StreamOutput::writeString, StreamOutput::writeWriteable);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.map(tiers);
        return builder;
    }
}

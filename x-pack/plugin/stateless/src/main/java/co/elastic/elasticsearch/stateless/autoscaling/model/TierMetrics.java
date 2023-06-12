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

public class TierMetrics implements ToXContentObject, Writeable {

    private final Map<String, Object> metrics;

    public TierMetrics(Map<String, Object> metrics) {
        this.metrics = metrics;
    }

    public TierMetrics(StreamInput input) throws IOException {
        this(input.readMap(StreamInput::readString, StreamInput::readGenericValue));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(metrics, StreamOutput::writeString, StreamOutput::writeGenericValue);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("metrics", metrics);
        builder.endObject();
        return builder;
    }
}

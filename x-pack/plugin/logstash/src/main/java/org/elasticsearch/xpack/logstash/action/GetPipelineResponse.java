/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logstash.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

public class GetPipelineResponse extends ActionResponse implements ToXContentObject {

    private final Map<String, BytesReference> pipelines;

    public GetPipelineResponse(Map<String, BytesReference> pipelines) {
        this.pipelines = pipelines;
    }

    public GetPipelineResponse(StreamInput in) throws IOException {
        this.pipelines = in.readMap(StreamInput::readBytesReference);
    }

    public Map<String, BytesReference> pipelines() {
        return pipelines;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(pipelines, StreamOutput::writeBytesReference);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (Entry<String, BytesReference> entry : pipelines.entrySet()) {
            builder.rawField(entry.getKey(), entry.getValue().streamInput());
        }
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetPipelineResponse that = (GetPipelineResponse) o;
        return Objects.equals(pipelines, that.pipelines);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pipelines);
    }
}

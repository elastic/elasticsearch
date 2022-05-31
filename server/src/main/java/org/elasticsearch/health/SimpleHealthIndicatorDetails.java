/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public record SimpleHealthIndicatorDetails(Map<String, Object> details) implements HealthIndicatorDetails {

    public SimpleHealthIndicatorDetails(StreamInput in) throws IOException {
        this(getMapFromStreamInput(in));
    }

    private static Map<String, Object> getMapFromStreamInput(StreamInput in) throws IOException {
        return in.readMap(StreamInput::readString, StreamInput::readString);
    }

    public SimpleHealthIndicatorDetails {
        Objects.requireNonNull(details);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.map(details);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(details, StreamOutput::writeString, (streamOutput, value) -> streamOutput.writeString(value.toString()));
    }

    @Override
    public HealthIndicatorDetails read(StreamInput in) throws IOException {
        return new SimpleHealthIndicatorDetails(in);
    }
}

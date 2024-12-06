/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.settings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMap;

public record InferenceHeadersAndBody(Map<String, String> headers, Map<String, Object> body)
    implements
        ToXContentObject,
        VersionedNamedWriteable {

    public InferenceHeadersAndBody(Map<String, String> headers, Map<String, Object> body) {
        this.headers = headers != null ? headers : Map.of();
        this.body = body != null ? body : Map.of();
    }

    public InferenceHeadersAndBody(StreamInput in) throws IOException {
        this(in.readMap(StreamInput::readString), in.readGenericMap());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(headers, StreamOutput::writeString);
        out.writeGenericMap(body);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("headers", headers);
        builder.field("body", body);
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return "";
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.current();
    }

    public static InferenceHeadersAndBody fromStorage(Map<String, Object> storage) {
        return new InferenceHeadersAndBody(removeFromMap(storage, "headers"), removeFromMap(storage, "body"));
    }
}

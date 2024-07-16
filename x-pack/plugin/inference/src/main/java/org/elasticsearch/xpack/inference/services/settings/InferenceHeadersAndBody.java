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
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMap;

public record InferenceHeadersAndBody(Map<String, Object> headers, Map<String, Object> body)
    implements
        ToXContentObject,
        VersionedNamedWriteable {

    public InferenceHeadersAndBody(Map<String, Object> headers, Map<String, Object> body) {
        this.headers = headers != null ? headers : Map.of();
        this.body = body != null ? body : Map.of();
    }

    public InferenceHeadersAndBody(StreamInput in) throws IOException {
        this(readSafeMap(in), readSafeMap(in));
    }

    private static Map<String, Object> readSafeMap(StreamInput in) throws IOException {
        var map = in.readGenericMap();
        return map != null ? map : Map.of();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writeSafeMap(out, headers);
        writeSafeMap(out, body);
    }

    private static void writeSafeMap(StreamOutput out, Map<String, Object> map) throws IOException {
        if (map.isEmpty()) {
            out.writeGenericNull();
        } else {
            out.writeGenericMap(map);
        }
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
        var headers = removeFromMap(storage, "headers");
        var body = removeFromMap(storage, "body");
        return new InferenceHeadersAndBody(headers, body);
    }
}

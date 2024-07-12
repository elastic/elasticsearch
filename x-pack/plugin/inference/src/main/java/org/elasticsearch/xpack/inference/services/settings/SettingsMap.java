/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.settings;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

abstract class SettingsMap implements ToXContentObject, VersionedNamedWriteable {
    private final Map<String, Object> headers;
    private final Map<String, Object> body;

    SettingsMap(Map<String, Object> headers, Map<String, Object> body) {
        this.headers = headers != null ? headers : Map.of();
        this.body = body != null ? body : Map.of();
    }

    SettingsMap(StreamInput in) throws IOException {
        this.headers = readSafeMap(in);
        this.body = readSafeMap(in);
    }

    private static Map<String, Object> readSafeMap(StreamInput in) throws IOException {
        var map = in.readGenericMap();
        return map != null ? map : Map.of();
    }

    public Map<String, Object> headers() {
        return headers;
    }

    public Map<String, Object> body() {
        return body;
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
        builder.field("headers", headers);
        builder.field("body", body);
        return builder;
    }
}

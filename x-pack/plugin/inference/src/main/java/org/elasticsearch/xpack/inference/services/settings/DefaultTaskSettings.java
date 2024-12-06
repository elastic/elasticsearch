/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.settings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrThrowIfNull;

public class DefaultTaskSettings implements TaskSettings {
    private final InferenceHeadersAndBody headersAndBody;

    public DefaultTaskSettings(InferenceHeadersAndBody headersAndBody) {
        this.headersAndBody = headersAndBody;
    }

    @Override
    public String getWriteableName() {
        return "inference_default_task_settings";
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeWriteable(headersAndBody);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("headersAndBody", headersAndBody);
        builder.endObject();
        return builder;
    }

    public static DefaultTaskSettings fromStorage(Map<String, Object> storage) {
        var headersAndBody = InferenceHeadersAndBody.fromStorage(removeFromMapOrThrowIfNull(storage, "headersAndBody"));
        return new DefaultTaskSettings(headersAndBody);
    }
}

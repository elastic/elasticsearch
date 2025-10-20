/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.authorization;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.health.node.selection.HealthNodeTaskParams;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.xpack.inference.services.elastic.authorization.AuthorizationPoller.TASK_NAME;

public class AuthorizationTaskParams implements PersistentTaskParams {
    private static final AuthorizationTaskParams INSTANCE = new AuthorizationTaskParams();

    private static final ObjectParser<AuthorizationTaskParams, Void> PARSER = new ObjectParser<>(TASK_NAME, true, () -> INSTANCE);

    AuthorizationTaskParams() {}

    AuthorizationTaskParams(StreamInput in) {}

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return TASK_NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_5_0;
    }

    @Override
    public void writeTo(StreamOutput out) {}

    public static AuthorizationTaskParams fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof HealthNodeTaskParams;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.authorization;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.xpack.inference.services.elastic.authorization.AuthorizationPoller.TASK_NAME;

/**
 * Empty parameters for the authorization persistent task.
 */
public class AuthorizationTaskParams implements PersistentTaskParams {
    public static final AuthorizationTaskParams INSTANCE = new AuthorizationTaskParams();

    private static final ObjectParser<AuthorizationTaskParams, Void> PARSER = new ObjectParser<>(TASK_NAME, true, () -> INSTANCE);
    private static final TransportVersion INFERENCE_API_EIS_AUTHORIZATION_PERSISTENT_TASK = TransportVersion.fromName(
        "inference_api_eis_authorization_persistent_task"
    );

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
        return INFERENCE_API_EIS_AUTHORIZATION_PERSISTENT_TASK;
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
    public boolean equals(Object o) {
        return this == o || (o != null && getClass() == o.getClass());
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.support;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class RoleMappingCleanupTaskParams implements PersistentTaskParams {
    public static final String TASK_NAME = "role-mapping-cleanup";

    public static final ObjectParser<RoleMappingCleanupTaskParams, Void> PARSER = new ObjectParser<>(
        TASK_NAME,
        true,
        RoleMappingCleanupTaskParams::new
    );

    public RoleMappingCleanupTaskParams() {}

    public RoleMappingCleanupTaskParams(StreamInput in) throws IOException {}

    @Override
    public void writeTo(StreamOutput out) throws IOException {}

    @Override
    public String getWriteableName() {
        return TASK_NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ADD_METADATA_FLATTENED_TO_ROLES;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.endObject();
        return builder;
    }

    public static RoleMappingCleanupTaskParams fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}

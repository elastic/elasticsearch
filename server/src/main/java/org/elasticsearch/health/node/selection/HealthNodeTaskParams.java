/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node.selection;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.health.node.selection.HealthNode.TASK_NAME;

/**
 * Encapsulates the parameters needed to start the health node task, currently no parameters are required.
 */
public class HealthNodeTaskParams implements PersistentTaskParams {

    public static final HealthNodeTaskParams INSTANCE = new HealthNodeTaskParams();

    public static final ObjectParser<HealthNodeTaskParams, Void> PARSER = new ObjectParser<>(TASK_NAME, true, () -> INSTANCE);

    HealthNodeTaskParams() {}

    HealthNodeTaskParams(StreamInput ignored) {}

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return TASK_NAME;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_8_4_0;
    }

    @Override
    public void writeTo(StreamOutput out) {}

    public static HealthNodeTaskParams fromXContent(XContentParser parser) {
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

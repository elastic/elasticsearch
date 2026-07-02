/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * This class defines an empty task settings object. This is useful for services that do not have any task settings.
 *
 * <p>This is not a record so that it can be subclassed by {@code EnforcingEmptyTaskSettings}, which reuses this
 * class's writeable name and serialization so it remains readable by nodes that predate it, while still enforcing
 * rejection of unknown settings on {@link #updatedTaskSettings}.
 */
public class EmptyTaskSettings implements TaskSettings {
    public static final String NAME = "empty_task_settings";

    public static final EmptyTaskSettings INSTANCE = new EmptyTaskSettings();

    protected EmptyTaskSettings() {}

    public EmptyTaskSettings(StreamInput in) {
        this();
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {}

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        return INSTANCE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o != null && getClass() == o.getClass();
    }

    @Override
    public int hashCode() {
        // The class doesn't have any members so return the same hash code
        return Objects.hash(NAME);
    }
}

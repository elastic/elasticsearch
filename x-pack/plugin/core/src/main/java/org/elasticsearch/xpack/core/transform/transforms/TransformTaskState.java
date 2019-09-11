/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Locale;

public enum TransformTaskState implements Writeable {
    STOPPED, STARTED, FAILED;

    public static TransformTaskState fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    public static TransformTaskState fromStream(StreamInput in) throws IOException {
        return in.readEnum(TransformTaskState.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        TransformTaskState state = this;
        out.writeEnum(state);
    }

    public String value() {
        return name().toLowerCase(Locale.ROOT);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tasks;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static org.elasticsearch.common.xcontent.XContentHelper.convertToMap;

/**
 * Raw, unparsed status from the task results index.
 */
public class RawTaskStatus implements Task.Status {
    public static final String NAME = "raw";

    private final BytesReference status;

    public RawTaskStatus(BytesReference status) {
        this.status = requireNonNull(status, "status may not be null");
    }

    /**
     * Read from a stream.
     */
    public RawTaskStatus(StreamInput in) throws IOException {
        status = in.readOptionalBytesReference();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalBytesReference(status);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        try (InputStream stream = status.streamInput()) {
            return builder.rawValue(stream, XContentHelper.xContentType(status));
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    /**
     * Convert the from XContent to a Map for easy reading.
     */
    public Map<String, Object> toMap() {
        return convertToMap(status, false).v2();
    }

    // Implements equals and hashcode for testing
    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != RawTaskStatus.class) {
            return false;
        }
        RawTaskStatus other = (RawTaskStatus) obj;
        // Totally not efficient, but ok for testing because it ignores order and spacing differences
        return toMap().equals(other.toMap());
    }

    @Override
    public int hashCode() {
        // Totally not efficient, but ok for testing because consistent with equals
        return toMap().hashCode();
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.notifications;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Locale;

public enum Level implements Writeable {
    INFO, ACTIVITY, WARNING, ERROR;

    /**
     * Case-insensitive from string method.
     *
     * @param value
     *            String representation
     * @return The condition type
     */
    public static Level fromString(String value) {
        return Level.valueOf(value.toUpperCase(Locale.ROOT));
    }

    public static Level readFromStream(StreamInput in) throws IOException {
        return in.readEnum(Level.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(this);
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}

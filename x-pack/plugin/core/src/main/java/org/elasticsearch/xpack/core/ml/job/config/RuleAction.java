/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Locale;

public enum RuleAction implements Writeable {
    SKIP_RESULT,
    SKIP_MODEL_UPDATE;

    /**
     * Case-insensitive from string method.
     *
     * @param value String representation
     * @return The rule action
     */
    public static RuleAction fromString(String value) {
        return RuleAction.valueOf(value.toUpperCase(Locale.ROOT));
    }

    public static RuleAction readFromStream(StreamInput in) throws IOException {
        return in.readEnum(RuleAction.class);
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

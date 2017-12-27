/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config;


import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Locale;

public enum RuleConditionType implements Writeable {
    CATEGORICAL(false),
    NUMERICAL_ACTUAL(true),
    NUMERICAL_TYPICAL(true),
    NUMERICAL_DIFF_ABS(true),
    TIME(false);

    private final boolean isNumerical;

    RuleConditionType(boolean isNumerical) {
        this.isNumerical = isNumerical;
    }

    public boolean isNumerical() {
        return isNumerical;
    }

    /**
     * Case-insensitive from string method.
     *
     * @param value
     *            String representation
     * @return The condition type
     */
    public static RuleConditionType fromString(String value) {
        return RuleConditionType.valueOf(value.toUpperCase(Locale.ROOT));
    }

    public static RuleConditionType readFromStream(StreamInput in) throws IOException {
        return in.readEnum(RuleConditionType.class);
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

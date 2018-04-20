/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.config;


import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Locale;

public enum RuleConditionType implements Writeable {
    CATEGORICAL(false, true),
    NUMERICAL_ACTUAL(true, false),
    NUMERICAL_TYPICAL(true, false),
    NUMERICAL_DIFF_ABS(true, false),
    TIME(false, false),
    CATEGORICAL_COMPLEMENT(false, true);

    private final boolean isNumerical;
    private final boolean isCategorical;

    RuleConditionType(boolean isNumerical, boolean isCategorical) {
        this.isNumerical = isNumerical;
        this.isCategorical = isCategorical;
    }

    public boolean isNumerical() {
        return isNumerical;
    }

    public boolean isCategorical() {
        return isCategorical;
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
        if (this == CATEGORICAL_COMPLEMENT && out.getVersion().before(Version.V_6_3_0)) {
            out.writeEnum(CATEGORICAL);
        } else {
            out.writeEnum(this);
        }
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Locale;

/**
 * Enum representing logical comparisons on doubles
 */
public enum Operator implements Writeable {
    GT {
        @Override
        public boolean test(double lhs, double rhs) {
            return Double.compare(lhs, rhs) > 0;
        }
    },
    GTE {
        @Override
        public boolean test(double lhs, double rhs) {
            return Double.compare(lhs, rhs) >= 0;
        }
    },
    LT {
        @Override
        public boolean test(double lhs, double rhs) {
            return Double.compare(lhs, rhs) < 0;
        }
    },
    LTE {
        @Override
        public boolean test(double lhs, double rhs) {
            return Double.compare(lhs, rhs) <= 0;
        }
    };
    // EQ was considered but given the oddity of such a
    // condition and the fact that it would be a numerically
    // unstable condition, it was rejected.

    public static final ParseField OPERATOR_FIELD = new ParseField("operator");

    public boolean test(double lhs, double rhs) {
        return false;
    }

    public static Operator fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    public static Operator readFromStream(StreamInput in) throws IOException {
        return in.readEnum(Operator.class);
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

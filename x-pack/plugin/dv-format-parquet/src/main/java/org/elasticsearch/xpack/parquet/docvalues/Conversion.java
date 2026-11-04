/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import java.util.Locale;

/**
 * How the values of an integer column are turned into the values that the mapped field type expects in its doc values. The mapping and
 * the conversion of a field must agree: for example a {@code date} field holds epoch milliseconds, so a column of epoch seconds needs
 * {@link #SECONDS_TO_MILLIS}. The conversion is stored in the segment, next to the binding of the field to its column.
 */
public enum Conversion {
    /** The value is used as it is. */
    NONE {
        @Override
        public long apply(long value) {
            return value;
        }
    },
    /** An epoch in seconds becomes an epoch in milliseconds, for a {@code date} field. */
    SECONDS_TO_MILLIS {
        @Override
        public long apply(long value) {
            return Math.multiplyExact(value, 1_000L);
        }
    },
    /** A number of days since the epoch becomes an epoch in milliseconds, for a {@code date} field. */
    DAYS_TO_MILLIS {
        @Override
        public long apply(long value) {
            return Math.multiplyExact(value, 86_400_000L);
        }
    },
    /** An epoch in microseconds becomes an epoch in milliseconds (rounded down), for a {@code date} field. */
    MICROS_TO_MILLIS {
        @Override
        public long apply(long value) {
            return Math.floorDiv(value, 1_000L);
        }
    },
    /** An epoch in microseconds becomes an epoch in nanoseconds, for a {@code date_nanos} field. */
    MICROS_TO_NANOS {
        @Override
        public long apply(long value) {
            return Math.multiplyExact(value, 1_000L);
        }
    };

    /** Converts one value. Every conversion keeps the order of the values, which the skippers rely on. */
    public abstract long apply(long value);

    /** The name that a mount request uses, for example {@code seconds_to_millis}. */
    public String externalName() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static Conversion fromExternalName(String name) {
        for (Conversion conversion : values()) {
            if (conversion.externalName().equals(name)) {
                return conversion;
            }
        }
        throw new IllegalArgumentException("unknown conversion [" + name + "]");
    }
}

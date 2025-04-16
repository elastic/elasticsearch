/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;

/**
 * Wrapper class for the {@link DataStreamGlobalRetentionSettings}.
 */
public record DataStreamGlobalRetention(
    @Nullable TimeValue defaultRetention,
    @Nullable TimeValue maxRetention,
    TimeValue failuresDefaultRetention
) implements Writeable {

    public static final TimeValue MIN_RETENTION_VALUE = TimeValue.timeValueSeconds(10);
    public static final TimeValue FAILURES_DEFAULT_VALUE = TimeValue.timeValueDays(20);

    /**
     * @param defaultRetention the default retention or null if it's undefined
     * @param maxRetention     the max retention or null if it's undefined
     * @param failuresDefaultRetention the default retention for failure store or null if it's undefined
     * @throws IllegalArgumentException when the default retention is greater than the max retention.
     */
    public DataStreamGlobalRetention {
        if (defaultRetention != null && maxRetention != null && defaultRetention.getMillis() > maxRetention.getMillis()) {
            throw new IllegalArgumentException(
                "Default global retention ["
                    + defaultRetention.getStringRep()
                    + "] cannot be greater than the max global retention ["
                    + maxRetention.getStringRep()
                    + "]."
            );
        }
        if (validateRetentionValue(defaultRetention) == false
            || validateRetentionValue(maxRetention) == false
            || validateRetentionValue(failuresDefaultRetention) == false) {
            throw new IllegalArgumentException("Global retention values should be greater than " + MIN_RETENTION_VALUE.getStringRep());
        }
    }

    public DataStreamGlobalRetention(@Nullable TimeValue defaultRetention, @Nullable TimeValue maxRetention) {
        this(defaultRetention, maxRetention, FAILURES_DEFAULT_VALUE);
    }

    private boolean validateRetentionValue(@Nullable TimeValue retention) {
        return retention == null || retention.getMillis() >= MIN_RETENTION_VALUE.getMillis();
    }

    public static DataStreamGlobalRetention read(StreamInput in) throws IOException {
        return new DataStreamGlobalRetention(
            in.readOptionalTimeValue(),
            in.readOptionalTimeValue(),
            in.getTransportVersion().onOrAfter(TransportVersions.INTRODUCE_FAILURES_LIFECYCLE)
                ? in.readOptionalTimeValue()
                : FAILURES_DEFAULT_VALUE
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalTimeValue(defaultRetention);
        out.writeOptionalTimeValue(maxRetention);
        if (out.getTransportVersion().onOrAfter(TransportVersions.INTRODUCE_FAILURES_LIFECYCLE)) {
            out.writeOptionalTimeValue(failuresDefaultRetention);
        }
    }

    @Override
    public String toString() {
        return "DataStreamGlobalRetention{"
            + "defaultRetention="
            + (defaultRetention == null ? "null" : defaultRetention.getStringRep())
            + ", maxRetention="
            + (maxRetention == null ? "null" : maxRetention.getStringRep())
            + ", failuresDefaultRetention="
            + failuresDefaultRetention.getStringRep()
            + '}';
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.utils.MemoryTrackedTaskState;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;

public enum DataFrameAnalyticsState implements Writeable, MemoryTrackedTaskState {

    // States reindexing and analyzing are no longer used.
    // However, we need to keep them for BWC as tasks may be
    // awaiting assignment in older versioned nodes.
    STARTED,
    REINDEXING,
    ANALYZING,
    STOPPING,
    STOPPED,
    FAILED,
    STARTING;

    public static DataFrameAnalyticsState fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    public static DataFrameAnalyticsState fromStream(StreamInput in) throws IOException {
        return in.readEnum(DataFrameAnalyticsState.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(this);
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    /**
     * @return {@code true} if state matches any of the given {@code candidates}
     */
    public boolean isAnyOf(DataFrameAnalyticsState... candidates) {
        return Arrays.stream(candidates).anyMatch(candidate -> this == candidate);
    }

    /**
     * @return {@code true} if state matches none of the given {@code candidates}
     */
    public boolean isNoneOf(DataFrameAnalyticsState... candidates) {
        return Arrays.stream(candidates).noneMatch(candidate -> this == candidate);
    }

    @Override
    public boolean consumesMemory() {
        return isNoneOf(FAILED, STOPPED);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;

public enum DataFrameAnalyticsState implements Writeable {

    STARTED, REINDEXING, ANALYZING, STOPPING, STOPPED, FAILED;

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
}

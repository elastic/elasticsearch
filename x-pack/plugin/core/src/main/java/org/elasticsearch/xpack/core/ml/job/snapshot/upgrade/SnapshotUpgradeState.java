/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.snapshot.upgrade;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.utils.MemoryTrackedTaskState;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;

public enum SnapshotUpgradeState implements Writeable, MemoryTrackedTaskState {

    LOADING_OLD_STATE, SAVING_NEW_STATE, STOPPED, FAILED;

    public static SnapshotUpgradeState fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    public static SnapshotUpgradeState fromStream(StreamInput in) throws IOException {
        return in.readEnum(SnapshotUpgradeState.class);
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
     * @return {@code true} if state matches none of the given {@code candidates}
     */
    public boolean isNoneOf(SnapshotUpgradeState... candidates) {
        return Arrays.stream(candidates).noneMatch(candidate -> this == candidate);
    }

    @Override
    public boolean consumesMemory() {
        return isNoneOf(FAILED, STOPPED);
    }
}

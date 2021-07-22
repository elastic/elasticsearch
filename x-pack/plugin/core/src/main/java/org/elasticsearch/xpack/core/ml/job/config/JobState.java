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
import org.elasticsearch.xpack.core.ml.utils.MemoryTrackedTaskState;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;

/**
 * Jobs whether running or complete are in one of these states.
 * When a job is created it is initialised in to the state closed
 * i.e. it is not running.
 */
public enum JobState implements Writeable, MemoryTrackedTaskState {

    CLOSING, CLOSED, OPENED, FAILED, OPENING;

    public static JobState fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    public static JobState fromStream(StreamInput in) throws IOException {
        return in.readEnum(JobState.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        JobState state = this;
        out.writeEnum(state);
    }

    public String value() {
        return name().toLowerCase(Locale.ROOT);
    }


    /**
     * @return {@code true} if state matches any of the given {@code candidates}
     */
    public boolean isAnyOf(JobState... candidates) {
        return Arrays.stream(candidates).anyMatch(candidate -> this == candidate);
    }

    /**
     * @return {@code true} if state matches none of the given {@code candidates}
     */
    public boolean isNoneOf(JobState... candidates) {
        return Arrays.stream(candidates).noneMatch(candidate -> this == candidate);
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    @Override
    public boolean consumesMemory() {
        return isNoneOf(CLOSED, FAILED);
    }

    @Override
    public boolean isAllocating() {
        return this == OPENING;
    }
}

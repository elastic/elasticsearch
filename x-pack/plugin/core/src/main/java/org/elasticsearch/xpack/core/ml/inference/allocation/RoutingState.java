/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.allocation;

import org.elasticsearch.xpack.core.ml.utils.MemoryTrackedTaskState;

import java.util.Arrays;
import java.util.Locale;

public enum RoutingState implements MemoryTrackedTaskState {
    STARTING,
    STARTED,
    STOPPING,
    FAILED,
    STOPPED;

    public static RoutingState fromString(String value) {
        return valueOf(value.toUpperCase(Locale.ROOT));
    }

    /**
     * @param candidates one or more candidate states
     * @return {@code true} if state matches none of the given {@code candidates}
     */
    public boolean isNoneOf(RoutingState... candidates) {
        return Arrays.stream(candidates).noneMatch(candidate -> this == candidate);
    }

    /**
     * @param candidates one or more candidate states
     * @return {@code true} if state matches one of the given {@code candidates}
     */
    public boolean isAnyOf(RoutingState... candidates) {
        return Arrays.stream(candidates).anyMatch(candidate -> this == candidate);
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    @Override
    public boolean consumesMemory() {
        return isNoneOf(FAILED, STOPPED);
    }

    @Override
    public boolean isAllocating() {
        return this == STARTING;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.allocation;

import java.util.Arrays;
import java.util.Locale;

public enum AllocationState {
    STARTING,
    PARTIALLY_STARTED,
    STARTED,
    STOPPING;

    public static AllocationState fromString(String value) {
        return valueOf(value.toUpperCase(Locale.ROOT));
    }

    public boolean isAnyOf(AllocationState... candidates) {
        return Arrays.stream(candidates).anyMatch(candidate -> this == candidate);
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}

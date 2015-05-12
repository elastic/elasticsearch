/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.execution;

import java.util.Locale;

/**
 */
public enum ExecutionPhase {

    AWAITS_EXECUTION,
    INPUT,
    CONDITION,
    WATCH_TRANSFORM,
    ACTIONS,
    FINISHED;

    public static ExecutionPhase parse(String value) {
        return valueOf(value.toUpperCase(Locale.ROOT));
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}

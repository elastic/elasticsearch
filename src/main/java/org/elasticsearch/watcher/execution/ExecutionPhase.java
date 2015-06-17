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

    AWAITS_EXECUTION(false),
    STARTED(false),
    INPUT(false),
    CONDITION(false),
    WATCH_TRANSFORM(false),
    ACTIONS(false),
    ABORTED(true),
    FINISHED(true);

    private final boolean sealed;

    ExecutionPhase(boolean sealed) {
        this.sealed = sealed;
    }

    public boolean sealed() {
        return sealed;
    }

    public String id() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static ExecutionPhase resolve(String id) {
        return valueOf(id.toUpperCase(Locale.ROOT));
    }

    @Override
    public String toString() {
        return id();
    }
}

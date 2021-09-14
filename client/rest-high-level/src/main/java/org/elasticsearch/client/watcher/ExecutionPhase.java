/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.watcher;

public enum ExecutionPhase {

    // awaiting execution of the watch
    AWAITS_EXECUTION(false),
    // initial phase, watch execution has started, but the input is not yet processed
    STARTED(false),
    // input is being executed
    INPUT(false),
    // condition phase is being executed
    CONDITION(false),
    // transform phase (optional, depends if a global transform was configured in the watch)
    WATCH_TRANSFORM(false),
    // actions phase, all actions, including specific action transforms
    ACTIONS(false),
    // missing watch, failed execution of input/condition/transform,
    ABORTED(true),
    // successful run
    FINISHED(true);

    private final boolean sealed;

    ExecutionPhase(boolean sealed) {
        this.sealed = sealed;
    }

    public boolean sealed() {
        return sealed;
    }
}

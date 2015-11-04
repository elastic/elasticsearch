/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.execution;

import java.util.Locale;

public enum ExecutionState {

    EXECUTION_NOT_NEEDED,
    THROTTLED,
    EXECUTED,
    FAILED,
    NOT_EXECUTED_WATCH_MISSING,
    EXECUTED_MULTIPLE_TIMES;

    public String id() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static ExecutionState resolve(String id) {
        return valueOf(id.toUpperCase(Locale.ROOT));
    }

    @Override
    public String toString() {
        return id();
    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import java.util.concurrent.RejectedExecutionException;

public class EsRejectedExecutionException extends RejectedExecutionException {

    private final boolean isExecutorShutdown;

    public EsRejectedExecutionException(String message, boolean isExecutorShutdown) {
        super(message);
        this.isExecutorShutdown = isExecutorShutdown;
    }

    public EsRejectedExecutionException(String message) {
        this(message, false);
    }

    public EsRejectedExecutionException() {
        this(null, false);
    }

    @Override
    public Throwable fillInStackTrace() {
        return this; // this exception doesn't imply a bug, no need for a stack trace
    }

    /**
     * Checks if the thread pool that rejected the execution was terminated
     * shortly after the rejection. Its possible that this returns false and the
     * thread pool has since been terminated but if this returns false then the
     * termination wasn't a factor in this rejection. Conversely if this returns
     * true the shutdown was probably a factor in this rejection but might have
     * been triggered just after the action rejection.
     */
    public boolean isExecutorShutdown() {
        return isExecutorShutdown;
    }

}

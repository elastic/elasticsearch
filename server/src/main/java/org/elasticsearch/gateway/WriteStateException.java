/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gateway;

import java.io.IOError;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * This exception is thrown when there is a problem of writing state to disk.
 */
public class WriteStateException extends IOException {
    private final boolean dirty;

    WriteStateException(boolean dirty, String message, Exception cause) {
        super(message, cause);
        this.dirty = dirty;
    }

    /**
     * If this method returns false, state is guaranteed to be not written to disk.
     * If this method returns true, we don't know if state is written to disk.
     */
    public boolean isDirty() {
        return dirty;
    }

    /**
     * Rethrows this {@link WriteStateException} as {@link IOError} if dirty flag is set, which will lead to JVM shutdown.
     * If dirty flag is not set, this exception is wrapped into {@link UncheckedIOException}.
     */
    public void rethrowAsErrorOrUncheckedException() {
        if (isDirty()) {
            throw new IOError(this);
        } else {
            throw new UncheckedIOException(this);
        }
    }
}

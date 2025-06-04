/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.translog;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.exception.ExceptionsHelper;

import java.util.concurrent.atomic.AtomicReference;

public class TragicExceptionHolder {
    private final AtomicReference<Exception> tragedy = new AtomicReference<>();

    /**
     * Sets the tragic exception or if the tragic exception is already set adds passed exception as suppressed exception
     * @param ex tragic exception to set
     */
    public void setTragicException(Exception ex) {
        assert ex != null;
        if (tragedy.compareAndSet(null, ex)) {
            return; // first exception
        }
        final Exception tragedy = this.tragedy.get();
        // ensure no circular reference
        if (ExceptionsHelper.unwrapCausesAndSuppressed(ex, e -> e == tragedy).isEmpty()) {
            tragedy.addSuppressed(ex);
        } else {
            assert ex == tragedy || ex instanceof AlreadyClosedException : new AssertionError("must be ACE or tragic exception", ex);
        }
    }

    public Exception get() {
        return tragedy.get();
    }
}

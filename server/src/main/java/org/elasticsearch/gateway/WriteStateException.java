/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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

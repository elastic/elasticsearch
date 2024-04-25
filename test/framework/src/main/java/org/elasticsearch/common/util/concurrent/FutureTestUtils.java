/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.elasticsearch.test.ESTestCase.fail;

public class FutureTestUtils {

    /**
     * Get the result of a future, retrying on interruption, and permitting the result to be a {@link RuntimeException} or {@link
     * IOException}. This is useful for making an IO-related computation async in production while still having tests that block until it
     * completes as if it were synchronous.
     */
    public static <T> T uninterruptibleIOGet(Future<T> future) throws IOException {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    return future.get();
                } catch (InterruptedException e) {
                    interrupted = true;
                    // retry the wait (interrupt status is reinstated later)
                } catch (ExecutionException e) {
                    final var cause = e.getCause();
                    if (cause instanceof RuntimeException runtimeException) {
                        throw runtimeException;
                    }
                    if (cause instanceof IOException ioException) {
                        throw ioException;
                    }
                    fail(e);
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}

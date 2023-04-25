/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.elasticsearch.core.Nullable;

import java.util.concurrent.Callable;

/**
 * Test utilities
 */
public class TestUtils {
    public static <T> T assetNoException(Callable<T> callable, @Nullable String msg) {
        try {
            return callable.call();
        } catch (Throwable th) {
            throw new AssertionError(((msg != null) ? msg + "; original error: " : "unexpected error: ") + th.getMessage());
        }
    }
}

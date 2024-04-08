/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster.util;

public final class ExceptionUtils {
    private ExceptionUtils() {}

    public static Throwable findRootCause(Throwable t) {
        Throwable cause = t.getCause();
        if (cause == null) {
            return t;
        }

        return findRootCause(cause);
    }
}

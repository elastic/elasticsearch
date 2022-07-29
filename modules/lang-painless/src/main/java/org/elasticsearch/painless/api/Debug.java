/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.api;

import org.elasticsearch.painless.PainlessExplainError;

/**
 * Utility methods for debugging painless scripts that are accessible to painless scripts.
 */
public class Debug {
    private Debug() {}

    /**
     * Throw an {@link Error} that "explains" an object.
     */
    public static void explain(Object objectToExplain) throws PainlessExplainError {
        throw new PainlessExplainError(objectToExplain);
    }
}

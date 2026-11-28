/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

public final class FrequencyCappedActionTestUtils {

    private FrequencyCappedActionTestUtils() {}

    /**
     * Make the action due again, as if it had never run. Lets tests that share a static {@link FrequencyCappedAction} be independent of
     * each other, and of how many times they are repeated.
     */
    public static void reset(FrequencyCappedAction action) {
        action.reset();
    }
}

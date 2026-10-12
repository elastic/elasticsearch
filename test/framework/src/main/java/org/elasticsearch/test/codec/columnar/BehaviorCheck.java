/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.codec.columnar;

/**
 * One named keyword behavior comparison between a baseline index and a contender index. An
 * implementation issues the same request to both indices, validates the baseline response against the corpus
 * oracle in {@link DuelContext} so broken setup fails loudly, then asserts the contender matches under the
 * comparison contract the API dictates. Failures throw {@link AssertionError} carrying
 * {@link DuelContext#failureContext} so the check, comparison mode, scenario, layouts, and write plan are all
 * reported. DSL and ES|QL checks implement this same interface in their respective modules.
 */
public interface BehaviorCheck {

    /**
     * @return the check name, included in failure messages.
     */
    String name();

    /**
     * Runs the comparison, throwing {@link AssertionError} on a baseline or contender mismatch.
     *
     * @param context the baseline, contender, field, scenario, write plan, and corpus oracles
     */
    void check(DuelContext context);
}

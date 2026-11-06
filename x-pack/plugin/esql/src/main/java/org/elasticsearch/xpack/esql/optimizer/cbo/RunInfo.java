/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.Trigger;

import java.util.Objects;

/**
 * Identifies one optimizer run within a query, so that reports from the initial plan and from plans rebuilt after subplans can be
 * told apart.
 *
 * @param ordinal 0-based position of the run within the query
 * @param trigger why the run happened
 * @param subplan 1-based ordinal of the subplan whose result produced the plan, or {@code null} for the initial run
 */
public record RunInfo(int ordinal, Trigger trigger, @Nullable Integer subplan) {
    public static final RunInfo INITIAL = new RunInfo(0, Trigger.INITIAL, null);

    public RunInfo {
        Objects.requireNonNull(trigger, "trigger");
    }
}

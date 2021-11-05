/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

/**
 * Signals that the policy for an index has been fully executed.
 */
public class TerminalPolicyStep extends Step {
    public static final String COMPLETED_PHASE = "completed";
    public static final StepKey KEY = new StepKey(COMPLETED_PHASE, "completed", "completed");
    public static final TerminalPolicyStep INSTANCE = new TerminalPolicyStep(KEY, null);

    TerminalPolicyStep(StepKey key, StepKey nextStepKey) {
        super(key, nextStepKey);
    }

    @Override
    public boolean isRetryable() {
        // this is marker step so it doesn't make sense to be retryable
        return false;
    }
}

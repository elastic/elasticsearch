/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

/**
 * This is essentially a marker that a phase has ended, and we need to check
 * the age of an index before proceeding to the next phase.
 */
public class PhaseCompleteStep extends Step {
    public static final String NAME = "complete";

    public PhaseCompleteStep(StepKey key, StepKey nextStepKey) {
        super(key, nextStepKey);
    }
}

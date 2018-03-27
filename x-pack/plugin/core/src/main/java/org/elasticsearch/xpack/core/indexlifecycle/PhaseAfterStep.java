/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.common.unit.TimeValue;

public class PhaseAfterStep extends Step {
    private final TimeValue after;

    public PhaseAfterStep(String phase, String action, String name, TimeValue after, StepKey nextStepKey) {
        super(name, action, phase, nextStepKey);
        this.after = after;
    }
}

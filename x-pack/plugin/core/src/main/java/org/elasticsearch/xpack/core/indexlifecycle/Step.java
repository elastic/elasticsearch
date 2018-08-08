/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.protocol.xpack.indexlifecycle.StepKey;

import java.util.Objects;

/**
 * A {@link LifecycleAction} which deletes the index.
 */
public abstract class Step {
    private final StepKey key;
    private final StepKey nextStepKey;

    public Step(StepKey key, StepKey nextStepKey) {
        this.key = key;
        this.nextStepKey = nextStepKey;
    }

    public final StepKey getKey() {
        return key;
    }

    public final StepKey getNextStepKey() {
        return nextStepKey;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, nextStepKey);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Step other = (Step) obj;
        return Objects.equals(key, other.key) &&
                Objects.equals(nextStepKey, other.nextStepKey);
    }

    @Override
    public String toString() {
        return key + " => " + nextStepKey;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

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

    public StepKey getKey() {
        return key;
    }

    public StepKey getNextStepKey() {
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
        return Objects.equals(key, other.key) && Objects.equals(nextStepKey, other.nextStepKey);
    }

    @Override
    public String toString() {
        return key + " => " + nextStepKey;
    }

    public static class StepKey {
        private final String phase;
        private final String action;
        private final String name;

        public StepKey(String phase, String action, String name) {
            this.phase = phase;
            this.action = action;
            this.name = name;
        }

        public String getPhase() {
            return phase;
        }

        public String getAction() {
            return action;
        }

        public String getName() {
            return name;
        }

        @Override
        public int hashCode() {
            return Objects.hash(phase, action, name);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            StepKey other = (StepKey) obj;
            return Objects.equals(phase, other.phase) && Objects.equals(action, other.action) && Objects.equals(name, other.name);
        }

        @Override
        public String toString() {
            return String.format("[%s][%s][%s]", phase, action, name);
        }
    }
}

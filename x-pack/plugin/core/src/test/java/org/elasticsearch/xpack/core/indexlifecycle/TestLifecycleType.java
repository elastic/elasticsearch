/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy.NextActionProvider;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

public class TestLifecycleType implements LifecycleType {
    public static final TestLifecycleType INSTANCE = new TestLifecycleType();

    public static final String TYPE = "test";

    private TestLifecycleType() {
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public NextActionProvider getActionProvider(IndexLifecycleContext context, Phase phase) {
        return a -> Optional.ofNullable(phase.getActions().entrySet().iterator().next()).map(Map.Entry::getValue).orElse(null);
    }

    @Override
    public Phase getFirstPhase(Map<String, Phase> phases) {
        return phases.values().iterator().next();
    }

    @Override
    public Phase nextPhase(Map<String, Phase> phases, @Nullable Phase currentPhase) {
        if (currentPhase == null) {
            return getFirstPhase(phases);
        }

        boolean foundPhase = false;
        for (Phase phase : phases.values()) {
            if (foundPhase) {
                return phase;
            } else if (phase.equals(currentPhase)) {
                foundPhase = true;
            }
        }

        return null;
    }

    @Override
    public void validate(Collection<Phase> phases) {
        // always valid
    }
}

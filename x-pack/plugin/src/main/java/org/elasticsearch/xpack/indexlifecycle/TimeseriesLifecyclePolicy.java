/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Represents the lifecycle of an index from creation to deletion. A
 * {@link TimeseriesLifecyclePolicy} is made up of a set of {@link Phase}s which it will
 * move through. Soon we will constrain the phases using some kinda of lifecycle
 * type which will allow only particular {@link Phase}s to be defined, will
 * dictate the order in which the {@link Phase}s are executed and will define
 * which {@link LifecycleAction}s are allowed in each phase.
 */
public class TimeseriesLifecyclePolicy extends LifecyclePolicy {
    public static final String TYPE = "timeseries";
    static final List<String> VALID_PHASES = Arrays.asList("hot", "warm", "cold", "delete");

    /**
     * @param name
     *            the name of this {@link TimeseriesLifecyclePolicy}
     * @param phases
     *            a {@link Map} of {@link Phase}s which make up this
     *            {@link TimeseriesLifecyclePolicy}.
     */
    public TimeseriesLifecyclePolicy(String name, Map<String, Phase> phases) {
        super(name, phases);
    }

    /**
     * For Serialization
     */
    public TimeseriesLifecyclePolicy(StreamInput in) throws IOException {
        super(in);
    }

    public static TimeseriesLifecyclePolicy parse(XContentParser parser, Object context) {
        ToXContentContext factory = (ToXContentContext) context;
        return new TimeseriesLifecyclePolicy(factory.getName(), factory.getPhases());
    }

    @Override
    protected String getType() {
        return TYPE;
    }

    @Override
    protected Phase getFirstPhase() {
        Phase firstPhase = phases.get("hot");
        if (firstPhase == null) {
            firstPhase = phases.get("warm");
        }
        if (firstPhase == null) {
            firstPhase = phases.get("cold");
        }
        if (firstPhase == null) {
            firstPhase = phases.get("delete");
        }
        return firstPhase;
    }

    @Override
    protected Phase nextPhase(@Nullable Phase currentPhase) {
        if (currentPhase == null) {
            return getFirstPhase();
        }

        // VALID_PHASES is in order of execution
        boolean readyToSetNext = false;
        for (String phaseName : VALID_PHASES) {
            if (readyToSetNext && phases.containsKey(phaseName)) {
                return phases.get(phaseName);
            }
            if (phaseName.equals(currentPhase.getName())) {
                readyToSetNext = true;
            }
        }

        return null;
    }

    @Override
    public void validate(Collection<Phase> phases) {
        Set<String> allowedPhases = new HashSet<>(VALID_PHASES);
        phases.forEach(phase -> {
            if (allowedPhases.contains(phase.getName()) == false) {
                throw new IllegalArgumentException("Timeseries lifecycle does not support phase [" + phase.getName() + "]");
            }
        });
    }
}

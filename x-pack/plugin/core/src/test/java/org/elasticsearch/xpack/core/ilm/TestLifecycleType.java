/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TestLifecycleType implements LifecycleType {
    public static final TestLifecycleType INSTANCE = new TestLifecycleType();

    public static final String TYPE = "test";

    private TestLifecycleType() {
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public void validate(Collection<Phase> phases) {
        // always valid
    }

    @Override
    public List<Phase> getOrderedPhases(Map<String, Phase> phases) {
        return new ArrayList<>(phases.values());
    }

    @Override
    public String getNextPhaseName(String currentPhaseName, Map<String, Phase> phases) {
        List<String> orderedPhaseNames = getOrderedPhases(phases).stream().map(Phase::getName).collect(Collectors.toList());
        int index = orderedPhaseNames.indexOf(currentPhaseName);
        if (index < 0) {
            throw new IllegalArgumentException("[" + currentPhaseName + "] is not a valid phase for lifecycle type [" + TYPE + "]");
        } else if (index == orderedPhaseNames.size() - 1) {
            return null;
        } else {
            return orderedPhaseNames.get(index + 1);
        }
    }

    @Override
    public String getPreviousPhaseName(String currentPhaseName, Map<String, Phase> phases) {
        List<String> orderedPhaseNames = getOrderedPhases(phases).stream().map(Phase::getName).collect(Collectors.toList());
        int index = orderedPhaseNames.indexOf(currentPhaseName);
        if (index < 0) {
            throw new IllegalArgumentException("[" + currentPhaseName + "] is not a valid phase for lifecycle type [" + TYPE + "]");
        } else if (index == 0) {
            return null;
        } else {
            return orderedPhaseNames.get(index - 1);
        }
    }

    @Override
    public List<LifecycleAction> getOrderedActions(Phase phase) {
        return new ArrayList<>(phase.getActions().values());
    }

    @Override
    public String getNextActionName(String currentActionName, Phase phase) {
        List<String> orderedActionNames = getOrderedActions(phase).stream().map(LifecycleAction::getWriteableName)
                .collect(Collectors.toList());
        int index = orderedActionNames.indexOf(currentActionName);
        if (index < 0) {
            throw new IllegalArgumentException("[" + currentActionName + "] is not a valid action for phase [" + phase.getName()
                    + "] in lifecycle type [" + TYPE + "]");
        } else if (index == orderedActionNames.size() - 1) {
            return null;
        } else {
            return orderedActionNames.get(index + 1);
        }
    }
}

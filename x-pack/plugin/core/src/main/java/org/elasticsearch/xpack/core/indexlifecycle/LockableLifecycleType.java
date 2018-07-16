/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.common.io.stream.StreamOutput;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class LockableLifecycleType implements LifecycleType {
    public static final String TYPE = "lockable";
    public static final LockableLifecycleType INSTANCE = new LockableLifecycleType();

    @Override
    public List<Phase> getOrderedPhases(Map<String, Phase> phases) {
        return new ArrayList<>(phases.values());
    }

    @Override
    public List<LifecycleAction> getOrderedActions(Phase phase) {
        return new ArrayList<>(phase.getActions().values());
    }

    @Override
    public void validate(Collection<Phase> phases) {
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public void writeTo(StreamOutput out) {

    }
}

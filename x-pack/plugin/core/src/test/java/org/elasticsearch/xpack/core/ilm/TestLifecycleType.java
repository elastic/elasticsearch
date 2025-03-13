/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class TestLifecycleType implements LifecycleType {
    public static final TestLifecycleType INSTANCE = new TestLifecycleType();

    public static final String TYPE = "test";

    private TestLifecycleType() {}

    @Override
    public void writeTo(StreamOutput out) throws IOException {}

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
    public List<LifecycleAction> getOrderedActions(Phase phase) {
        return new ArrayList<>(phase.getActions().values());
    }

}

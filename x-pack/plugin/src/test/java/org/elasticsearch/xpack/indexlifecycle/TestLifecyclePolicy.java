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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TestLifecyclePolicy extends LifecyclePolicy {

    public static final String TYPE = "test";
    private List<Phase> phasesList;

    public TestLifecyclePolicy(String name, List<Phase> phasesList) {
        super(name, phasesList.stream().collect(Collectors.toMap(Phase::getName, Function.identity())));
        this.phasesList = phasesList;
    }

    public TestLifecyclePolicy(StreamInput in) throws IOException {
        super(in);
    }

    public static TestLifecyclePolicy parse(XContentParser parser, Object context) {
        ToXContentContext factory = (ToXContentContext) context;
        return new TestLifecyclePolicy(factory.getName(), new ArrayList<>(factory.getPhases().values()));
    }

    @Override
    protected String getType() {
        return TYPE;
    }
    @Override
    protected Phase getFirstPhase() {
        return phasesList.get(0);
    }

    @Override
    protected Phase nextPhase(@Nullable Phase currentPhase) {
        if (currentPhase == null) {
            return getFirstPhase();
        }

        for(int i=0; i < phasesList.size() - 1; i++) {
            if (phasesList.get(i).equals(currentPhase)) {
                return phasesList.get(i + 1);
            }
        }

        return null;
    }

    @Override
    protected void validate(Collection<Phase> phases) {
        // always valid
    }
}

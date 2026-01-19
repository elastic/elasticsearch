/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle.transitions.actions;

import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.lifecycle.transitions.DlmAction;
import org.elasticsearch.datastreams.lifecycle.transitions.DlmStep;
import org.elasticsearch.datastreams.lifecycle.transitions.steps.NoopStep;

import java.util.List;
import java.util.function.Function;

//TODO: REMOVE BEFORE PR
/**
 * A no-op action used as a placeholder for testing.
 */
public class NoopAction implements DlmAction {
    @Override
    public String actionName() {
        return "No-op action";
    }

    @Override
    public Function<DataStreamLifecycle, TimeValue> schedulingFieldFunction() {
        return dataStreamLifecycle -> TimeValue.ONE_MINUTE;
    }

    @Override
    public List<DlmStep> steps() {
        return List.of(new NoopStep());
    }
}

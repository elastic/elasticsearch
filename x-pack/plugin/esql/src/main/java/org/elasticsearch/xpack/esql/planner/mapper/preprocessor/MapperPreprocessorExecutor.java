/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.mapper.preprocessor;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plugin.TransportActionServices;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class MapperPreprocessorExecutor {

    private final TransportActionServices services;
    private final List<MappingPreProcessor> proprocessors = new ArrayList<>();

    public MapperPreprocessorExecutor(TransportActionServices services) {
        this.services = services;
    }

    public MapperPreprocessorExecutor addPreprocessor(MappingPreProcessor preProcessor) {
        proprocessors.add(preProcessor);
        return this;
    }

    public MapperPreprocessorExecutor addPreprocessors(Collection<MappingPreProcessor> preProcessors) {
        proprocessors.addAll(preProcessors);
        return this;
    }

    public void execute(LogicalPlan plan, ActionListener<LogicalPlan> listener) {
        execute(plan, 0, listener);
    }

    private void execute(LogicalPlan plan, int index, ActionListener<LogicalPlan> listener) {
        if (index == proprocessors.size()) {
            listener.onResponse(plan);
        } else {
            proprocessors.get(index).preprocess(plan, services, listener.delegateFailureAndWrap((l, p) -> execute(p, index + 1, l)));
        }
    }
}

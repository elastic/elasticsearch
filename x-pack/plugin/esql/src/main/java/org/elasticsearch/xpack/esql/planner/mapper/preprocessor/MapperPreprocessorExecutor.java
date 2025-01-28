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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MapperPreprocessorExecutor {

    private final TransportActionServices services;

    public MapperPreprocessorExecutor(TransportActionServices services) {
        this.services = services;
    }

    public void execute(LogicalPlan plan, ActionListener<LogicalPlan> listener) {
        execute(plan, preprocessors(plan), 0, listener);
    }

    private static List<MappingPreProcessor> preprocessors(LogicalPlan plan) {
        Set<MappingPreProcessor> preprocessors = new HashSet<>();
        plan.forEachExpressionDown(e -> {
            if (e instanceof MappingPreProcessor.MappingPreProcessorSupplier supplier) {
                preprocessors.add(supplier.mappingPreProcessor());
            }
        });
        return List.copyOf(preprocessors);
    }

    private void execute(LogicalPlan plan, List<MappingPreProcessor> preprocessors, int index, ActionListener<LogicalPlan> listener) {
        if (index == preprocessors.size()) {
            listener.onResponse(plan);
        } else {
            preprocessors.get(index)
                .preprocess(plan, services, listener.delegateFailureAndWrap((l, p) -> execute(p, preprocessors, index + 1, l)));
        }
    }
}

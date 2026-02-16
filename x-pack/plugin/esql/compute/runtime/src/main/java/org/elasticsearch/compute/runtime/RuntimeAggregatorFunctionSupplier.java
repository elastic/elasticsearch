/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.runtime;

import org.elasticsearch.compute.aggregation.AggregatorFunction;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.IntermediateStateDesc;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.operator.DriverContext;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Runtime implementation of {@link AggregatorFunctionSupplier} that creates
 * aggregator functions using bytecode generated at runtime.
 * <p>
 * This supplier generates aggregator classes on first use and caches them
 * for subsequent invocations.
 * </p>
 */
public class RuntimeAggregatorFunctionSupplier implements AggregatorFunctionSupplier {

    private final AggregatorSpec spec;
    private final String description;
    private final RuntimeAggregatorGenerator generator;

    private volatile Class<? extends AggregatorFunction> aggregatorClass;
    private volatile Class<? extends GroupingAggregatorFunction> groupingAggregatorClass;
    private volatile List<IntermediateStateDesc> intermediateStateDesc;

    public RuntimeAggregatorFunctionSupplier(Class<?> aggregatorDefinitionClass, String description) {
        this.spec = AggregatorSpec.from(aggregatorDefinitionClass);
        this.description = description;
        this.generator = RuntimeAggregatorGenerator.getInstance(aggregatorDefinitionClass.getClassLoader());
    }

    @Override
    public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
        return getIntermediateStateDesc();
    }

    @Override
    public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
        return getIntermediateStateDesc();
    }

    private List<IntermediateStateDesc> getIntermediateStateDesc() {
        if (intermediateStateDesc == null) {
            synchronized (this) {
                if (intermediateStateDesc == null) {
                    List<IntermediateStateDesc> desc = new ArrayList<>();
                    for (AggregatorSpec.IntermediateStateInfo state : spec.intermediateState()) {
                        ElementType elementType = ElementType.valueOf(state.elementType());
                        desc.add(new IntermediateStateDesc(state.name(), elementType));
                    }
                    intermediateStateDesc = List.copyOf(desc);
                }
            }
        }
        return intermediateStateDesc;
    }

    @Override
    public AggregatorFunction aggregator(DriverContext driverContext, List<Integer> channels) {
        Class<? extends AggregatorFunction> clazz = getAggregatorClass();
        try {
            // Find and invoke the static create method
            Method createMethod = clazz.getMethod("create", DriverContext.class, List.class);
            return (AggregatorFunction) createMethod.invoke(null, driverContext, channels);
        } catch (Exception e) {
            throw new RuntimeEvaluatorException("Failed to create aggregator instance: " + e.getMessage(), e);
        }
    }

    @Override
    public GroupingAggregatorFunction groupingAggregator(DriverContext driverContext, List<Integer> channels) {
        Class<? extends GroupingAggregatorFunction> clazz = getGroupingAggregatorClass();
        try {
            // Find and invoke the static create method
            Method createMethod = clazz.getMethod("create", List.class, DriverContext.class);
            return (GroupingAggregatorFunction) createMethod.invoke(null, channels, driverContext);
        } catch (Exception e) {
            throw new RuntimeEvaluatorException("Failed to create grouping aggregator instance: " + e.getMessage(), e);
        }
    }

    @Override
    public String describe() {
        return description;
    }

    private Class<? extends AggregatorFunction> getAggregatorClass() {
        if (aggregatorClass == null) {
            synchronized (this) {
                if (aggregatorClass == null) {
                    aggregatorClass = generator.getOrGenerateAggregator(spec);
                }
            }
        }
        return aggregatorClass;
    }

    private Class<? extends GroupingAggregatorFunction> getGroupingAggregatorClass() {
        if (groupingAggregatorClass == null) {
            synchronized (this) {
                if (groupingAggregatorClass == null) {
                    groupingAggregatorClass = generator.getOrGenerateGroupingAggregator(spec);
                }
            }
        }
        return groupingAggregatorClass;
    }
}

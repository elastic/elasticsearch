/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.injection;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.injection.spec.MethodHandleSpec;
import org.elasticsearch.injection.spec.ParameterSpec;
import org.elasticsearch.injection.step.InjectionStep;
import org.elasticsearch.injection.step.InstantiateStep;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Performs the actual injection operations by running the {@link InjectionStep}s.
 * <p>
 * The intent is that this logic is as simple as possible so that we don't run complex injection
 * logic alongside the user-supplied constructor logic. All the injector complexity is already
 * supposed to have happened in the planning phase. In particular, no injection-related errors
 * are supposed to be detected during execution; they should be detected during planning and validation.
 * All exceptions thrown during execution are supposed to be caused by user-supplied code.
 *
 * <p>
 * <strong>Execution model</strong>:
 * The state of the injector during injection comprises a map from classes to objects.
 * Before any steps execute, the map is pre-populated by object instances added via
 * {@link Injector#addInstance(Object)}  Injector.addInstance},
 * and then the steps begin to execute, reading and writing from this map.
 * Some steps create objects and add them to this map; others manipulate the map itself.
 */
final class PlanInterpreter {
    private static final Logger logger = LogManager.getLogger(PlanInterpreter.class);
    private final Map<Class<?>, Object> instances = new LinkedHashMap<>();

    PlanInterpreter(Map<Class<?>, Object> existingInstances) {
        existingInstances.forEach(this::addInstance);
    }

    /**
     * Main entry point. Contains the implementation logic for each {@link InjectionStep}.
     */
    void executePlan(List<InjectionStep> plan) {
        int numConstructorCalls = 0;
        for (InjectionStep step : plan) {
            if (step instanceof InstantiateStep i) {
                MethodHandleSpec spec = i.spec();
                logger.trace("Instantiating {}", spec.requestedType().getSimpleName());
                addInstance(spec.requestedType(), instantiate(spec));
                ++numConstructorCalls;
            } else {
                // TODO: switch patterns would make this unnecessary
                assert false : "Unexpected step type: " + step.getClass().getSimpleName();
                throw new IllegalStateException("Unexpected step type: " + step.getClass().getSimpleName());
            }
        }
        logger.debug("Instantiated {} objects", numConstructorCalls);
    }

    /**
     * @return the list element corresponding to instances.get(type).get(0),
     * assuming that instances.get(type) has exactly one element.
     * @throws IllegalStateException if instances.get(type) does not have exactly one element
     */
    public <T> T theInstanceOf(Class<T> type) {
        Object instance = instances.get(type);
        if (instance == null) {
            throw new IllegalStateException("No object of type " + type.getSimpleName());
        }
        return type.cast(instance);
    }

    private void addInstance(Class<?> requestedType, Object instance) {
        Object old = instances.put(requestedType, instance);
        if (old != null) {
            throw new IllegalStateException("Multiple objects for " + requestedType);
        }
    }

    /**
     * @throws IllegalStateException if the <code>MethodHandle</code> throws.
     */
    @SuppressForbidden(
        reason = "Can't call invokeExact because we don't know the method argument types statically, "
            + "since each constructor has a different signature"
    )
    private Object instantiate(MethodHandleSpec spec) {
        Object[] args = spec.parameters().stream().map(this::parameterValue).toArray();
        try {
            return spec.methodHandle().invokeWithArguments(args);
        } catch (Throwable e) {
            throw new IllegalStateException("Unexpected exception while instantiating {}" + spec, e);
        }
    }

    private Object parameterValue(ParameterSpec parameterSpec) {
        return theInstanceOf(parameterSpec.formalType());
    }

}

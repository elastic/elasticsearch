/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.injection;

import org.elasticsearch.injection.spec.ExistingInstanceSpec;
import org.elasticsearch.injection.spec.InjectionSpec;
import org.elasticsearch.injection.spec.MethodHandleSpec;
import org.elasticsearch.injection.step.InjectionStep;
import org.elasticsearch.injection.step.InstantiateStep;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;

/**
 * <em>Evolution note</em>: the intent is to plan one domain/subsystem at a time.
 */
final class Planner {
    private static final Logger logger = LogManager.getLogger(Planner.class);

    final List<InjectionStep> plan;
    final Map<Class<?>, InjectionSpec> specsByClass;
    final Set<Class<?>> requiredTypes; // The injector's job is to ensure there is an instance of these; this is like the "root set"
    final Set<Class<?>> allParameterTypes; // All the injectable types in all dependencies (recursively) of all required types
    final Set<InjectionSpec> startedPlanning;
    final Set<InjectionSpec> finishedPlanning;
    final Set<Class<?>> alreadyProxied;

    /**
     * @param specsByClass an {@link InjectionSpec} indicating how each class should be injected
     * @param requiredTypes the classes of which we need instances
     * @param allParameterTypes the classes that appear as the type of any parameter of any constructor we might call
     */
    Planner(Map<Class<?>, InjectionSpec> specsByClass, Set<Class<?>> requiredTypes, Set<Class<?>> allParameterTypes) {
        this.requiredTypes = requiredTypes;
        this.plan = new ArrayList<>();
        this.specsByClass = unmodifiableMap(specsByClass);
        this.allParameterTypes = unmodifiableSet(allParameterTypes);
        this.startedPlanning = new HashSet<>();
        this.finishedPlanning = new HashSet<>();
        this.alreadyProxied = new HashSet<>();
    }

    /**
     * Intended to be called once.
     * <p>
     * Note that not all proxies are resolved once this plan has been executed.
     * <p>
     *
     * <em>Evolution note</em>: in a world with multiple domains/subsystems,
     * it will become necessary to defer proxy resolution until after other plans
     * have been executed, because they could create additional objects that ought
     * to be included in the proxies created by this plan.
     *
     * @return the {@link InjectionStep} objects listed in execution order.
     */
    List<InjectionStep> injectionPlan() {
        for (Class<?> c : requiredTypes) {
            planForClass(c, 0);
        }
        return plan;
    }

    /**
     * Recursive procedure that determines what effect <code>requestedClass</code>
     * should have on the plan under construction.
     *
     * @param depth is used just for indenting the logs
     */
    private void planForClass(Class<?> requestedClass, int depth) {
        InjectionSpec spec = specsByClass.get(requestedClass);
        if (spec == null) {
            throw new IllegalStateException("Cannot instantiate " + requestedClass + ": no specification provided");
        }
        planForSpec(spec, depth);
    }

    private void planForSpec(InjectionSpec spec, int depth) {
        if (finishedPlanning.contains(spec)) {
            logger.trace("{}Already planned {}", indent(depth), spec);
            return;
        }

        logger.trace("{}Planning for {}", indent(depth), spec);
        if (startedPlanning.add(spec) == false) {
            // TODO: Better cycle detection and reporting. Use SCCs
            throw new IllegalStateException("Cyclic dependency involving " + spec);
        }

        if (spec instanceof MethodHandleSpec m) {
            for (var p : m.parameters()) {
                logger.trace("{}- Recursing into {} for actual parameter {}", indent(depth), p.injectableType(), p);
                planForClass(p.injectableType(), depth + 1);
            }
            addStep(new InstantiateStep(m), depth);
        } else if (spec instanceof ExistingInstanceSpec e) {
            logger.trace("{}- Plan {}", indent(depth), e);
            // Nothing to do. The injector will already have the required object.
        } else {
            throw new AssertionError("Unexpected injection spec: " + spec);
        }

        finishedPlanning.add(spec);
    }

    private void addStep(InjectionStep newStep, int depth) {
        logger.trace("{}- Add step {}", indent(depth), newStep);
        plan.add(newStep);
    }

    private static Supplier<String> indent(int depth) {
        return () -> "\t".repeat(depth);
    }
}

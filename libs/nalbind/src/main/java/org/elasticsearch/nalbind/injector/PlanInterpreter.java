/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nalbind.injector;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.nalbind.exceptions.InjectionConfigurationException;
import org.elasticsearch.nalbind.exceptions.InjectionExecutionException;
import org.elasticsearch.nalbind.exceptions.UnresolvedProxyException;
import org.elasticsearch.nalbind.injector.spec.MethodHandleSpec;
import org.elasticsearch.nalbind.injector.spec.ParameterSpec;
import org.elasticsearch.nalbind.injector.step.InjectionStep;
import org.elasticsearch.nalbind.injector.step.InstantiateStep;
import org.elasticsearch.nalbind.injector.step.ListProxyCreateStep;
import org.elasticsearch.nalbind.injector.step.ListProxyResolveStep;
import org.elasticsearch.nalbind.injector.step.RollUpStep;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptyList;
import static org.elasticsearch.nalbind.injector.spec.InjectionModifiers.LIST;

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
 * The state of the injector during injection comprises a map from classes to lists of objects.
 * Before any steps execute, the map is pre-populated by object instances added via
 * {@link Injector#addInstance(Class, Object) Injector.addInstance},
 * and then the steps begin to execute, reading and writing from this map.
 * Some steps create objects and add them to this map; others manipulate the map itself.
 * In addition to the map of instances, there is also a {@link ProxyPool} used to manage
 * list proxies; some steps create, use, or resolve proxies via the pool.
 */
final class PlanInterpreter {
    private final Map<Class<?>, List<Object>> instances = new LinkedHashMap<>();
    private final ProxyPool proxyPool;

    PlanInterpreter(Map<Class<?>, Object> existingInstances, ProxyPool proxyPool) {
        existingInstances.forEach(this::addInstance);
        this.proxyPool = proxyPool;
    }

    /**
     * Main entry point. Contains the implementation logic for each {@link InjectionStep}.
     */
    void executePlan(List<InjectionStep> plan) {
        AtomicInteger numConstructorCalls = new AtomicInteger(0);
        plan.forEach(step -> {
            if (step instanceof InstantiateStep i) {
                MethodHandleSpec spec = i.spec();
                LOGGER.trace("Instantiating {}", spec.requestedType().getSimpleName());
                addInstance(spec.requestedType(), instantiate(spec));
                numConstructorCalls.incrementAndGet();
            } else if (step instanceof RollUpStep r) {
                var requestedType = r.requestedType();
                var subtype = r.subtype();
                LOGGER.trace("Rolling up {} into {}", subtype.getSimpleName(), requestedType.getSimpleName());
                addInstances(requestedType, instances.getOrDefault(subtype, emptyList()));
            } else if (step instanceof ListProxyCreateStep s) {
                if (proxyPool.putNewListProxy(s.elementType()) != null) {
                    throw new InjectionExecutionException("Two proxies for " + s.elementType());
                }
            } else if (step instanceof ListProxyResolveStep s) {
                Class<?> elementType = s.elementType();
                proxyPool.resolveListProxy(elementType, this.instances.getOrDefault(elementType, emptyList()));
            } else {
                // TODO: switch patterns would make this unnecessary
                throw new InjectionExecutionException("Unexpected step type: " + step.getClass().getSimpleName());
            }
        });
        LOGGER.debug("Instantiated {} objects", numConstructorCalls.get());
    }

    /**
     * @return the list element corresponding to instances.get(type).get(0),
     * assuming that instances.get(type) has exactly one element.
     * @throws InjectionExecutionException if instances.get(type) does not have exactly one element
     */
    public <T> T theOnlyInstance(Class<T> type) {
        List<Object> candidates = getInstances(type);
        if (candidates.size() == 1) {
            return type.cast(candidates.get(0));
        }

        throw new InjectionConfigurationException(
            "No unique object of type "
                + type.getSimpleName()
                + ": "
                + candidates.stream().map(x -> x.getClass().getSimpleName()).toList()
        );
    }

    /**
     * @return The objects currently associated with <code>type</code>.
     * Note that this is not <em>necessarily</em> all the instances of <code>type</code> that were ever instantiated,
     * unless the appropriate {@link RollUpStep}s have run.
     * It can also include objects that we didn't instantiate, but were included in the <code>existingInstances</code>
     * passed in this object's constructor.
     */
    public <T> List<Object> getInstances(Class<T> type) {
        return instances.getOrDefault(type, emptyList());
    }

    private void addInstance(Class<?> requestedType, Object instance) {
        instances.computeIfAbsent(requestedType, __ -> new ArrayList<>()).add(requestedType.cast(instance));
    }

    private void addInstances(Class<?> requestedType, Collection<?> c) {
        instances.computeIfAbsent(requestedType, __ -> new ArrayList<>()).addAll(c);
    }

    /**
     * @throws IllegalStateException if the <code>MethodHandle</code> throws.
     * This isn't a {@link InjectionExecutionException} because it's not an injector bug.
     */
    private Object instantiate(MethodHandleSpec spec) {
        Object[] args = spec.parameters().stream().map(this::parameterValue).toArray();
        try {
            return spec.methodHandle().invokeWithArguments(args);
        } catch (UnresolvedProxyException e) {
            // This exception is descriptive enough already. Catching it and wrapping it here
            // only makes the stack trace a little more complex for no benefit.
            throw e;
        } catch (Throwable e) {
            throw new IllegalStateException("Unexpected exception while instantiating {}" + spec, e);
        }
    }

    private Object parameterValue(ParameterSpec parameterSpec) {
        if (parameterSpec.modifiers().contains(LIST)) {
            return proxyPool.theProxyFor(parameterSpec.injectableType());
        } else {
            return theOnlyInstance(parameterSpec.formalType());
        }
    }

    private static final Logger LOGGER = LogManager.getLogger(PlanInterpreter.class);
}

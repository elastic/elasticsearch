/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.injection;

import org.elasticsearch.injection.api.Inject;
import org.elasticsearch.injection.spec.ExistingInstanceSpec;
import org.elasticsearch.injection.spec.InjectionSpec;
import org.elasticsearch.injection.spec.MethodHandleSpec;
import org.elasticsearch.injection.spec.ParameterSpec;
import org.elasticsearch.injection.step.InjectionStep;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toMap;

/**
 * The main object for dependency injection.
 * <p>
 * Allows the user to specify the requirements, then call {@link #inject} to create an object plus all its dependencies.
 * <p>
 * <em>Implementation note</em>: this class itself contains logic for <em>specifying</em> the injection requirements;
 * the actual injection operations are performed in other classes like {@link Planner} and {@link PlanInterpreter},
 */
public final class Injector {
    private static final Logger logger = LogManager.getLogger(Injector.class);

    /**
     * The specifications supplied by the user, as opposed to those inferred by the injector.
     */
    private final Map<Class<?>, InjectionSpec> seedSpecs;

    Injector(Map<Class<?>, InjectionSpec> seedSpecs) {
        this.seedSpecs = seedSpecs;
    }

    public static Injector create() {
        return new Injector(new LinkedHashMap<>());
    }

    /**
     * Instructs the injector to instantiate <code>classToProcess</code>
     * in accordance with whatever annotations may be present on that class.
     * <p>
     * There are only three ways the injector can find out that it must instantiate some class:
     * <ol>
     *     <li>
     *         This method
     *     </li>
     *     <li>
     *         The parameter passed to {@link #inject}
     *     </li>
     *     <li>
     *         A constructor parameter of some other class being instantiated,
     *         having exactly the right class (not a supertype)
     *     </li>
     * </ol>
     *
     * @return <code>this</code>
     */
    public Injector addClass(Class<?> classToProcess) {
        MethodHandleSpec methodHandleSpec = methodHandleSpecFor(classToProcess);
        var existing = seedSpecs.put(classToProcess, methodHandleSpec);
        if (existing != null) {
            throw new IllegalArgumentException("class " + classToProcess.getSimpleName() + " has already been added");
        }
        return this;
    }

    /**
     * Equivalent to multiple chained calls to {@link #addClass}.
     */
    public Injector addClasses(Collection<Class<?>> classesToProcess) {
        classesToProcess.forEach(this::addClass);
        return this;
    }

    /**
     * Equivalent to {@link #addInstance addInstance(object.getClass(), object)}.
     */
    public <T> Injector addInstance(Object object) {
        @SuppressWarnings("unchecked")
        Class<T> actualClass = (Class<T>) object.getClass(); // Whatever the runtime type is, it's represented by T
        return addInstance(actualClass, actualClass.cast(object));
    }

    /**
     * Equivalent to multiple calls to {@link #addInstance(Object)}.
     */
    public Injector addInstances(Collection<?> objects) {
        for (var x : objects) {
            addInstance(x);
        }
        return this;
    }

    /**
     * Indicates that <code>object</code> is to be injected for parameters of type <code>type</code>.
     * The given object is treated as though it had been instantiated by the injector.
     */
    public <T> Injector addInstance(Class<? super T> type, T object) {
        assert type.isInstance(object); // No unchecked casting shenanigans allowed
        var existing = seedSpecs.put(type, new ExistingInstanceSpec(type, object));
        if (existing != null) {
            throw new IllegalStateException("There's already an object for " + type);
        }
        return this;
    }

    /**
     * Main entry point. Causes objects to be constructed.
     * @return {@link Map} whose keys are all the requested <code>resultTypes</code> and whose values are all the instances of those types.
     */
    public Map<Class<?>, Object> inject(Collection<? extends Class<?>> resultTypes) {
        resultTypes.forEach(this::ensureClassIsSpecified);
        PlanInterpreter i = doInjection();
        return resultTypes.stream().collect(toMap(c -> c, i::theInstanceOf));
    }

    private <T> void ensureClassIsSpecified(Class<T> resultType) {
        if (seedSpecs.containsKey(resultType) == false) {
            addClass(resultType);
        }
    }

    private PlanInterpreter doInjection() {
        logger.debug("Starting injection");
        Map<Class<?>, InjectionSpec> specMap = specClosure(seedSpecs);
        Map<Class<?>, Object> existingInstances = new LinkedHashMap<>();
        specMap.values().forEach((spec) -> {
            if (spec instanceof ExistingInstanceSpec e) {
                existingInstances.put(e.requestedType(), e.instance());
            }
        });
        PlanInterpreter interpreter = new PlanInterpreter(existingInstances);
        interpreter.executePlan(injectionPlan(seedSpecs.keySet(), specMap));
        logger.debug("Done injection");
        return interpreter;
    }

    /**
     * Finds an {@link InjectionSpec} for every class the injector is capable of injecting.
     * <p>
     * We do this once the injector is fully configured, with all calls to {@link #addClass} and {@link #addInstance} finished,
     * so that we can easily build the complete picture of how injection should occur.
     * <p>
     * This is not part of the planning process; it's just discovering all the things
     * the injector needs to know about. This logic isn't concerned with ordering or dependency cycles.
     *
     * @param seedMap the injections the user explicitly asked for
     * @return an {@link InjectionSpec} for every class the injector is capable of injecting.
     */
    private static Map<Class<?>, InjectionSpec> specClosure(Map<Class<?>, InjectionSpec> seedMap) {
        assert seedMapIsValid(seedMap);

        // For convenience, we pretend there's a gigantic method out there that takes
        // all the seed types as parameters.
        Queue<ParameterSpec> workQueue = seedMap.values()
            .stream()
            .map(InjectionSpec::requestedType)
            .map(Injector::syntheticParameterSpec)
            .collect(toCollection(ArrayDeque::new));

        // This map doubles as a checklist of classes we're already finished processing
        Map<Class<?>, InjectionSpec> result = new LinkedHashMap<>();

        ParameterSpec p;
        while ((p = workQueue.poll()) != null) {
            Class<?> c = p.injectableType();
            InjectionSpec existingResult = result.get(c);
            if (existingResult != null) {
                logger.trace("Spec for {} already exists", c.getSimpleName());
                continue;
            }

            InjectionSpec spec = seedMap.get(c);
            if (spec instanceof ExistingInstanceSpec) {
                // simple!
                result.put(c, spec);
                continue;
            }

            // At this point, we know we'll need a MethodHandleSpec
            MethodHandleSpec methodHandleSpec;
            if (spec == null) {
                // The user didn't specify this class; we must infer it now
                spec = methodHandleSpec = methodHandleSpecFor(c);
            } else if (spec instanceof MethodHandleSpec m) {
                methodHandleSpec = m;
            } else {
                throw new AssertionError("Unexpected spec: " + spec);
            }

            logger.trace("Inspecting parameters for constructor of {}", c);
            for (var ps : methodHandleSpec.parameters()) {
                logger.trace("Enqueue {}", ps);
                workQueue.add(ps);
            }

            registerSpec(spec, result);
        }

        if (logger.isTraceEnabled()) {
            logger.trace("Specs: {}", result.values().stream().map(Object::toString).collect(joining("\n\t", "\n\t", "")));
        }
        return result;
    }

    private static MethodHandleSpec methodHandleSpecFor(Class<?> c) {
        Constructor<?> constructor = getSuitableConstructorIfAny(c);
        if (constructor == null) {
            throw new IllegalStateException("No suitable constructor for " + c);
        }

        MethodHandle ctorHandle;
        try {
            ctorHandle = lookup().unreflectConstructor(constructor);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }

        List<ParameterSpec> parameters = Stream.of(constructor.getParameters()).map(ParameterSpec::from).toList();

        return new MethodHandleSpec(c, ctorHandle, parameters);
    }

    /**
     * @return true (unless an assertion fails). Never returns false.
     */
    private static boolean seedMapIsValid(Map<Class<?>, InjectionSpec> seed) {
        seed.forEach(
            (c, s) -> { assert s.requestedType().equals(c) : "Spec must be associated with its requestedType, not " + c + ": " + s; }
        );
        return true;
    }

    /**
     * For the classes we've been explicitly asked to inject,
     * pretend there's some massive method taking all of them as parameters
     */
    private static ParameterSpec syntheticParameterSpec(Class<?> c) {
        return new ParameterSpec("synthetic_" + c.getSimpleName(), c, c);
    }

    private static Constructor<?> getSuitableConstructorIfAny(Class<?> type) {
        var constructors = Stream.of(type.getConstructors()).filter(not(Constructor::isSynthetic)).toList();
        if (constructors.size() == 1) {
            return constructors.get(0);
        }
        var injectConstructors = constructors.stream().filter(c -> c.isAnnotationPresent(Inject.class)).toList();
        if (injectConstructors.size() == 1) {
            return injectConstructors.get(0);
        }
        logger.trace("No suitable constructor for {}", type);
        return null;
    }

    private static void registerSpec(InjectionSpec spec, Map<Class<?>, InjectionSpec> specsByClass) {
        Class<?> requestedType = spec.requestedType();
        var existing = specsByClass.put(requestedType, spec);
        if (existing == null || existing.equals(spec)) {
            logger.trace("Register spec: {}", spec);
        } else {
            throw new IllegalStateException("Ambiguous specifications for " + requestedType + ": " + existing + " and " + spec);
        }
    }

    private List<InjectionStep> injectionPlan(Set<Class<?>> requiredClasses, Map<Class<?>, InjectionSpec> specsByClass) {
        logger.trace("Constructing instantiation plan");
        Set<Class<?>> allParameterTypes = new HashSet<>();
        specsByClass.values().forEach(spec -> {
            if (spec instanceof MethodHandleSpec m) {
                m.parameters().stream().map(ParameterSpec::injectableType).forEachOrdered(allParameterTypes::add);
            }
        });

        var plan = new Planner(specsByClass, requiredClasses, allParameterTypes).injectionPlan();
        if (logger.isDebugEnabled()) {
            logger.debug("Injection plan: {}", plan.stream().map(Object::toString).collect(joining("\n\t", "\n\t", "")));
        }
        return plan;
    }

    /**
     * <em>Evolution note</em>: there may be cases in the where we allow the user to
     * supply a {@link java.lang.invoke.MethodHandles.Lookup} for convenience,
     * so that they aren't required to make things public just to participate in injection.
     */
    private static MethodHandles.Lookup lookup() {
        return MethodHandles.publicLookup();
    }

}

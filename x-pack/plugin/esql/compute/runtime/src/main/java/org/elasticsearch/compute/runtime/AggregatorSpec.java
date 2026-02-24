/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.runtime;

import org.elasticsearch.compute.aggregation.AggregatorState;
import org.elasticsearch.compute.aggregation.GroupingAggregatorState;
import org.elasticsearch.compute.ann.RuntimeAggregator;
import org.elasticsearch.compute.ann.RuntimeIntermediateState;
import org.elasticsearch.core.SuppressForbidden;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Specification for a runtime-generated aggregator, used as cache key and
 * to drive bytecode generation.
 * <p>
 * This is the aggregator equivalent of {@link EvaluatorSpec}.
 * </p>
 */
public record AggregatorSpec(
    Class<?> declaringClass,
    Method initMethod,
    Method combineMethod,
    Method firstMethod,
    List<IntermediateStateInfo> intermediateState,
    List<Class<? extends Exception>> warnExceptions,
    boolean generateGrouping,
    Class<?> stateType,
    List<Class<?>> combineParameterTypes
) {

    /**
     * Information about an intermediate state column.
     */
    public record IntermediateStateInfo(String name, String elementType, boolean isBlock) {
        public static IntermediateStateInfo from(RuntimeIntermediateState annotation) {
            String type = annotation.type();
            boolean block = false;
            if (type.toUpperCase(Locale.ROOT).endsWith("_BLOCK")) {
                type = type.substring(0, type.length() - "_BLOCK".length());
                block = true;
            }
            return new IntermediateStateInfo(annotation.name(), type.toUpperCase(Locale.ROOT), block);
        }
    }

    /**
     * Creates an AggregatorSpec from a class annotated with @RuntimeAggregator.
     */
    public static AggregatorSpec from(Class<?> clazz) {
        Objects.requireNonNull(clazz, "class must not be null");

        RuntimeAggregator annotation = clazz.getAnnotation(RuntimeAggregator.class);
        if (annotation == null) {
            throw new IllegalArgumentException("Class " + clazz.getName() + " is not annotated with @RuntimeAggregator");
        }

        // Find init method (init or initSingle)
        Method initMethod = findInitMethod(clazz);
        if (initMethod == null) {
            throw new IllegalArgumentException("Class " + clazz.getName() + " must have a static 'init' or 'initSingle' method");
        }

        // Determine state type from init method return type
        Class<?> stateType = initMethod.getReturnType();

        // Find combine method
        Method combineMethod = findCombineMethod(clazz, stateType);
        if (combineMethod == null) {
            throw new IllegalArgumentException(
                "Class " + clazz.getName() + " must have a static 'combine' method with first parameter matching state type"
            );
        }

        // Parse intermediate state
        List<IntermediateStateInfo> intermediateState = new ArrayList<>();
        for (RuntimeIntermediateState state : annotation.intermediateState()) {
            intermediateState.add(IntermediateStateInfo.from(state));
        }

        // Get combine parameter types (excluding state)
        List<Class<?>> combineParams = new ArrayList<>();
        Class<?>[] paramTypes = combineMethod.getParameterTypes();
        for (int i = 1; i < paramTypes.length; i++) {
            combineParams.add(paramTypes[i]);
        }

        // Find optional first method (only for non-primitive state)
        Method firstMethod = null;
        if (stateType.isPrimitive() == false) {
            firstMethod = findFirstMethod(clazz, combineMethod);
        }

        return new AggregatorSpec(
            clazz,
            initMethod,
            combineMethod,
            firstMethod,
            List.copyOf(intermediateState),
            List.of(annotation.warnExceptions()),
            annotation.grouping(),
            stateType,
            List.copyOf(combineParams)
        );
    }

    /**
     * Finds the optional first method, which has the same parameters as combine but returns void.
     * The first method is used for non-primitive state types to handle the first value specially.
     */
    @SuppressForbidden(reason = "Runtime aggregator generation requires reflection to discover annotated methods")
    private static Method findFirstMethod(Class<?> clazz, Method combineMethod) {
        Class<?>[] combineParams = combineMethod.getParameterTypes();
        for (Method method : clazz.getDeclaredMethods()) {
            if (method.getName().equals("first") && Modifier.isStatic(method.getModifiers())) {
                if (method.getReturnType() == void.class) {
                    Class<?>[] params = method.getParameterTypes();
                    if (Arrays.equals(params, combineParams)) {
                        return method;
                    }
                }
            }
        }
        return null;
    }

    @SuppressForbidden(reason = "Runtime aggregator generation requires reflection to discover annotated methods")
    private static Method findInitMethod(Class<?> clazz) {
        // Try "init" first, then "initSingle"
        for (String name : new String[] { "init", "initSingle" }) {
            for (Method method : clazz.getDeclaredMethods()) {
                if (method.getName().equals(name) && Modifier.isStatic(method.getModifiers())) {
                    return method;
                }
            }
        }
        return null;
    }

    @SuppressForbidden(reason = "Runtime aggregator generation requires reflection to discover annotated methods")
    private static Method findCombineMethod(Class<?> clazz, Class<?> stateType) {
        Method primaryCombine = null;
        Method intermediateCombine = null;

        for (Method method : clazz.getDeclaredMethods()) {
            if (method.getName().equals("combine") && Modifier.isStatic(method.getModifiers())) {
                Class<?>[] params = method.getParameterTypes();
                if (params.length >= 2 && params[0].equals(stateType)) {
                    // First param matches state type
                    if (params[1].equals(stateType)) {
                        // Second param also matches state type - this is for intermediate state combination
                        intermediateCombine = method;
                    } else {
                        // Second param is different - this is for raw value combination
                        primaryCombine = method;
                    }
                }
            }
        }

        // Prefer the primary combine method (for raw values) over intermediate combine
        return primaryCombine != null ? primaryCombine : intermediateCombine;
    }

    /**
     * Finds the combine method for intermediate state (state + state -> state).
     * This is used when combining partial aggregations.
     */
    @SuppressForbidden(reason = "Runtime aggregator generation requires reflection to discover annotated methods")
    private static Method findIntermediateCombineMethod(Class<?> clazz, Class<?> stateType) {
        for (Method method : clazz.getDeclaredMethods()) {
            if (method.getName().equals("combine") && Modifier.isStatic(method.getModifiers())) {
                Class<?>[] params = method.getParameterTypes();
                if (params.length == 2 && params[0].equals(stateType) && params[1].equals(stateType)) {
                    return method;
                }
            }
        }
        return null;
    }

    /**
     * Returns the combine method for intermediate state combination.
     * May return null if no such method exists.
     */
    public Method intermediateCombineMethod() {
        return findIntermediateCombineMethod(declaringClass, stateType);
    }

    /**
     * Returns true if the state type is a primitive (int, long, double, etc.).
     * This means the state is wrapped in a generated wrapper class like LongState.
     */
    public boolean isPrimitiveState() {
        return stateType.isPrimitive();
    }

    /**
     * Returns true if the state type is a custom class implementing AggregatorState.
     * This means the state is stored directly without a wrapper.
     */
    public boolean isCustomState() {
        return stateType.isPrimitive() == false && AggregatorState.class.isAssignableFrom(stateType);
    }

    /**
     * Returns true if the init method requires a DriverContext parameter.
     * This is typically the case for custom state types that need circuit breakers.
     */
    public boolean initRequiresDriverContext() {
        for (Class<?> paramType : initMethod.getParameterTypes()) {
            if (paramType.getName().equals("org.elasticsearch.compute.operator.DriverContext")) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns true if this aggregator has a custom grouping state type.
     * Custom grouping state implements GroupingAggregatorState.
     */
    public boolean hasCustomGroupingState() {
        Method initGroupingMethod = findInitGroupingMethod(declaringClass);
        if (initGroupingMethod == null) {
            return false;
        }
        Class<?> returnType = initGroupingMethod.getReturnType();
        return GroupingAggregatorState.class.isAssignableFrom(returnType);
    }

    /**
     * Returns the initGrouping method if it exists.
     */
    public Method initGroupingMethod() {
        return findInitGroupingMethod(declaringClass);
    }

    /**
     * Returns the grouping state type (return type of initGrouping method).
     */
    public Class<?> groupingStateType() {
        Method initGroupingMethod = findInitGroupingMethod(declaringClass);
        if (initGroupingMethod != null) {
            return initGroupingMethod.getReturnType();
        }
        return null;
    }

    /**
     * Returns the grouping combine method: combine(GroupingState, int groupId, value).
     */
    public Method groupingCombineMethod() {
        Class<?> groupingStateType = groupingStateType();
        if (groupingStateType == null) {
            return null;
        }
        return findGroupingCombineMethod(declaringClass, groupingStateType);
    }

    /**
     * Returns the grouping combineIntermediate method.
     */
    @SuppressForbidden(reason = "Runtime aggregator generation requires reflection to discover annotated methods")
    public Method groupingCombineIntermediateMethod() {
        Class<?> groupingStateType = groupingStateType();
        if (groupingStateType == null) {
            return null;
        }
        for (Method method : declaringClass.getDeclaredMethods()) {
            if (method.getName().equals("combineIntermediate") && Modifier.isStatic(method.getModifiers())) {
                Class<?>[] params = method.getParameterTypes();
                // combineIntermediate(GroupingState, int groupId, value, boolean seen)
                if (params.length >= 3 && params[0].equals(groupingStateType) && params[1] == int.class) {
                    return method;
                }
            }
        }
        return null;
    }

    /**
     * Returns the grouping evaluateFinal method.
     */
    @SuppressForbidden(reason = "Runtime aggregator generation requires reflection to discover annotated methods")
    public Method groupingEvaluateFinalMethod() {
        Class<?> groupingStateType = groupingStateType();
        if (groupingStateType == null) {
            return null;
        }
        for (Method method : declaringClass.getDeclaredMethods()) {
            if (method.getName().equals("evaluateFinal") && Modifier.isStatic(method.getModifiers())) {
                Class<?>[] params = method.getParameterTypes();
                // evaluateFinal(GroupingState, IntVector selected, GroupingAggregatorEvaluationContext)
                if (params.length >= 2 && params[0].equals(groupingStateType)) {
                    return method;
                }
            }
        }
        return null;
    }

    @SuppressForbidden(reason = "Runtime aggregator generation requires reflection to discover annotated methods")
    private static Method findInitGroupingMethod(Class<?> clazz) {
        for (Method method : clazz.getDeclaredMethods()) {
            if (method.getName().equals("initGrouping") && Modifier.isStatic(method.getModifiers())) {
                return method;
            }
        }
        return null;
    }

    @SuppressForbidden(reason = "Runtime aggregator generation requires reflection to discover annotated methods")
    private static Method findGroupingCombineMethod(Class<?> clazz, Class<?> groupingStateType) {
        for (Method method : clazz.getDeclaredMethods()) {
            if (method.getName().equals("combine") && Modifier.isStatic(method.getModifiers())) {
                Class<?>[] params = method.getParameterTypes();
                // combine(GroupingState, int groupId, value)
                if (params.length >= 3 && params[0].equals(groupingStateType) && params[1] == int.class) {
                    return method;
                }
            }
        }
        return null;
    }

    /**
     * Returns the simple class name for the generated non-grouping aggregator.
     * Example: "Sum2Aggregator" -> "Sum2AggregatorFunction"
     */
    public String aggregatorSimpleName() {
        String name = declaringClass.getSimpleName();
        if (name.endsWith("Aggregator")) {
            return name + "Function";
        }
        return name + "AggregatorFunction";
    }

    /**
     * Returns the full class name for the generated non-grouping aggregator.
     * Generated classes are placed in the same package as the declaring class
     * so they can be loaded by the same classloader and access the user's aggregator class.
     */
    public String aggregatorClassName() {
        return declaringClass.getPackageName() + "." + aggregatorSimpleName();
    }

    /**
     * Returns the internal name (slashes instead of dots) for ASM.
     */
    public String aggregatorInternalName() {
        return aggregatorClassName().replace('.', '/');
    }

    /**
     * Returns the simple class name for the generated grouping aggregator.
     * Example: "Sum2Aggregator" -> "Sum2GroupingAggregatorFunction"
     */
    public String groupingAggregatorSimpleName() {
        String name = declaringClass.getSimpleName();
        if (name.endsWith("Aggregator")) {
            return name.substring(0, name.length() - "Aggregator".length()) + "GroupingAggregatorFunction";
        }
        return name + "GroupingAggregatorFunction";
    }

    /**
     * Returns the full class name for the generated grouping aggregator.
     * Generated classes are placed in the same package as the declaring class
     * so they can be loaded by the same classloader and access the user's aggregator class.
     */
    public String groupingAggregatorClassName() {
        return declaringClass.getPackageName() + "." + groupingAggregatorSimpleName();
    }

    /**
     * Returns the internal name of the declaring class for ASM.
     */
    public String declaringClassInternalName() {
        return declaringClass.getName().replace('.', '/');
    }

    /**
     * Returns true if this aggregator has warning exception handling.
     */
    public boolean hasWarnExceptions() {
        return warnExceptions.isEmpty() == false;
    }

    /**
     * Returns true if this aggregator has a first method for handling the first value.
     * The first method is only used for non-primitive state types.
     */
    public boolean hasFirstMethod() {
        return firstMethod != null;
    }

    /**
     * Returns the number of intermediate state columns.
     */
    public int intermediateBlockCount() {
        return intermediateState.size();
    }

    /**
     * Returns the element type of the first combine parameter (the value being aggregated).
     */
    public Class<?> valueType() {
        if (combineParameterTypes.isEmpty()) {
            throw new IllegalStateException("Combine method has no value parameters");
        }
        return combineParameterTypes.get(0);
    }

    /**
     * Returns the state wrapper class name for primitive types.
     * For example, for long state, returns "LongState".
     * For custom state types, returns the state type's full class name.
     */
    public String stateWrapperClassName() {
        if (isCustomState()) {
            // Custom state - use the state type directly
            return stateType.getName();
        }
        if (isPrimitiveState() == false) {
            // Non-primitive, non-AggregatorState - use the class name
            return stateType.getName();
        }
        // Primitive state - use wrapper class
        String typeName = stateType.getSimpleName();
        String capitalized = Character.toUpperCase(typeName.charAt(0)) + typeName.substring(1);
        if (hasWarnExceptions()) {
            return "org.elasticsearch.compute.aggregation." + capitalized + "FallibleState";
        }
        return "org.elasticsearch.compute.aggregation." + capitalized + "State";
    }

    /**
     * Returns the array state wrapper class name for grouping aggregators with primitive types.
     * For example, for long state, returns "LongArrayState".
     * For custom grouping state, returns the grouping state type's full class name.
     */
    public String arrayStateWrapperClassName() {
        if (hasCustomGroupingState()) {
            return groupingStateType().getName();
        }
        if (isPrimitiveState() == false) {
            return stateType.getName();
        }
        String typeName = stateType.getSimpleName();
        String capitalized = Character.toUpperCase(typeName.charAt(0)) + typeName.substring(1);
        if (hasWarnExceptions()) {
            return "org.elasticsearch.compute.aggregation." + capitalized + "FallibleArrayState";
        }
        return "org.elasticsearch.compute.aggregation." + capitalized + "ArrayState";
    }

    /**
     * Returns the init method parameters (excluding BigArrays and DriverContext).
     */
    public List<Class<?>> initParameters() {
        List<Class<?>> params = new ArrayList<>();
        for (Class<?> paramType : initMethod.getParameterTypes()) {
            String name = paramType.getName();
            if (name.equals("org.elasticsearch.common.util.BigArrays") == false
                && name.equals("org.elasticsearch.compute.operator.DriverContext") == false) {
                params.add(paramType);
            }
        }
        return params;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregatorSpec that = (AggregatorSpec) o;
        return generateGrouping == that.generateGrouping
            && Objects.equals(declaringClass, that.declaringClass)
            && Objects.equals(intermediateState, that.intermediateState);
    }

    @Override
    public int hashCode() {
        return Objects.hash(declaringClass, intermediateState, generateGrouping);
    }
}

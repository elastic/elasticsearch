/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.runtime;

import org.elasticsearch.compute.ann.RuntimeEvaluator;
import org.elasticsearch.compute.ann.RuntimeFixed;
import org.elasticsearch.compute.ann.RuntimeMvEvaluator;
import org.elasticsearch.core.SuppressForbidden;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Specification for a runtime-generated evaluator, used as cache key and
 * to drive bytecode generation.
 */
public record EvaluatorSpec(
    Class<?> declaringClass,
    String processMethodName,
    String extraName,
    List<Class<?>> parameterTypes,
    List<Boolean> fixedParameters,
    List<Boolean> includeInToString,
    Class<?> returnType,
    List<Class<? extends Exception>> warnExceptions,
    boolean isMvEvaluator,
    String finishMethodName,
    String singleMethodName,
    String ascendingMethodName
) {
    /**
     * Constructor for backward compatibility (non-MV evaluators).
     */
    public EvaluatorSpec(
        Class<?> declaringClass,
        String processMethodName,
        String extraName,
        List<Class<?>> parameterTypes,
        List<Boolean> fixedParameters,
        List<Boolean> includeInToString,
        Class<?> returnType,
        List<Class<? extends Exception>> warnExceptions
    ) {
        this(
            declaringClass,
            processMethodName,
            extraName,
            parameterTypes,
            fixedParameters,
            includeInToString,
            returnType,
            warnExceptions,
            false,
            "",
            "",
            ""
        );
    }

    /**
     * Constructor for MV evaluators without optimization methods (backward compatibility).
     */
    public EvaluatorSpec(
        Class<?> declaringClass,
        String processMethodName,
        String extraName,
        List<Class<?>> parameterTypes,
        List<Boolean> fixedParameters,
        List<Boolean> includeInToString,
        Class<?> returnType,
        List<Class<? extends Exception>> warnExceptions,
        boolean isMvEvaluator
    ) {
        this(
            declaringClass,
            processMethodName,
            extraName,
            parameterTypes,
            fixedParameters,
            includeInToString,
            returnType,
            warnExceptions,
            isMvEvaluator,
            "",
            "",
            ""
        );
    }

    /**
     * Creates an EvaluatorSpec from a method annotated with @RuntimeEvaluator.
     */
    public static EvaluatorSpec from(Method method) {
        Objects.requireNonNull(method, "method must not be null");

        RuntimeEvaluator annotation = method.getAnnotation(RuntimeEvaluator.class);
        if (annotation == null) {
            throw new IllegalArgumentException("Method " + method + " is not annotated with @RuntimeEvaluator");
        }

        // Check each parameter for @RuntimeFixed annotation
        Parameter[] params = method.getParameters();
        List<Boolean> fixedParams = new ArrayList<>(params.length);
        List<Boolean> includeInToStringList = new ArrayList<>(params.length);

        for (Parameter param : params) {
            RuntimeFixed fixedAnnotation = param.getAnnotation(RuntimeFixed.class);
            if (fixedAnnotation != null) {
                fixedParams.add(true);
                includeInToStringList.add(fixedAnnotation.includeInToString());
            } else {
                fixedParams.add(false);
                includeInToStringList.add(true); // Non-fixed params always included in toString
            }
        }

        return new EvaluatorSpec(
            method.getDeclaringClass(),
            method.getName(),
            annotation.extraName(),
            List.of(method.getParameterTypes()),
            List.copyOf(fixedParams),
            List.copyOf(includeInToStringList),
            method.getReturnType(),
            List.of(annotation.warnExceptions()),
            false,
            "",
            "",
            ""
        );
    }

    /**
     * Creates an EvaluatorSpec from a method annotated with @RuntimeMvEvaluator.
     * MvEvaluators use pairwise processing: process(T current, T next) to reduce
     * multivalued fields to a single value.
     */
    public static EvaluatorSpec fromMv(Method method) {
        Objects.requireNonNull(method, "method must not be null");

        RuntimeMvEvaluator annotation = method.getAnnotation(RuntimeMvEvaluator.class);
        if (annotation == null) {
            throw new IllegalArgumentException("Method " + method + " is not annotated with @RuntimeMvEvaluator");
        }

        // MvEvaluators use pairwise processing - must have exactly 2 parameters of the same type
        Class<?>[] paramTypes = method.getParameterTypes();
        if (paramTypes.length != 2) {
            throw new IllegalArgumentException(
                "MvEvaluator process method must have exactly 2 parameters for pairwise processing, got " + paramTypes.length
            );
        }
        if (paramTypes[0].equals(paramTypes[1]) == false) {
            throw new IllegalArgumentException(
                "MvEvaluator pairwise parameters must have the same type, got " + paramTypes[0] + " and " + paramTypes[1]
            );
        }

        // MvEvaluators don't use @RuntimeFixed
        List<Boolean> fixedParams = List.of(false, false);
        List<Boolean> includeInToStringList = List.of(true, true);

        return new EvaluatorSpec(
            method.getDeclaringClass(),
            method.getName(),
            annotation.extraName(),
            List.of(paramTypes),
            fixedParams,
            includeInToStringList,
            method.getReturnType(),
            List.of(annotation.warnExceptions()),
            true,
            annotation.finish(),
            annotation.single(),
            annotation.ascending()
        );
    }

    /**
     * Returns true if this MvEvaluator has a single value optimization method.
     */
    public boolean hasSingleValueMethod() {
        return singleMethodName != null && singleMethodName.isEmpty() == false;
    }

    /**
     * Returns true if this MvEvaluator has an ascending optimization method.
     */
    public boolean hasAscendingMethod() {
        return ascendingMethodName != null && ascendingMethodName.isEmpty() == false;
    }

    /**
     * Returns true if this MvEvaluator has a finish method.
     */
    public boolean hasFinishMethod() {
        return finishMethodName != null && finishMethodName.isEmpty() == false;
    }

    /**
     * Finds and returns the single value method if specified.
     * The single method signature should be: ResultType single(FieldType value)
     */
    @SuppressForbidden(reason = "Runtime evaluator generation requires reflection to discover annotated methods")
    public Optional<Method> findSingleMethod() {
        if (hasSingleValueMethod() == false) {
            return Optional.empty();
        }
        Class<?> fieldType = parameterType(0);
        try {
            return Optional.of(declaringClass.getDeclaredMethod(singleMethodName, fieldType));
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(
                "Could not find single method: " + singleMethodName + "(" + fieldType.getSimpleName() + ")",
                e
            );
        }
    }

    /**
     * Finds and returns the ascending method if specified.
     * The ascending method can have different signatures - see AscendingFunction in MvEvaluatorImplementer.
     */
    @SuppressForbidden(reason = "Runtime evaluator generation requires reflection to discover annotated methods")
    public Optional<Method> findAscendingMethod() {
        if (hasAscendingMethod() == false) {
            return Optional.empty();
        }
        // Try different signatures in order of preference
        // 1. Index lookup: int ascending(int valueCount)
        try {
            return Optional.of(declaringClass.getDeclaredMethod(ascendingMethodName, int.class));
        } catch (NoSuchMethodException e) {
            // Try other signatures - for now we only support index lookup mode
            throw new IllegalArgumentException("Could not find ascending method: " + ascendingMethodName + "(int)", e);
        }
    }

    /**
     * Finds and returns the finish method if specified.
     * The finish method signature should be: ResultType finish(WorkType work) or finish(WorkType work, int valueCount)
     */
    @SuppressForbidden(reason = "Runtime evaluator generation requires reflection to discover annotated methods")
    public Optional<Method> findFinishMethod() {
        if (hasFinishMethod() == false) {
            return Optional.empty();
        }
        Class<?> workType = parameterType(0);
        // Try with valueCount parameter first
        try {
            return Optional.of(declaringClass.getDeclaredMethod(finishMethodName, workType, int.class));
        } catch (NoSuchMethodException e) {
            // Try without valueCount
            try {
                return Optional.of(declaringClass.getDeclaredMethod(finishMethodName, workType));
            } catch (NoSuchMethodException e2) {
                throw new IllegalArgumentException(
                    "Could not find finish method: " + finishMethodName + "(" + workType.getSimpleName() + "...)",
                    e2
                );
            }
        }
    }

    /**
     * Returns the simple class name for the generated evaluator.
     * Example: "Abs2" + "Double" + "Evaluator" = "Abs2DoubleEvaluator"
     */
    public String evaluatorSimpleName() {
        return declaringClass.getSimpleName() + extraName + "Evaluator";
    }

    /**
     * Returns the full class name for the generated evaluator.
     */
    public String evaluatorClassName() {
        return declaringClass.getPackageName() + "." + evaluatorSimpleName();
    }

    /**
     * Returns the internal name (slashes instead of dots) for ASM.
     */
    public String evaluatorInternalName() {
        return evaluatorClassName().replace('.', '/');
    }

    /**
     * Returns the internal name of the declaring class for ASM.
     */
    public String declaringClassInternalName() {
        return declaringClass.getName().replace('.', '/');
    }

    /**
     * Returns true if this evaluator needs warning exception handling.
     */
    public boolean hasWarnExceptions() {
        return warnExceptions.isEmpty() == false;
    }

    /**
     * Returns the internal name of the function class (for calling static process methods).
     */
    public String functionClassInternal() {
        return declaringClass.getName().replace('.', '/');
    }

    /**
     * Returns the number of parameters for this evaluator.
     * Unary functions have 1 parameter, binary have 2, etc.
     */
    public int parameterCount() {
        return parameterTypes.size();
    }

    /**
     * Returns the field type (the first parameter type of the process method).
     * For unary functions, this is the single parameter type.
     */
    public String fieldType() {
        if (parameterTypes.isEmpty()) {
            throw new IllegalStateException("Process method has no parameters");
        }
        Class<?> type = parameterTypes.get(0);
        return type.getSimpleName().toLowerCase(java.util.Locale.ROOT);
    }

    /**
     * Returns the field type as a Class (the first parameter type of the process method).
     * For unary functions, this is the single parameter type.
     */
    public Class<?> fieldTypeClass() {
        if (parameterTypes.isEmpty()) {
            throw new IllegalStateException("Process method has no parameters");
        }
        return parameterTypes.get(0);
    }

    /**
     * Returns the parameter type at the given index.
     *
     * @param index the parameter index (0-based)
     * @return the parameter type class
     * @throws IndexOutOfBoundsException if index is out of range
     */
    public Class<?> parameterType(int index) {
        return parameterTypes.get(index);
    }

    /**
     * Returns the process method from the declaring class.
     */
    public Method processMethod() throws NoSuchMethodException {
        return declaringClass.getMethod(processMethodName, parameterTypes.toArray(new Class<?>[0]));
    }

    /**
     * Returns true if all parameters have the same type.
     * This is common for functions like add(int, int) or max(double, double).
     */
    public boolean hasHomogeneousParameters() {
        if (parameterTypes.isEmpty()) {
            return true;
        }
        Class<?> firstType = parameterTypes.get(0);
        for (int i = 1; i < parameterTypes.size(); i++) {
            if (parameterTypes.get(i).equals(firstType) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns true if the parameter at the given index is @RuntimeFixed.
     */
    public boolean isFixed(int index) {
        return fixedParameters.get(index);
    }

    /**
     * Returns true if the parameter at the given index should be included in toString().
     */
    public boolean includeInToString(int index) {
        return includeInToString.get(index);
    }

    /**
     * Returns the number of evaluated (non-fixed) parameters.
     * These are the parameters that need ExpressionEvaluator fields.
     */
    public int evaluatedParameterCount() {
        int count = 0;
        for (boolean fixed : fixedParameters) {
            if (fixed == false) {
                count++;
            }
        }
        return count;
    }

    /**
     * Returns the number of fixed parameters.
     */
    public int fixedParameterCount() {
        int count = 0;
        for (boolean fixed : fixedParameters) {
            if (fixed) {
                count++;
            }
        }
        return count;
    }

    /**
     * Returns true if this spec has any @RuntimeFixed parameters.
     */
    public boolean hasFixedParameters() {
        return fixedParameters.contains(true);
    }

    /**
     * Returns the index of the n-th evaluated (non-fixed) parameter.
     * For example, if parameters are [fixed, evaluated, fixed, evaluated],
     * getEvaluatedParameterIndex(0) returns 1, getEvaluatedParameterIndex(1) returns 3.
     */
    public int getEvaluatedParameterIndex(int evaluatedIndex) {
        int count = 0;
        for (int i = 0; i < parameterTypes.size(); i++) {
            if (fixedParameters.get(i) == false) {
                if (count == evaluatedIndex) {
                    return i;
                }
                count++;
            }
        }
        throw new IndexOutOfBoundsException("Evaluated parameter index " + evaluatedIndex + " out of range");
    }

    /**
     * Returns the index of the n-th fixed parameter.
     */
    public int getFixedParameterIndex(int fixedIndex) {
        int count = 0;
        for (int i = 0; i < parameterTypes.size(); i++) {
            if (fixedParameters.get(i)) {
                if (count == fixedIndex) {
                    return i;
                }
                count++;
            }
        }
        throw new IndexOutOfBoundsException("Fixed parameter index " + fixedIndex + " out of range");
    }

    /**
     * Returns true if this is a variadic function (takes an array parameter).
     * Variadic functions have exactly one parameter which is an array type.
     * Example: process(int[] values) for GREATEST, LEAST, COALESCE.
     */
    public boolean isVariadic() {
        return parameterCount() == 1 && parameterType(0).isArray();
    }

    /**
     * Returns the component type of the variadic array parameter.
     * For example, for int[] returns int.class.
     *
     * @throws IllegalStateException if this is not a variadic function
     */
    public Class<?> variadicComponentType() {
        if (isVariadic() == false) {
            throw new IllegalStateException("Not a variadic function");
        }
        return parameterType(0).getComponentType();
    }
}

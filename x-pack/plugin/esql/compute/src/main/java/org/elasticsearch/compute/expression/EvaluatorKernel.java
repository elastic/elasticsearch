/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.expression;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.List;

/**
 * Describes the per-row "kernel" of a generated {@link ExpressionEvaluator}: the pure function the evaluator
 * applies to each position once its inputs have been unpacked from blocks and checked for nulls and
 * multi-values.
 * <p>
 * Generated evaluators whose per-row function has a simple enough shape expose it as a
 * {@code public static} method named {@link #METHOD_NAME} on the evaluator class, with exactly the
 * signature described here, and publish this descriptor as a {@code public static final KERNEL} field.
 * This is the seam that lets a fused evaluator, built at runtime for a whole expression tree such as
 * {@code (a + b) * c}, call the per-row functions of several evaluators from one loop without
 * materializing the intermediate blocks. The {@code @Evaluator} annotation is not retained at runtime and
 * the annotated methods are package private, so without this descriptor there is nothing to call.
 * <p>
 * A kernel's contract mirrors the generated evaluator's per-row contract:
 * <ul>
 *   <li>every {@link Parameter#fixed() value} parameter receives a non-null, single-valued element of
 *       its block type;</li>
 *   <li>every {@link Parameter#fixed() fixed} parameter receives the matching entry of
 *       {@link FusableEvaluatorFactory#fixedValues()};</li>
 *   <li>throwing one of {@link #warnExceptions()} means "warn and produce {@code null}" for that
 *       position; any other exception propagates.</li>
 * </ul>
 *
 * @param evaluatorClass the generated evaluator class declaring the {@code public static process} method
 * @param resultType the primitive result type of the kernel
 * @param parameters the kernel's parameters, in declaration order
 * @param warnExceptions the exceptions the generated evaluator catches and converts to a warning and a
 *                       {@code null} result
 */
public record EvaluatorKernel(
    Class<?> evaluatorClass,
    Class<?> resultType,
    List<Parameter> parameters,
    List<Class<? extends Exception>> warnExceptions
) {
    /**
     * Name of the {@code public static} forwarder the generated evaluator exposes for this kernel.
     */
    public static final String METHOD_NAME = "process";

    /**
     * One parameter of the kernel.
     *
     * @param type the declared parameter type
     * @param fixed {@code true} if the parameter is a per-evaluator constant ({@code @Fixed}) rather than a
     *              per-row value read from an input block
     */
    public record Parameter(Class<?> type, boolean fixed) {}

    public EvaluatorKernel {
        parameters = List.copyOf(parameters);
        warnExceptions = List.copyOf(warnExceptions);
    }

    /**
     * A parameter read per row from an input block.
     */
    public static Parameter value(Class<?> type) {
        return new Parameter(type, false);
    }

    /**
     * A parameter that is constant for the lifetime of the evaluator.
     */
    public static Parameter fixed(Class<?> type) {
        return new Parameter(type, true);
    }

    /**
     * The number of parameters that are read per row from an input block.
     */
    public int valueParameterCount() {
        int count = 0;
        for (Parameter p : parameters) {
            if (p.fixed() == false) {
                count++;
            }
        }
        return count;
    }

    /**
     * The exact type of the {@link #METHOD_NAME} forwarder.
     */
    @SuppressWarnings("unchecked")
    public MethodType methodType() {
        List<? extends Class<?>> pTypes = parameters.stream().map(Parameter::type).toList();
        return MethodType.methodType(resultType, (List<Class<?>>) pTypes);
    }

    /**
     * Resolves the {@link #METHOD_NAME} forwarder on {@link #evaluatorClass()}. Generated code guarantees it
     * exists with exactly {@link #methodType()}; failing to find it means the generated class and this
     * descriptor have drifted apart, which is a bug in the code generator.
     */
    public MethodHandle methodHandle() {
        try {
            return MethodHandles.publicLookup().findStatic(evaluatorClass, METHOD_NAME, methodType());
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("kernel [" + this + "] has no matching public static [" + METHOD_NAME + "] method", e);
        }
    }
}

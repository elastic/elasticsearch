/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.expression;

import org.elasticsearch.compute.operator.WarningSourceLocation;

import java.util.List;

/**
 * An {@link ExpressionEvaluator.Factory} whose evaluator is a tree node with a simple per-row
 * {@link EvaluatorKernel kernel}: it evaluates its {@link #inputs()}, unpacks one value per input and
 * position, and applies the kernel.
 * <p>
 * Exposing this structure lets a fusion pass recognize subtrees of such factories and replace them with a
 * single evaluator that applies every kernel of the subtree in one loop over the page. Implementations
 * are generated for every {@code @Evaluator} method whose shape allows it; hand-written factories should
 * only implement this if their evaluator really behaves exactly like a generated one would, because the
 * fused evaluator reproduces that behavior rather than calling the evaluator.
 */
public interface FusableEvaluatorFactory extends ExpressionEvaluator.Factory {
    /**
     * The per-row function of the evaluator this factory builds.
     */
    EvaluatorKernel kernel();

    /**
     * The factories of the evaluators feeding the kernel's non-fixed parameters, in parameter order.
     */
    List<ExpressionEvaluator.Factory> inputs();

    /**
     * The values of the kernel's fixed parameters, in parameter order. Empty if the kernel has none.
     */
    List<Object> fixedValues();

    /**
     * The source location warnings produced by the evaluator are attributed to.
     */
    WarningSourceLocation source();
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression;

/**
 * Marker interface for expressions that propagate {@code null} through <b>all</b>
 * of their arguments: if any argument evaluates to {@code null} at a given position,
 * the expression itself evaluates to {@code null} at that position.
 * <p>
 * For aggregation functions, this means that if any argument is always
 * {@code null}, the aggregated value will also be {@code null}.
 * <p>
 * If a function has this property, it <b>must</b> be marked with this interface,
 * so that optimizer rules can apply.
 * <p>
 * Folding must also return {@code null} if any argument folds to {@code null}. Optimizer rules may fold these
 * expressions before {@code FoldNull} replaces them with {@code null}, with arguments that aren't {@code null}
 * literals yet (a {@code CASE} without a match, {@code 1/0}...), or are typed {@code null}s.
 * {@link org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper#fold} handles this before calling
 * {@link org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper#doFold}, so to customize folding, override
 * {@code doFold} instead of {@link Expression#fold}.
 */
public interface AnyNullIsNull {}

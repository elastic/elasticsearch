/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.xpack.esql.capabilities.ConfigurationAware;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

/**
 * Marker interface for nodes (expressions or plans) that require {@code @timestamp} bounds derived from the query DSL filter.
 * <p>
 * Implementations are resolved during analysis by the {@code ResolveTimestampBoundsAware} analyzer rule,
 * following the same pattern as {@link ConfigurationAware}.
 * </p>
 * <p>
 * Implementations are responsible for their own validation via {@link PostAnalysisVerificationAware#postAnalysisVerification}.
 * </p>
 * <p>
 * Use the sub-interfaces {@link OfExpression} and {@link OfLogicalPlan}
 * rather than implementing this interface directly.
 * </p>
 *
 * @param <T> the type returned by {@link #withTimestampBounds}, typically {@code Expression} or {@code LogicalPlan}
 */
public sealed interface TimestampBoundsAware<T extends Node<T>> extends PostAnalysisVerificationAware permits
    TimestampBoundsAware.OfExpression, TimestampBoundsAware.OfLogicalPlan {

    /**
     * Returns {@code true} if this node still needs timestamp bounds to be injected.
     */
    boolean needsTimestampBounds();

    /**
     * Returns a copy of this node with the given timestamp bounds applied.
     */
    T withTimestampBounds(Literal start, Literal end);

    /**
     * Sub-interface for {@link Expression} nodes that require timestamp bounds.
     */
    non-sealed interface OfExpression extends TimestampBoundsAware<Expression> {}

    /**
     * Sub-interface for {@link LogicalPlan} nodes that require timestamp bounds.
     */
    non-sealed interface OfLogicalPlan extends TimestampBoundsAware<LogicalPlan> {}
}

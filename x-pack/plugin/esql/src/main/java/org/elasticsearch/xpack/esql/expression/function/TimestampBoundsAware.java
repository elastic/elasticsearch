/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.capabilities.ConfigurationAware;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.core.expression.Literal;

/**
 * Marker interface for nodes (expressions or plans) that require {@code @timestamp} bounds derived from the query DSL filter.
 * <p>
 * Implementations are resolved during analysis by {@link Analyzer}'s {@code ResolveTimestampBoundsAware} rule,
 * following the same pattern as {@link ConfigurationAware}.
 * </p>
 * <p>
 * Expression implementations that still {@link #needsTimestampBounds() need bounds} after analysis are automatically
 * rejected by the {@link Verifier} with a client error.
 * LogicalPlan implementations are responsible for their own validation via {@link PostAnalysisVerificationAware#postAnalysisVerification}.
 * </p>
 *
 * @param <T> the type returned by {@link #withTimestampBounds}, typically {@code Expression} or {@code LogicalPlan}
 */
public interface TimestampBoundsAware<T> {

    /**
     * Returns {@code true} if this node still needs timestamp bounds to be injected.
     */
    boolean needsTimestampBounds();

    /**
     * Returns a copy of this node with the given timestamp bounds applied.
     */
    T withTimestampBounds(Literal start, Literal end);
}

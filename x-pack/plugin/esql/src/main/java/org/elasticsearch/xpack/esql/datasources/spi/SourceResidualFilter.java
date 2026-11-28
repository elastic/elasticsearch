/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.xpack.esql.core.expression.Expression;

import java.util.Collection;
import java.util.List;

/**
 * A pushed filter that also evaluates expressions the source cannot represent, using the same evaluator
 * {@code FilterExec} would. Those expressions are exact on the page the source emits, so they leave {@code FilterExec}.
 * A native predicate the source only approximates stays in {@code FilterExec}.
 */
public interface SourceResidualFilter {
    /** Expressions evaluated on the source page. Empty when the filter only prunes. */
    List<Expression> sourceResidual();

    /**
     * The same native filter, without evaluating {@code heldBack}. The caller keeps those expressions in
     * {@code FilterExec}, which sees values the source page does not, such as a value cast after the read.
     */
    Object withoutSourceResidual(Collection<Expression> heldBack);
}

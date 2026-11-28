/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

import org.elasticsearch.compute.data.BooleanBlock;

/**
 * Evaluates a predicate over positions already present in a batch. Used when the engine must keep false and null rows,
 * as {@code EVAL} does, and when one branch of an {@code OR} is tested without replacing the input stream. The result is
 * a {@link RowPredicate} result: one three-valued position per selected position, closed by the caller.
 */
public interface PredicateRefiner {
    BooleanBlock evaluate(SelectionBatch batch, int[] selection);
}

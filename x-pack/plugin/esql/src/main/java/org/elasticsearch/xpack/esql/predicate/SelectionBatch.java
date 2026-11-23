/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

/**
 * A bounded row space from any source. Positions stay valid until the caller has finished evaluating this batch and
 * closed every column loaded from it. The source may then release the batch. An operator that changes cardinality,
 * such as expanding a child relation, builds a new batch and a new {@link RowSpace} rather than reusing these positions.
 * Loading is scoped to a selection, and the caller owns the returned column.
 */
public interface SelectionBatch {
    int positionCount();

    /** Positions in this batch that the source has already kept. */
    SelectionMask candidates();

    /**
     * Loads {@code field} for {@code selection}. The column is compacted into selection order. Only selected positions
     * are read.
     */
    Column load(FieldRef field, SelectionMask selection);

    /** The row space these positions belong to. Ids from another space are different rows. */
    RowSpace rowSpace();

    /** This position's identity inside {@link #rowSpace()}. */
    SourceRow sourceRow(int position);
}

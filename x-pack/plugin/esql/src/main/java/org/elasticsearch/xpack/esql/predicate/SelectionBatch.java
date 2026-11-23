/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

/**
 * A bounded, stable row space from any source. Positions stay valid for the life of the batch. {@link #sourceRow(int)}
 * is the source's identity for that position, so a later batch can refer to the same row. The batch is the execution
 * context for column access: loading is scoped to a selection, and the caller owns the returned column.
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

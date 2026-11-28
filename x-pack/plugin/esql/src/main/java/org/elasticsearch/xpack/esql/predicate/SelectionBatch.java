/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

import org.elasticsearch.compute.data.Block;

/**
 * A bounded row space from any source. Positions stay valid until the caller has finished evaluating this batch and
 * released every block loaded from it. The source may then release the batch. An operator that changes cardinality,
 * such as expanding a child relation, builds a new batch and a new {@link RowSpace} rather than reusing these positions.
 * Loading is scoped to a selection, and the caller owns the returned block.
 * <p>
 * A selection is the batch positions still in play, in ascending order. A result for a selection is a block with one
 * position per selected position, in selection order.
 */
public interface SelectionBatch {
    int positionCount();

    /** Positions in this batch that the source has already kept, in ascending order. */
    int[] candidates();

    /**
     * Loads {@code field} for {@code selection}. The block has one position per selected position, in selection order, and
     * its type is {@link FieldRef#type()}. Only selected positions are read.
     */
    Block load(FieldRef field, int[] selection);

    /** The row space these positions belong to. Ids from another space are different rows. */
    RowSpace rowSpace();

    /** This position's identity inside {@link #rowSpace()}. */
    SourceRow sourceRow(int position);
}

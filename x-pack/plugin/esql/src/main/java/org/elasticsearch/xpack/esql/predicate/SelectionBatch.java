/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

/**
 * A bounded set of logical rows from any source. Positions are indexes into this batch. {@link #sourceRow(int)} is the
 * source's stable identity for that position, so a later batch can refer to the same row.
 */
public interface SelectionBatch {
    int positionCount();

    /** Positions in this batch that the source has already kept. */
    SelectionMask candidates();

    /**
     * Loads {@code field} for the batch's current candidates. Implementations read only those positions.
     */
    Column load(FieldRef field);

    /** The source's stable id for this position, such as a Lucene doc id, a Parquet row, or a remote result row. */
    int sourceRow(int position);

    /** Same rows, with a tighter candidate mask, so a later predicate does not load eliminated positions. */
    SelectionBatch withCandidates(SelectionMask candidates);
}

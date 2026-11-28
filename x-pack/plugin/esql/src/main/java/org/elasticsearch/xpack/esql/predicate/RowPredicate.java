/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.core.Releasable;

/**
 * Three-valued evaluation over a selection of a batch. The result has one position per selected position, in selection
 * order: true, false, or null. Positions outside the selection are not read and are not part of the result. The caller
 * closes the result block. Closing the predicate releases the evaluators it holds.
 */
public interface RowPredicate extends Releasable {
    BooleanBlock evaluate(SelectionBatch batch, int[] selection);

    @Override
    default void close() {}
}

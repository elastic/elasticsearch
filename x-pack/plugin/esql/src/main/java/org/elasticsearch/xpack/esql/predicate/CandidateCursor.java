/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

import org.elasticsearch.core.Releasable;

/**
 * A source-produced stream of batches. {@link #nextBatch()} returns null when the stream ends.
 * <p>
 * For a filtering plan, every row that could satisfy the whole predicate occurs in some batch: the cursor may return
 * extra rows and must not drop a possible match. {@link CandidateCoverage}, fixed when the plan is opened, says whether
 * those rows are a superset that still needs the residual or already the true set. The caller closes the cursor, and
 * with it any source resources held across batches.
 */
public interface CandidateCursor extends Releasable {
    SelectionBatch nextBatch();

    @Override
    default void close() {}
}

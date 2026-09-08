/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.search.IndexSearcher;

/**
 * Consulted once per segment before a columnar query starts reading it, so a caller that keeps a budget for
 * that work can refuse it before anything is allocated.
 *
 * <p>Not every shape scans, but the ones that cannot bisect do: a predicate or an automaton is tested against
 * every term, and a segment handed over as an overlay rather than as a column is decoded a document at a time.
 * Whether there is room for that is not a question this library can answer — it knows nothing of heap
 * accounting — so the search layer supplies the answer and this is the whole of the seam.
 *
 * <p>An implementation is expected to throw when the budget is spent, and to be cheap enough to call for
 * every segment of every query.
 */
@FunctionalInterface
public interface ScanBudget {

    /** Permits every scan, for a caller that keeps no budget. */
    ScanBudget UNLIMITED = searcher -> {};

    /** Throws when {@code searcher} has no room to read a segment of this column. */
    void check(IndexSearcher searcher);
}

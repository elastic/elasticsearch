/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sourcebatch;

/**
 * A column that carries its own window ({@code [from, from + count)}) over backing data.
 * Slicing yields a new instance sharing the same backing data but adjusted to a
 * sub-range — no copying occurs.
 *
 * <p>Implementations that can also produce Lucene fields for indexing should extend
 * {@link LuceneColumn}.
 */
public interface SliceableColumn {

    /**
     * Returns a view over {@code [from, from + count)} of this column's row range.
     *
     * @param from  start (inclusive) relative to this window, must be ≥ 0
     * @param count number of rows in the new window, must be ≥ 0 and {@code from + count}
     *              must be ≤ this column's row count
     */
    SliceableColumn slice(int from, int count);
}

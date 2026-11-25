/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

/**
 * Consulted before a page's storage grows, so a caller keeping a heap budget can refuse the growth rather
 * than discover it afterwards. Whether there is room is not a question this library can answer — it knows
 * nothing of heap accounting — so the caller supplies the answer.
 *
 * <p>A page's storage is reused between pages and only grows, so a reader charges the difference once and
 * nothing again until a larger page arrives.
 *
 * @see StringColumnReader#readBlock
 */
@FunctionalInterface
public interface PageBudget {

    /** Permits every page, for a caller that keeps no budget. */
    PageBudget UNLIMITED = bytes -> {};

    /** Charges {@code bytes} against the budget, throwing when it has no room for them. */
    void charge(long bytes);
}

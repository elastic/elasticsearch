/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

/** Receives each span of documents that {@link OrdinalRangePresence} steps over without reading. */
interface IntervalSkipObserver {

    IntervalSkipObserver NONE = (fromDoc, toDocExclusive) -> {};

    /**
     * Called once for each span, before the iterator advances past it.
     *
     * @param fromDoc        the first document of the span that was stepped over
     * @param toDocExclusive the document after the last one of the span
     */
    void rejected(int fromDoc, int toDocExclusive);
}

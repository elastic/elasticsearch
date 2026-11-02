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
 * The slots a merge cursor is about to hand over that come from one plain column, in the order that column holds
 * them and with nothing in between: the documents they belong to survived and landed next to one another. A
 * writer can then take the column's stored chunks as they are instead of the values one at a time; see
 * {@link PlainValues.Writer#copy}.
 */
public final class PlainRun {

    private final PlainValues.Reader source;
    private final long from;
    private final long to;
    private final boolean sourceSorted;

    PlainRun(PlainValues.Reader source, long from, long to, boolean sourceSorted) {
        assert from < to : "[" + from + ", " + to + ")";
        this.source = source;
        this.from = from;
        this.to = to;
        this.sourceSorted = sourceSorted;
    }

    /** How many slots the run covers, starting at the cursor's current document. */
    public long slots() {
        return to - from;
    }

    PlainValues.Reader source() {
        return source;
    }

    /** The source value address of the run's first slot. */
    long from() {
        return from;
    }

    /** The source value address just past the run. */
    long to() {
        return to;
    }

    /** Whether the source column's values are in term order, and so the run's are. */
    boolean sourceSorted() {
        return sourceSorted;
    }
}

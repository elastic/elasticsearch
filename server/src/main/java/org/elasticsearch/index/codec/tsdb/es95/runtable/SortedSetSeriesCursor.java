/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95.runtable;

import java.io.IOException;

/**
 * Ascending-by-global-{@code _tsid}-ordinal view of one source segment's series for a {@code SortedSet} field,
 * consumed by {@link SortedSetRunMerger} during segment merge. A dimension's ordinal set is constant across every
 * doc of a series, so the merger only needs each series' identity, doc count, and ordinal set rather than the
 * per-doc ordinal stream. The set ordinals are reported ascending and already remapped to the merged terms
 * dictionary.
 */
public interface SortedSetSeriesCursor {

    /** Advances to the next series, returning {@code false} once the segment's series are exhausted. */
    boolean next() throws IOException;

    /** The current series' global {@code _tsid} ordinal, strictly increasing across a segment. */
    long tsidOrd();

    /** The number of docs the current series holds in this segment. */
    int docCount();

    /** The number of ordinals in the current series' set; zero for an absent series. */
    int ordCount();

    /** The {@code index}-th ordinal of the current series' set, ascending. */
    int ordAt(int index);
}

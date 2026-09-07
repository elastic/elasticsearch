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
 * Forward iterator over the series of a source segment, a series being a maximal run of docs that share a
 * {@code _tsid}. Each step reports the series' global {@code _tsid} ordinal (comparable across segments so a
 * k-way merge can order series), its first doc, and its doc count.
 *
 * <p>This is the segment-merge series enumeration primitive: a merge builds one iterator per source segment and
 * a run merger interleaves them by {@link #tsidOrd()}. It is deliberately forward-only, matching how merge
 * consumes series; random doc-to-run positioning stays the concern of the per-doc read cursor. The same shape is
 * intended to back per-series query execution later, over a stored series index rather than a live scan.
 */
public interface SeriesIterator {

    /** Advances to the next series, returning {@code false} once the segment's series are exhausted. */
    boolean next() throws IOException;

    /** The current series' global {@code _tsid} ordinal, strictly increasing across a segment. */
    long tsidOrd();

    /** The first doc of the current series. */
    int startDoc();

    /** The number of docs in the current series. */
    int docCount();
}

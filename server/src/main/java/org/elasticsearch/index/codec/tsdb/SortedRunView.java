/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

/**
 * Random-access view of a single-valued {@code Sorted} field as runs rather than per doc, exposed by a
 * {@link SortedOrdinalReader} that stores the field run-table encoded. Segment merge reads a source field once
 * per run instead of once per doc, so its ordinal work scales with the number of runs (series) rather than the
 * number of docs.
 *
 * <p>This interface lives in the base codec package so the base producer can hand a source segment's runs to the
 * merge path without the base depending on any specific format's run-table implementation.
 */
public interface SortedRunView {

    /** The number of runs in the field. */
    int count();

    /** The first doc covered by {@code run}. */
    int startDoc(int run);

    /** The number of docs covered by {@code run}. */
    int length(int run);

    /** The ordinal shared by every doc in {@code run}; equals {@link #sentinel()} for an absent run. */
    long ordinal(int run);

    /** The reserved ordinal marking absent docs, equal to the field cardinality {@code K}. */
    int sentinel();
}

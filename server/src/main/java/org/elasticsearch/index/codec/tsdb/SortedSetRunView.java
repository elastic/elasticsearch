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
 * Random-access view of a multi-valued {@code SortedSet} field as runs rather than per doc, exposed by a
 * {@link SortedSetOrdinalReader} that stores the field run-table encoded. Segment merge reads a source field once
 * per run instead of once per doc, so its ordinal work scales with the number of runs (series) rather than the
 * number of docs.
 *
 * <p>This interface lives in the base codec package so the base producer can hand a source segment's runs to the
 * merge path without the base depending on any specific format's run-table implementation.
 */
public interface SortedSetRunView {

    /** The number of runs in the field. */
    int count();

    /** The first doc covered by {@code run}. */
    int startDoc(int run);

    /** The number of docs covered by {@code run}. */
    int length(int run);

    /** The number of ordinals in {@code run}'s set; zero for an absent (empty-set) run. */
    int ordCount(int run);

    /** The {@code index}-th ordinal of {@code run}'s set, ascending within the run. */
    long ordAt(int run, int index);
}
